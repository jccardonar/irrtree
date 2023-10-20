import logging
import asyncio
from typing import Set, Dict, Optional

import progressbar
from irrtree.datamodels import (
    re_asn,
    re_as_set,
    is_as_set,
    IRRServerOptions,
)

LOGGER = logging.getLogger()


class Worker:
    """
    A Worker keeps a connection towards the irrd server and performs queries
    agasint it using async functions.
    """

    def __init__(
        self,
        worker_id: int,
        server: IRRServerOptions,
        queue: asyncio.Queue,
    ):
        self.queue = queue
        self.irr_host = server.irr_host
        self.irr_port = server.irr_port
        self.server = server
        self.worker_id = worker_id
        self.max_restarts = server.max_restarts
        self.restarts = self.max_restarts

    def debug(self, msg: str):
        LOGGER.debug("w%s: %s", self.worker_id, msg)

    def warning(self, msg: str):
        LOGGER.warning("w%s: %s", self.worker_id, msg)

    async def initialize(self) -> None:
        """
        Start the connection and return.
        """
        sources_list = self.server.sources_list
        self.reader, self.writer = await asyncio.open_connection(
            self.irr_host, self.irr_port, limit=3 * 1024 * 1024
        )
        await self.send("!!")
        await self.send("!t1000")  # instruct irrd to set max timeout (1000s)
        await self.receive()  # silently ignore result
        if sources_list:
            await self.send("!s%s" % sources_list)
            answer = await self.receive()
            if answer != "C":
                raise Exception(
                    f"w{self.worker_id}: Got an error while setting the list. Answer"
                    f" {answer}"
                )

    async def terminate(self) -> None:
        await self.send("!q")
        self.writer.close()
        # await self.writer.wait_closed()

    async def send(self, command: str) -> None:
        self.debug(f"sending: {command}")
        msg = command + "\r\n"
        self.writer.write(msg.encode())
        await self.writer.drain()

    async def restart(self) -> None:
        """
        Restarts the connection
        """
        self.writer.close()
        await self.initialize()

    async def receive(self) -> str:
        """
        Returns a line from the reader
        The whois should not return an EOF. This handles this case
        reinitating the connection, but only for a limited number of tiems
        """
        line = await self.reader.readline()
        return line.decode()[:-1]

    async def send_and_receive(self, command: str) -> str:
        """
        Sends a command and receives. Streams should have been initialized..
        Deals with broken sockets by reinitializing the connection
        """
        while True:
            await self.send(command)
            response = await self.receive()
            if not response:
                # According to https://docs.python.org/3/library/asyncio-stream.html
                # this is an EOF. Restart the connection if possible
                self.restarts = self.restarts - 1
                if self.restarts < 0:
                    raise Exception("We got to the limit of restarts, failing")
                self.warning("We got an EOF from socket. Restarting connection ")
                try:
                    await self.restart()
                except Exception as e:
                    raise Exception("Failed restarting connection.") from e
                continue
            return response

    async def query(self, cmd: str, as_set: str, recurse=False) -> Set[str]:
        """
        Queries the server and returns a set of objects.
        It processes the type of respond based on the query, the return type
        could be, for instance,  prefixes, member objects.
        It does validate member objects (i query), but only throws warnings if
        one is invalid.
        TODO: It does not differentiate between errors (F), non-existence (D),
        empty sets (Single C).
        """
        query = "!%s%s%s" % (cmd, as_set, ",1" if recurse else "")
        # receive the first line, we should conform to the response operation result.
        # see https://irrd.readthedocs.io/en/stable/users/queries/whois/
        answer = await self.send_and_receive(query)
        if answer == "D":
            return set()
        elif answer[0] == "F":
            self.debug("Error: %s" % answer[1:])
            self.debug("Query was: %s" % query)
            raise Exception(
                f"w{self.worker_id}: Error on query {query}, answer was {answer}"
            )
        elif answer[0] == "A":
            self.debug("Info: receiving %s bytes" % answer[1:])
            unfiltered_value = await self.receive()
            results = set(unfiltered_value.split())

            # An A closes with a C
            close_return = await self.receive()
            if not close_return == "C":
                self.warning(
                    "Error: something went wrong with: %s. Not closing with C" % query
                )
            return set(results)
        else:
            self.warning(f"Response for query not processed. Query is {query}")
            return set()

    async def get_members(self, as_set: str, recurse: bool = False) -> Set[str]:
        if not is_as_set(as_set):
            raise Exception(
                f"Trying to get members from {as_set} which does not look like an"
                " as-set"
            )
        unfiltered_members = await self.query("i", as_set, recurse=recurse)
        members = set()
        for result in unfiltered_members:
            # Run data validation on the member objects.
            if re_asn.match(result):
                members.add(result.upper())  # found an autnum or hierarchical as-set
            elif re_as_set.match(result):
                members.add(result.upper())  # found a simple as-set
            else:
                self.warning(
                    "Warning: not honoring mbrs-by-ref for object %s with '%s'"
                    % (as_set, result)
                )
        return members

    async def run_get_tree(
        self,
        members_per_asset: Dict[str, Set[str]],
        visited: Set[str],
        filtered_as_sets: Set[str],
        pbar: Optional[progressbar.ProgressBar] = None,
    ) -> None:
        """
        Keep reasing the queue for as-objects and resolving them.
        The result should be stored in the db object (global)
        Store new objects in the query.
        It runs indefinitly, it needs to be canceled.
        The visited function is a set shared by workers needed to check
        if a as-set has been processed. It is not enough to check if the
        as-set is the members_per_db, since the as-set might be on the queue.
        Unfornutatly there is not simple way of checking the queue for content,
        so the visited is needed.
        """
        if self.reader is None or self.writer is None:
            raise Exception("Worker not initialized, run worder.initialize() first")

        # The visited should contain at least the root object. This is is simple check.
        if not visited:
            raise Exception(
                "The starting visited set is empty but should contain at least the root"
                " as-set"
            )

        while True:
            # get an item and process it
            item = await self.queue.get()
            self.debug("Processing %s for members" % item)

            if pbar:
                pbar.increment()

            if item in members_per_asset:
                self.warning(f"Reprocessing item {item}")
                # we need this if not, it will hang
                self.queue.task_done()
                continue

            # We get all members but filter those in filtered_as_sets
            unfiltered_sets = await self.get_members(item)
            members_per_asset[item] = set(
                m for m in unfiltered_sets if m not in filtered_as_sets
            )

            # add the as-sets to process on the queue
            for member in members_per_asset[item]:
                # simple way of testing the members is an aut-num
                if "-" not in member:
                    continue
                # if we already visited  or we already have the members, ignore
                if member in visited or member in members_per_asset:
                    continue
                visited.add(member)
                self.queue.put_nowait(member)

            self.queue.task_done()

    async def run_resolve_objects(
        self,
        prefixes_per_autun: Dict[str, Set[str]],
        pbar: Optional[progressbar.ProgressBar] = None,
    ):
        if self.reader is None or self.writer is None:
            raise Exception("Worker not initialized, run worder.initialize() first")

        while True:
            # get an item and process it
            item = await self.queue.get()
            self.debug("Resolving %s" % item)

            if pbar:
                pbar.increment()

            # if it is an autnum, get the prefixes
            if "-" in item:
                raise Exception("Resolving as-sets is not yet implemented")

            if item in prefixes_per_autun:
                self.warning(f"Attempted repeated processing of {item}")
                self.queue.task_done()
                continue

            prefixes = await self.query(
                "g" if self.server.afi == 4 else "6", item, False
            )

            prefixes_per_autun[item] = prefixes

            self.queue.task_done()

    async def run_get_origin_asns(self, origin_asns: Dict[str, Set[str]]):
        if self.reader is None or self.writer is None:
            raise Exception("Worker not initialized, run worder.initialize() first")

        while True:
            # get an item and process it
            item = await self.queue.get()
            self.debug("Resolving %s" % item)

            # if it is an autnum, get the prefixes
            if "-" not in item:
                raise Exception(f"Found a autnum {item} while resolving origin_asns")

            if item in origin_asns:
                self.warning(f"Attempted repeated processing of {item}")
                self.queue.task_done()
                continue

            origin_asns[item] = await self.query("i", item, True)

            self.queue.task_done()


async def join_queue_or_workers(
    queue: asyncio.Queue, worker_tasks: Dict[asyncio.Task, Worker]
):
    """
    Waits for a queue to  finish, if one worker finishes sooner,
    it fails and shows an error
    """
    queue_join_task = asyncio.create_task(queue.join())

    tasks = [queue_join_task, *worker_tasks]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    for task in done:
        if task != queue_join_task:
            # we had an issue, raise errors
            exception = task.exception()
            if exception is not None:
                LOGGER.error("Worker got an exception", exc_info=exception)
                raise exception

    # this is just in case the wait returns with an empty done (it should not)
    if queue_join_task not in done:
        raise Exception("Main job did not finish..")

    # cancel the pending tasks (which have not way of finishing)
    [t.cancel() for t in pending]
