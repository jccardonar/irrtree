import logging
from typing import Optional, Set, Dict, List, Tuple, FrozenSet
import asyncio
import copy

import progressbar
from irrtree.datamodels import (
    IRRServerOptions,
    IRRAsciiTreeOptions,
    ASSetTree,
    IRRAsciiTreeData,
)
from irrtree.query_workers import Worker, join_queue_or_workers
from irrtree.irrtree_print import print_asset_tree


LOGGER = logging.getLogger()


def _remove_recursivity(
    asset: str,
    members_per_asset: Dict[str, Set[str]],
    parents: Set[str],
    removed_links: Set[Tuple[str, str]],
):
    new_parents = set(parents)
    new_parents.add(asset)
    for member in sorted(members_per_asset[asset]):
        if "-" not in member:
            continue
        if member in parents:
            removed_links.add((asset, member))
            members_per_asset[asset].remove(member)
            continue
        _remove_recursivity(member, members_per_asset, new_parents, removed_links)


def remove_recursivity_from_tree(
    root_asset: str,
    members_per_asset: Dict[str, Set[str]],
) -> Tuple[Dict[str, Set[str]], Set[Tuple[str, str]]]:
    """
    Returns a copy of the members_per_asset without recursivity, together with
    the removed links
    """
    new_members_per_asset: Dict[str, Set[str]] = copy.deepcopy(members_per_asset)
    removed_links: Set[Tuple[str, str]] = set()
    _remove_recursivity(root_asset, new_members_per_asset, set(), removed_links)
    # some basic tests
    assert set(new_members_per_asset) == set(members_per_asset)
    assert set(m for s in members_per_asset.values() for m in s) == (
        set(m for s in new_members_per_asset.values() for m in s) | {root_asset}
    )
    # we are not using networkx, but this one is also a proper test
    # import networkx as nx
    # convert to graph
    # nx.is_directed_acyclic_graph(graph)

    return new_members_per_asset, removed_links


def _filter_autnum(
    as_set: str,
    allowed_autnum: Set[str],
    original_members_per_asset: Dict[str, Set[str]],
    new_members_per_asset: Dict[str, Set[str]],
    visited_assets: Set[str],
    parents: Set[str],
):
    # if as_set is already in new_members_per_asset, we dont need to do anything
    if as_set in visited_assets:
        return
    if as_set in new_members_per_asset:
        return
    visited_assets.add(as_set)
    new_members = set()
    new_parents = set(parents)
    new_parents.add(as_set)
    for member in sorted(original_members_per_asset[as_set]):
        # I am not sure what to do with parents. I will assume that
        # we will removing this level of recursivity
        if member in parents:
            continue
        # deal with autnum
        if "-" not in member:
            if member in allowed_autnum:
                new_members.add(member)
            continue
        if member not in visited_assets:
            # we need to process it
            _filter_autnum(
                member,
                allowed_autnum,
                original_members_per_asset,
                new_members_per_asset,
                visited_assets,
                new_parents,
            )
        if member in new_members_per_asset:
            new_members.add(member)
            continue
    if new_members:
        new_members_per_asset[as_set] = new_members


def filter_autnum(
    root_asset: str,
    allowed_autnum: Set[str],
    original_members_per_asset: Dict[str, Set[str]],
) -> Dict[str, Set[str]]:
    new_members_per_asset: Dict[str, Set[str]] = {}
    _filter_autnum(
        root_asset,
        allowed_autnum,
        original_members_per_asset,
        new_members_per_asset,
        set(),
        set(),
    )
    return new_members_per_asset


def get_origin_asns(
    as_set: str,
    origin_ases_per_asset: Dict[str, FrozenSet[str]],
    as_sets_members: Dict[str, Set[str]],
    parents: List[str],
    recursive_as_sets: Dict[str, Set[str]],
) -> Tuple[Optional[str], FrozenSet[str]]:
    """
    Finds all  AUT-NUM  under AS-SET (called origin_ases) from a list of
    as_sets_members (not only directly but under the hierarchy)

    Populates origin_ases_per_asset with the set of origin_ases for each as-set.

    Recursive, returns the recursive parent, if any, and the Set of origin ASes
    for this as-set without recursion.
    The main problem is that recursivity can be on different levels, so
    something like this is possible
    AS-1
     AS-2
      AS-3
       AS-2
       AS-4
       AS5
      AS-4
       AS-1
       AS6
    So we go over the members, keep the recursiveness in recursive_as_sets and when
    we resolve it, we assign the same data to the recursive children
    """
    # if the as-set is already in the db, return
    if as_set in origin_ases_per_asset:
        return None, origin_ases_per_asset[as_set]

    # nr_ means no recursive
    nr_origin_asns: Set[str] = set()
    new_parents = parents + [as_set]

    # the complication here is that we can have different recursiveness.
    # the as-set can reference itself directly
    # the as-set can be referenced in one of the children.
    recursive_parent = None
    for member in as_sets_members[as_set]:
        if member == as_set:
            # member is the same as_set, skipping then
            continue
        if member in parents:
            # the member is one of the parents. Set the highest one as the
            # recursive parent
            if recursive_parent is None:
                recursive_parent = member
                continue
            # we cannot have multiple parents, so even if there is redundancy
            # of two, we just pick the highest (i.e. the closest to the top)
            if parents.index(member) < parents.index(recursive_parent):
                recursive_parent = member
            continue

        # as-sets without a -, e.g. AS123, is an origin ases, add it to the
        # nr_origin_asns of this as-set
        if "-" not in member:
            nr_origin_asns.add(member)
            continue
        # we now go recursively to process the as-set member. We will get the
        # original ases of this as, and whether the member has a recursive
        # parent
        this_member_recursive_parent, member_origin_asns = get_origin_asns(
            member,
            origin_ases_per_asset,
            as_sets_members,
            new_parents,
            recursive_as_sets,
        )

        # we extend the origin asnes with the ones of the children
        nr_origin_asns |= member_origin_asns

        # now we deal with the recursive parent of a member.
        # if it exists and the recusive parent is larger than the one we have
        # found already, we
        # update it and continue

        if this_member_recursive_parent is None:
            continue
        # now we have to change the recursive father if higher
        if this_member_recursive_parent == as_set:
            continue
        if this_member_recursive_parent not in parents:
            raise Exception(
                "we got a recursive parent for a child, that we dont have.."
            )
        if recursive_parent is None:
            recursive_parent = this_member_recursive_parent
            continue
        if parents.index(this_member_recursive_parent) < parents.index(
            recursive_parent
        ):
            recursive_parent = this_member_recursive_parent

    # if the as-set member has a recursive parent, we dont add them to the db
    # yet.
    # we add it to recursive_as_sets, which keeps the children for each parent
    # as-set
    # but then, if the as-set has children we need to move them to the
    # recursive_parent set.
    if recursive_parent:
        # we cannot assign the definitve origin_asns yet,
        # but, we have to deal with multiple recursiveness
        recursive_as_sets.setdefault(recursive_parent, set()).add(as_set)
        if as_set in recursive_as_sets:
            # we move the chidlren to the recursive_parent set, adn remove this
            # as-set  from recursive_parent
            for child_as_set_referencing_parent in recursive_as_sets[as_set]:
                recursive_as_sets.setdefault(recursive_parent, set()).add(
                    child_as_set_referencing_parent
                )
            del recursive_as_sets[as_set]
    else:
        # if we are resolved, then the nr_origin_asns are the same origin_asns
        # and we can set the children with the right set
        origin_ases_per_asset[as_set] = frozenset(nr_origin_asns)
        if as_set in recursive_as_sets:
            for child_as_set_referencing_parent in recursive_as_sets[as_set]:
                origin_ases_per_asset[
                    child_as_set_referencing_parent
                ] = origin_ases_per_asset[as_set]
            del recursive_as_sets[as_set]

    return recursive_parent, frozenset(nr_origin_asns)


def get_origin_asns_from_members(
    root_as_set: str, members_per_asset: Dict[str, Set[str]]
) -> Dict[str, FrozenSet[str]]:
    """
    We can pre-calculate the origin_asns for each as-set by traversing the
    tree
    TODO: We could use the irrserver to get the same data. This is what the
    original irrtree does.
    """

    origin_ases_per_asset: Dict[str, FrozenSet[str]] = {}

    recursive_as_sets: Dict[str, Set[str]] = {}
    rp, _ = get_origin_asns(
        root_as_set,
        origin_ases_per_asset,
        members_per_asset,
        parents=[],
        recursive_as_sets=recursive_as_sets,
    )

    # we need to double check the process did not fail (it is, after all, the
    # most complex process)
    assert rp is None, "The parent tree depends on a as-set that is not included"
    assert (
        not recursive_as_sets
    ), "Internal error: The recursive_as_sets contains unmanaged recursive as_sets"
    assert set(origin_ases_per_asset) == set(
        members_per_asset
    ), "we did not find the origin ases for all as-sets on the tree"

    return origin_ases_per_asset


async def irrtree_process(
    root_as_set: str,
    irr_server_options: IRRServerOptions,
    irr_treeoptions: IRRAsciiTreeOptions,
    debug: bool,
    disable_progress_bar: bool,
) -> str:
    query_object = root_as_set
    queue: asyncio.Queue = asyncio.Queue()

    # start workers, this actually establishes the session.
    workers = []
    for n in range(0, irr_server_options.workers):
        worker = Worker(n, irr_server_options, queue)
        await worker.initialize()
        workers.append(worker)

    # The irrtree process means
    # Obtains the irrtree based on the parameters. They contain soruces, filters, AFI
    # A filter is only for as-sets. We will ingore those members.

    # get tree
    queue.put_nowait(query_object)
    visited = set([query_object])
    pbar = None
    if not disable_progress_bar:
        widgets = [
            "Getting member tree. Processed: ",
            progressbar.Counter(),
            " objects (",
            progressbar.Timer(),
            ")",
        ]
        pbar = progressbar.ProgressBar(
            widgets=widgets, max_value=progressbar.UnknownLength
        )

    members_per_asset: Dict[str, Set[str]] = {}

    worker_runs = {}
    for worker in workers:
        task = asyncio.create_task(
            worker.run_get_tree(
                members_per_asset=members_per_asset,
                visited=visited,
                filtered_as_sets=(
                    irr_server_options.filters
                    if irr_server_options.filters is not None
                    else set()
                ),
                pbar=pbar,
            )
        )
        worker_runs[task] = worker

    try:
        await join_queue_or_workers(queue, worker_runs)
    except Exception as e:
        LOGGER.error("Got an exception getting tree", exc_info=task.exception())
        raise e

    if pbar:
        pbar.finish()
        pbar = None

    # if search is set, modify the members.
    if irr_server_options.search:
        members_per_asset = filter_autnum(
            query_object, set([irr_server_options.search]), members_per_asset
        )

    # we process the irrtree (remove edges, for instances in loops, even add
    # edges eventually, filtering asns)
    if irr_server_options.remove_recursivity:
        members_per_asset, removed_links = remove_recursivity_from_tree(
            root_as_set, members_per_asset
        )
        LOGGER.info(f"Removed the next links to cut recursivity: {removed_links}")

    # we resolve the asn objects of the processed irrtree.  Resolving is going
    # over all the asn objects and getting their prefixes.
    # TODO: We could also resolve as-sets for caching or for partial tree fetching.

    # Resolve tree
    # Normally, we'll get the prefixes for the aut-num objectes.
    asn_objects = set()
    for as_set in members_per_asset:
        for member in members_per_asset[as_set]:
            if "-" in member:
                continue
            asn_objects.add(member)

    pbar = None
    if not disable_progress_bar:
        widgets = [
            "Resolving AUTNUM prefixes. Resolved ",
            progressbar.Percentage(),
            " objects (",
            progressbar.Timer(),
            ")",
        ]
        pbar = progressbar.ProgressBar(
            widgets=widgets,
            max_value=len(asn_objects),
        )

    for autnum in asn_objects:
        queue.put_nowait(autnum)

    prefixes_per_autun: Dict[str, Set[str]] = {}

    worker_runs = {}
    for worker in workers:
        task = asyncio.create_task(
            worker.run_resolve_objects(
                prefixes_per_autun=prefixes_per_autun,
                pbar=pbar,
            )
        )
        worker_runs[task] = worker

    try:
        await join_queue_or_workers(queue, worker_runs)
    except Exception as e:
        LOGGER.error("Got an exception getting tree", exc_info=task.exception())
        raise e

    if pbar:
        pbar.finish()
        pbar = None

    # we dont need the workers anymore, let us terminate them
    for w in workers:
        await w.terminate()

    origin_ases_per_asset: Dict[str, FrozenSet[str]] = get_origin_asns_from_members(
        root_as_set, members_per_asset
    )

    # We can now calculate the irrtree ascii data.
    # We will calculate the number of prefixes per as-set, in recursive cases
    # many as-sets share the same set of origin_ases_per_asset, so we will
    # calcualte them from there.
    # the current algorithm would work better for recursive cases
    # TODO: Find a way of reuse sets to speed up the process
    assets_per_origin_as_set: Dict[FrozenSet[str], Set[str]] = {}
    for asset, origin_as_set in origin_ases_per_asset.items():
        assets_per_origin_as_set.setdefault(origin_as_set, set()).add(asset)

    number_prefixes_per_asset: Dict[str, int] = {}
    number_origin_asn_per_asset: Dict[str, int] = {}
    for origin_as_set, assets in assets_per_origin_as_set.items():
        this_prefix_set = set()
        for asn_origin in origin_as_set:
            if asn_origin not in prefixes_per_autun:
                raise Exception(f"We did not preload prefixes for {asn_origin}")
            this_prefix_set |= prefixes_per_autun[asn_origin]
        for asset in assets:
            number_prefixes_per_asset[asset] = len(this_prefix_set)
            number_origin_asn_per_asset[asset] = len(origin_as_set)

    # The number of prefixes per asn are taken directly from their sets
    number_prefixes_per_asn: Dict[str, int] = {}
    for asn in prefixes_per_autun:
        number_prefixes_per_asn[asn] = len(prefixes_per_autun[asn])

    # Ready to calculate the data for the irrtree
    asset_tree_data = ASSetTree(
        root_as_set=query_object, members_per_asset=members_per_asset
    )
    ascii_data = IRRAsciiTreeData(
        as_set_tree=asset_tree_data,
        number_prefixes_per_asn=number_prefixes_per_asn,
        number_origin_asn_per_asset=number_origin_asn_per_asset,
        number_prefixes_per_asset=number_prefixes_per_asset,
    )

    ascii_tree = print_asset_tree(ascii_data, irr_server_options, irr_treeoptions)

    return ascii_tree
