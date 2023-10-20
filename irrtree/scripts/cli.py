# Copyright (C) 2015-2023 Job Snijders <job@instituut.net>, Juan Camilo Cardona
# <jccardona82@gmail.com>
#
# This file is part of IRRTree
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import argparse
import asyncio
from typing import Optional
from pathlib import Path
import logging
import sys

import irrtree
from irrtree.process_functions import irrtree_process
from irrtree.datamodels import (
    is_as_set,
    IRRServerOptions,
)
from irrtree.args_functions import (
    validate_asn,
    validate_positive_int,
    add_args_for_tree_options,
    build_irr_treeoptions,
)


# The format will be modified later to add the as-set and the afi
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
LOGGER = logging.getLogger()


parser = argparse.ArgumentParser(
    prog="IRRTree",
    description="Queries a IRRd for the composition of an AS-SET",
    epilog=(
        "Written by Job Snijders <job@instituut.net>, Juan Camilo Cardona <jccardona82@gmail.com>\nSource:"
        " https://github.com/job/irrtree"
    ),
)
parser.add_argument(
    "--list", "-l", dest="sources", help="List of sources (e.g.: RIPE,NTTCOM,RADB)"
)
parser.add_argument("--host", "-H", default="rr.ntt.net", help="Hostname to connect to")
parser.add_argument(
    "--port", "-p", type=int, default=43, help="Port on which IRRd runs"
)
parser.add_argument(
    "--connections",
    "-n",
    type=validate_positive_int,
    default=1,
    help="Number of connections to open to the IRRd",
)

parser.add_argument(
    "--max_restarts",
    type=validate_positive_int,
    default=3,
    help="Maximum number of restarts per connection. (default 3)",
)

# Mutually exclusive group for --ipv4 and --ipv6
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "--ipv4", "-4", action="store_true", help="Resolve IPv4 prefixes (default)"
)
group.add_argument("--ipv6", "-6", action="store_true", help="Resolve IPv6 prefixes")

parser.add_argument(
    "--asset_filters",
    "-f",
    dest="asset_filters",
    help="List of filtered AS-SET objects",
)

# Add an option to not add recursivity
parser.add_argument(
    "--remove_recursivity",
    action="store_true",
    help="Removes recursivity on the irrtree (for analysis purposes)",
)
parser.add_argument(
    "--search",
    "-s",
    type=validate_asn,
    help="Output only related to autnum (in ASXXX format)",
)

add_args_for_tree_options(parser)


parser.add_argument("--output_file", "-o", help="File to store the irrtree report.")
parser.add_argument("--debug", "-d", action="store_true", help="Enable debug mode")
parser.add_argument(
    "--disable_progress_bar",
    action="store_true",
    help="Disables progress bar.",
)
parser.add_argument(
    "--version", action="version", version=f"%(prog)s v{irrtree.__version__}"
)

parser.add_argument("as_set", help="ASSET to query")


def main() -> None:
    args = parser.parse_args()

    if args.debug:
        LOGGER.setLevel(logging.DEBUG)

    LOGGER.debug("Args are %s", args)

    if not is_as_set(args.as_set):
        raise Exception(f"{args.as_set} is not a valid as-set")

    root_as_set = args.as_set

    # the logic for afi is a bit
    afi = 4
    if args.ipv6:
        afi = 6

    # Mofify logging format to add the as-set and the afi
    for handler in LOGGER.handlers:
        handler.setFormatter(
            logging.Formatter(
                f'%(asctime)s [%(levelname)s] {{"AS-SET": "{args.as_set}", "AFI":'
                f" {afi}}} %(message)s"
            )
        )

    # Set file
    output_file: Optional[Path] = None
    if args.output_file:
        output_file = Path(args.output_file)

    # deal with filtered as-sets
    filters = set()
    if args.asset_filters:
        invalid = set()
        for candidate_filter in args.asset_filters.split(","):
            candidate_filter = candidate_filter.strip()
            if not is_as_set(candidate_filter):
                invalid.add(candidate_filter)
                continue
            filters.add(candidate_filter)
        if invalid:
            raise Exception(f"The next are not valid as-sets for filters: {invalid}")

    irr_server_options = IRRServerOptions(
        irr_host=args.host,
        irr_port=args.port,
        afi=afi,
        sources_list=args.sources,
        max_restarts=args.max_restarts,
        workers=args.connections,
        filters=filters,
        search=args.search,
        remove_recursivity=args.remove_recursivity,
    )

    irr_treeoptions = build_irr_treeoptions(args)

    LOGGER.debug("irr_server_options: %s", irr_server_options)
    LOGGER.debug("irr_treeoptions: %s", irr_treeoptions)

    try:
        irrtree_output = asyncio.run(
            irrtree_process(
                root_as_set,
                irr_server_options,
                irr_treeoptions,
                args.debug,
                args.disable_progress_bar,
            )
        )
    except Exception as e:
        LOGGER.error("Exiting with unhandled exception", exc_info=e)
        raise

    # print in screen if there is not output file
    if output_file is None:
        print(irrtree_output)
    else:
        try:
            output_file.write_text(irrtree_output)
        except Exception as e:
            LOGGER.error("Error writting to file", exc_info=e)
            raise


if __name__ == "__main__":
    main()
