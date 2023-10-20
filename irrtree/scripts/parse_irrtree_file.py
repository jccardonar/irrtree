import logging
from pathlib import Path
from typing import Dict, Set, FrozenSet, Optional
import copy
import argparse
import sys


import irrtree
from irrtree.irrtree_print import print_asset_tree
from irrtree.args_functions import (
    build_irr_treeoptions,
    add_args_for_tree_options,
    validate_asn,
)
from irrtree.process_functions import (
    get_origin_asns_from_members,
    filter_autnum,
    remove_recursivity_from_tree,
)
from irrtree.datamodels import IRRAsciiTreeData, ASSetTree, IRRServerOptions, is_as_set
from irrtree.irrtree_parser import parse_irrtree_return_irrasciitreedata

# The format will be modified later to add the as-set and the afi
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
LOGGER = logging.getLogger()


parser = argparse.ArgumentParser(
    prog="IRRTreeParser",
    description="Parses a file with irrtree output and provides processing capabilitites to it",
)

parser.add_argument(
    "--asset_filters",
    "-f",
    dest="asset_filters",
    help="List of filtered AS-SET objects",
)

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
    "--version", action="version", version=f"%(prog)s v{irrtree.__version__}"
)

parser.add_argument("irrtree_file", type=Path, help="Path to file with irrtree run")


def _filter_from_graph(
    asset: str,
    new_members: Dict[str, Set[str]],
    base_members: Dict[str, Set[str]],
    filtered: Set[str],
    seen: Set[str],
):
    if asset in seen or asset in filtered:
        return
    seen.add(asset)
    new_members[asset] = set()
    for member in base_members[asset]:
        if member in filtered:
            continue
        new_members[asset].add(member)
        if "-" not in member:
            # autnum
            continue
        new_members[asset].add(member)
        _filter_from_graph(member, new_members, base_members, filtered, seen)


def filter_from_graph(
    root_asset: str, base_members: Dict[str, Set[str]], filtered: Set[str]
) -> Dict[str, Set[str]]:
    """
    Excludes as-sets from an irrtree
    In cli.py, this is done while gathering the irrtree form the server.
    """
    new_members: Dict[str, Set[str]] = {}
    seen: Set[str] = set()
    _filter_from_graph(root_asset, new_members, base_members, filtered, seen)
    return new_members


def recalculate_irrtree(
    root_as_set: str,
    new_members_per_asset: Dict[str, Set[str]],
    old_irrtree_data: IRRAsciiTreeData,
) -> IRRAsciiTreeData:
    """
    Approximates the irrtree output based on new new_members_per_asset, and
    using the data from a base irrtree file data.
    Instead of calculating prefixes per as-set using the set of prefixes, we will
    calculate them summing the prefixes per as-set. This is an approximation.
    """
    # if the new_members_per_asset is the same, we dont need to recalculate
    # naything
    if new_members_per_asset == old_irrtree_data.as_set_tree.members_per_asset:
        return old_irrtree_data

    # Get origin asns per as-set
    origin_ases_per_asset: Dict[str, FrozenSet[str]] = get_origin_asns_from_members(
        root_as_set, new_members_per_asset
    )

    # Combine origin asnes per asset
    assets_per_origin_as_set: Dict[FrozenSet[str], Set[str]] = {}
    for asset, origin_as_set in origin_ases_per_asset.items():
        assets_per_origin_as_set.setdefault(origin_as_set, set()).add(asset)

    # calcualte prefixes per origin asns sets summing the prefixes per asn
    number_prefixes_per_asset: Dict[str, int] = {}
    number_origin_asn_per_asset: Dict[str, int] = {}
    for origin_as_set, assets in assets_per_origin_as_set.items():
        prefix_counter: int = 0
        for asn_origin in origin_as_set:
            if asn_origin not in old_irrtree_data.number_prefixes_per_asn:
                raise Exception(f"We did not preload prefixes for {asn_origin}")
            prefix_counter += old_irrtree_data.number_prefixes_per_asn[asn_origin]
        for asset in assets:
            number_prefixes_per_asset[asset] = prefix_counter
            number_origin_asn_per_asset[asset] = len(origin_as_set)

    # The number of prefixes per asn are taken directly from their sets
    number_prefixes_per_asn = old_irrtree_data.number_prefixes_per_asn

    asset_tree_data = ASSetTree(
        root_as_set=root_as_set, members_per_asset=new_members_per_asset
    )
    ascii_data = IRRAsciiTreeData(
        as_set_tree=asset_tree_data,
        number_prefixes_per_asn=number_prefixes_per_asn,
        number_origin_asn_per_asset=number_origin_asn_per_asset,
        number_prefixes_per_asset=number_prefixes_per_asset,
    )

    return ascii_data


def main() -> None:
    # load irrtree from file
    args = parser.parse_args()

    if args.debug:
        LOGGER.setLevel(logging.DEBUG)

    # Set file
    output_file: Optional[Path] = None
    if args.output_file:
        output_file = Path(args.output_file)

    irrtree_file = args.irrtree_file

    if not irrtree_file.is_file():
        raise Exception(f"{irrtree_file} is not a valid file")

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

    # if there is a new tree, recalculate irtree numbers based on auntum and summing values
    irr_treeoptions = build_irr_treeoptions(args)

    remove_recursivity = args.remove_recursivity

    LOGGER.debug("args: %s", args)

    LOGGER.debug("irr_treeoptions: %s", irr_treeoptions)

    # parse the file
    metadata, original_irrtreeascii_data = parse_irrtree_return_irrasciitreedata(
        irrtree_file.read_text()
    )
    root_as_set = metadata.as_set

    #
    irr_server_options = IRRServerOptions(
        irr_host=metadata.server,
        irr_port=9999,
        afi=metadata.ipversion,
        workers=1,
        filters=filters,
        search=args.search,
        remove_recursivity=remove_recursivity,
        date=metadata.date,
    )

    original_members_per_asset = (
        original_irrtreeascii_data.as_set_tree.members_per_asset
    )
    members_per_asset = copy.deepcopy(original_members_per_asset)

    # Remove assets
    # filters should be removed right in the memnbers_asets
    if filters:
        members_per_asset = filter_from_graph(root_as_set, members_per_asset, filters)

    # if search is set, modify the members.
    if irr_server_options.search:
        members_per_asset = filter_autnum(
            root_as_set, set([irr_server_options.search]), members_per_asset
        )

    # apply tree preprocessing
    if remove_recursivity:
        members_per_asset, removed_links = remove_recursivity_from_tree(
            root_as_set, members_per_asset
        )
        LOGGER.info(f"Removed the next links to cut recursivity: {removed_links}")

    # rebuild irrtree ascii data, if needed
    if original_members_per_asset != members_per_asset:
        final_irrtreedata = recalculate_irrtree(
            root_as_set, members_per_asset, original_irrtreeascii_data
        )
    else:
        final_irrtreedata = original_irrtreeascii_data

    # print tree
    irrtree_output = print_asset_tree(
        final_irrtreedata, irr_server_options, irr_treeoptions
    )

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
