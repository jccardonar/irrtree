from datetime import datetime
from typing import Dict, Tuple, Optional, Set, Any
import json

import asciitree
import irrtree
from irrtree.datamodels import (
    IRRAsciiTreeOptions,
    IRRAsciiTreeData,
    IRRServerOptions,
    MembersSorting,
    IRRTree,
    TIME_FORMAT,
    asdict,
)


def print_autnum(autum: str, prefix_number: int) -> str:
    res = "%s (%s pfxs)" % (autum, prefix_number)
    return res


def print_as_set(
    asset,
    origin_ases: int,
    prefixes: int,
    non_expansion_text: Optional[str],
    prefix_approximation: bool,
) -> str:
    if prefix_approximation:
        prefixes_word = "aprox_pfxs"
    else:
        prefixes_word = "pfxs"

    res = "%s (%s ASNs, %s %s)" % (
        asset,
        origin_ases,
        prefixes,
        prefixes_word,
    )
    if non_expansion_text:
        res = f"{res} - {non_expansion_text}"
    return res


def print_branch(
    asset: str,
    data: IRRAsciiTreeData,
    seen_assets: Set[str],
    level: int,
    irrtree_options: IRRAsciiTreeOptions,
) -> Tuple[str, IRRTree]:
    """
    Returns the key and branch for this asset
    """
    branch: IRRTree = {}
    is_seen = False
    if asset in seen_assets:
        is_seen = True
    seen_assets.add(asset)

    new_level = level + 1

    # we need to design if we expand this as-set or not
    not_expansion_reason: Optional[str] = None
    if is_seen:
        not_expansion_reason = "already expanded"
    elif irrtree_options.top_level and new_level > irrtree_options.top_level:
        not_expansion_reason = "top level reached"

    # if not_expansion_reason is None or empty, we move to expand the as-set

    prefixes = data.number_prefixes_per_asset[asset]
    origin_asns = data.number_origin_asn_per_asset[asset]
    if not_expansion_reason:
        asset_key = print_as_set(
            asset,
            origin_asns,
            prefixes,
            not_expansion_reason,
            data.prefix_approximation,
        )
        return (asset_key, branch)

    asset_key = print_as_set(
        asset, origin_asns, prefixes, None, data.prefix_approximation
    )

    filtered_members = set()
    filtered_direct_asns = set()

    # we will sort the data, so this is only temporal
    # key is the member key, data is the tree key, its subtree, number of prefixes and origin asns
    unsorted_member_data: Dict[str, Tuple[int, int]] = {}

    for member in data.as_set_tree.members_per_asset[asset]:
        if "-" not in member:
            # this is an autnum
            # do not show if not set
            if not irrtree_options.show_autnum:
                filtered_direct_asns.add(member)
                continue

            member_prefixes: int = data.number_prefixes_per_asn[member]

            # do not show if there is a filter for max number of prefixs
            if (
                irrtree_options.filter_less_prefixes_than
                and irrtree_options.filter_less_prefixes_than > member_prefixes
            ):
                filtered_direct_asns.add(member)
                continue

            unsorted_member_data[member] = (member_prefixes, 0)
            continue

        # now we deal with assets
        member_prefixes = data.number_prefixes_per_asset[member]
        if (
            irrtree_options.filter_less_prefixes_than
            and irrtree_options.filter_less_prefixes_than > member_prefixes
        ):
            # we will hide it
            filtered_members.add(member)
            continue

        # fill the metrics for the member
        unsorted_member_data[member] = (
            data.number_prefixes_per_asset[member],
            data.number_origin_asn_per_asset[member],
        )

    # let us find the metric for the members
    metric_per_member: Dict[str, Any] = {}
    if irrtree_options.sorting_option == MembersSorting.by_name:
        metric_per_member = {x: x for x in unsorted_member_data}
    elif irrtree_options.sorting_option == MembersSorting.by_prefix_count:
        # here it is prefix count, then asn count then name
        metric_per_member = {
            x: (-v[0], -v[1], x) for x, v in unsorted_member_data.items()
        }
    else:
        raise Exception("Option irrtree_options.sorting_option not supported")

    for member in sorted(unsorted_member_data, key=lambda x: metric_per_member[x]):
        member_prefixes, *_ = unsorted_member_data[member]

        # fill up the details
        if "-" in member:
            member_key, member_branch = print_branch(
                member, data, seen_assets, new_level, irrtree_options
            )
        else:
            member_key = print_autnum(member, member_prefixes)
            member_branch = {}
        branch[member_key] = member_branch

    # print special lines for hidden eleemnts
    if filtered_members:
        branch[
            f"{len(filtered_members)} AS-SET with less than {irrtree_options.filter_less_prefixes_than}"
        ] = {}

    if filtered_direct_asns:
        branch[f"{len(filtered_direct_asns)} direct AUTNUM objects"] = {}

    return (asset_key, branch)


def print_asset_tree(
    data: IRRAsciiTreeData,
    server_options: IRRServerOptions,
    irrtree_options: IRRAsciiTreeOptions,
) -> str:
    output = []
    if not server_options.date:
        date = datetime.now().strftime(TIME_FORMAT)
    else:
        date = server_options.date.strftime(TIME_FORMAT)

    root_object = data.as_set_tree.root_as_set
    output.append(
        "IRRTree (%s) report for '%s' (IPv%i), using %s at %s"
        % (
            irrtree.__version__,
            root_object,
            server_options.afi,
            server_options.irr_host,
            date,
        )
    )

    # the next (optional) line is the new extra info, for the graph building.
    extra_options = {}
    if server_options.filters:
        extra_options["filters"] = list(server_options.filters)
    if server_options.remove_recursivity:
        extra_options["remove_recursivity"] = server_options.remove_recursivity

    if extra_options:
        output.append(f"IRRTree extra options: {json.dumps(extra_options)}")

    # The next (optional) line for the printing options. We'll need to deal with the defaults
    # this is super mega cheating for now
    printing_options = {
        x: y for x, y in asdict(irrtree_options).items() if y is not None
    }
    if "show_autnum" in printing_options and printing_options["show_autnum"]:
        del printing_options["show_autnum"]
    if printing_options:
        output.append(f"IRRTree printing options: {json.dumps(printing_options)}")

    seen_assets: Set[str] = set()
    root_key, tree = print_branch(root_object, data, seen_assets, 0, irrtree_options)
    tr = asciitree.LeftAligned()
    output.append((tr({root_key: tree})))

    return "\n".join(output)
