from typing import Dict, Set, FrozenSet, List, Tuple
from functools import reduce
import logging

from .process_functions import get_origin_asns
from .irrtree_parser import ASMembersType, IrrRunData

LOGGER = logging.getLogger()


def get_irr_output(
    metadata: IrrRunData, as_sets_members: ASMembersType
) -> Dict[str, FrozenSet[str]]:
    """
    Returns a DB with members and origin_asns for each as-set. This is similar
    to what Irrtree builds internally
    Origin asns are the as-sets of the form ASXYU (an as number)
    The as-sets are in the form AS-NAME or ASXYY:AS-NAME
    db is a Dict[str] with two keys members and origin_asns.
    Note: TypeDicts appeared in 3.8 use them for DB
    The recursive_as_sets folder holds TEMPORAL as-sets which are recursive
    (something typical in irr), if it returns something it means that a
    mentioned as-set was not expanded.
    """
    origin_ases_per_asset: Dict[str, FrozenSet[str]] = {}
    recursive_as_sets: Dict[str, Set[str]] = {}
    rp, _ = get_origin_asns(
        metadata.as_set,
        origin_ases_per_asset,
        as_sets_members,
        parents=[],
        recursive_as_sets=recursive_as_sets,
    )
    assert rp is None, "The parent tree depends on a as-set that is not included"
    assert (
        not recursive_as_sets
    ), "The recursive_as_sets contains unmanaged recursive as_sets"
    return origin_ases_per_asset


def _get_paths_to_autnum(
    asset: str,
    members_per_asset: ASMembersType,
    path: List[str],
    paths_per_autnum: Dict[str, Set[FrozenSet[str]]],
):
    new_path = path + [asset]
    for member in members_per_asset[asset]:
        if member in path:
            continue
        if "-" in member:
            _get_paths_to_autnum(member, members_per_asset, new_path, paths_per_autnum)
        else:
            # we will remove the root
            paths_per_autnum.setdefault(member, set()).add(
                frozenset(x for x in new_path[1:])
            )


def get_paths_to_autnum(
    root_as_set: str, members_per_asset: ASMembersType
) -> Dict[str, Set[FrozenSet[str]]]:
    """
    Gets the paths per autnum to the root but in terms of sets (to later find
    the common as-sets)
    We remove the root from the path
    """
    paths_per_autnum: Dict[str, Set[FrozenSet[str]]] = {}
    _get_paths_to_autnum(root_as_set, members_per_asset, [], paths_per_autnum)
    return paths_per_autnum


def find_affected_prefixes_estimatino(
    paths_per_autnum: Dict[str, Set[FrozenSet[str]]],
    num_prefixes_per_autum: Dict[str, int],
) -> Dict[str, Tuple[int, int]]:
    """
    Finds an estimate of affected prefixes if filtered.
    Returns number of affected prefixes and number of autnum
    """
    affected_autnum_if_filteres: Dict[str, Set[str]] = {}
    for auntum, paths in paths_per_autnum.items():
        common = reduce(lambda x, y: x & y, paths)
        for as_set in common:
            affected_autnum_if_filteres.setdefault(as_set, set()).add(auntum)

    stats_per_as_set: Dict[str, Tuple[int, int]] = {}
    for asset, autumsets in affected_autnum_if_filteres.items():
        stats_per_as_set[asset] = (
            sum(num_prefixes_per_autum[x] for x in autumsets),
            len(autumsets),
        )

    return stats_per_as_set


def _find_origin_asn_per_parent_set(
    root_as_set: str,
    parent_set: FrozenSet[str],
    parents_sets: Dict[FrozenSet[set], Set[str]],
    parents_per_object: Dict[str, Set[str]],
    single_links_to_root_per_parent_set: Dict[FrozenSet[str], FrozenSet[str]],
):
    if parent_set in single_links_to_root_per_parent_set:
        return

    # this is a trivial case, if we are linked directly to the root, we really
    # cannot filter this element
    if root_as_set in parent_set:
        single_links_to_root_per_parent_set[parent_set] = frozenset()
        return

    common_links_per_parent: Set[FrozenSet[str]] = set()
    for parent in parent_set:
        parent_parent_set: FrozenSet[str] = frozenset(parents_per_object[parent])
        if parent_parent_set not in single_links_to_root_per_parent_set:
            _find_origin_asn_per_parent_set(
                root_as_set,
                parent_parent_set,
                parents_sets,
                parents_per_object,
                single_links_to_root_per_parent_set,
            )
        # we need to add the parent links and the parent itself
        common_links_per_parent.add(
            single_links_to_root_per_parent_set[parent_parent_set] | frozenset([parent])
        )

    if common_links_per_parent:
        all_parents_common_links: Set[str] = set(
            reduce(lambda x, y: x & y, common_links_per_parent)
        )
    else:
        all_parents_common_links = set()

    single_links_to_root_per_parent_set[parent_set] = frozenset(
        all_parents_common_links
    )


def find_origin_asn_single_connected(
    root_as_set: str,
    members_per_asset: ASMembersType,
    parents_per_object: Dict[str, Set[str]],
) -> Dict[str, Set[str]]:
    """
    Finds the autnum that will be removed from the root_as_set when excluding
    each as-set.
    It finds the the paths towards the root (after getting recursively the
    single links of the parents), and finds if there are common elements among
    all paths
    This is an alternative to calculating directly the effect of eliminating
    the each as set, which did not scale for large as-sets
    TODO: Still in development
    """

    parents_sets: Dict[FrozenSet[str], Set[str]] = {}
    for element, this_element_parent_set in parents_per_object.items():
        parents_sets.setdefault(frozenset(this_element_parent_set), set()).add(element)

    single_links_to_root_per_parent_set: Dict[FrozenSet[str], FrozenSet[str]] = {}
    for parent_set in parents_sets:
        _find_origin_asn_per_parent_set(
            root_as_set,
            parent_set,
            parents_sets,
            parents_per_object,
            single_links_to_root_per_parent_set,
        )

    # now find for each object
    affected_autnum_per_asset: Dict[str, Set[str]] = {}
    for parent_set, single_links in single_links_to_root_per_parent_set.items():
        for element in parents_sets.get(parent_set, set()):
            if "-" in element:
                continue
            for asset in single_links:
                affected_autnum_per_asset.setdefault(asset, set()).add(element)

    return affected_autnum_per_asset


def find_parents_per_member(
    asset: str,
    members_per_asset: ASMembersType,
    parents_per_object: Dict[str, Set[str]],
    parents: Set[str],
    level: int,
    visited_assets: Set[str],
):
    if asset in parents:
        return
    if asset in visited_assets:
        return
    visited_assets.add(asset)
    new_parents = parents | set([asset])
    for member in members_per_asset[asset]:
        if member in new_parents:
            continue
        parents_per_object.setdefault(member, set()).add(asset)
        if "-" in member:
            find_parents_per_member(
                member,
                members_per_asset,
                parents_per_object,
                new_parents,
                level + 1,
                visited_assets,
            )
