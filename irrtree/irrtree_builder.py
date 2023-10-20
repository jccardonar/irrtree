from typing import Dict, Any, Set
import asciitree

from .irrtree_parser import IrrRunData, ASDataType, ASMembersType, ParseException


def build_ascii_tree(
    as_set: str,
    as_sets_data: ASDataType,
    as_sets_members: ASMembersType,
    tree: Dict[str, Any],
    seen: Set[str],
) -> None:
    """
    Builds the tree reesembling the one use in irrtree to create its output.
    Recursive. Does not return anything but populates the tree object.
    The trick is to add the  - already expanded" when needed
    The tree consists in a recusive TreeType = Dict[str, Union[str, TreeType],
    that we cannot obtain with python yet the str is an as-set with its data,
    and the children are the contained as-sets.
    If an as-set was expanded, it does not have children and you add the  -
    already expanded
    """
    # here dict should be ordered, so this puts a limit in the python version
    # we can run
    if as_set in seen:
        raise ParseException(f"Found {as_set} in seen")
    seen.add(as_set)

    # default sorting for the tree as done in irrtree
    # HOWEVER, it would be better to sort by name to
    # minimize diffs
    def sort_key(as_set):
        data = as_sets_data[as_set]
        return (
            0 if not data.asn_count else -data.asn_count,
            0 if not data.pfx_count else -data.pfx_count,
            data.as_set,
        )

    sorted_members = sorted(as_sets_members[as_set], key=sort_key)
    for member in sorted_members:
        # print the autnum
        if "-" not in member:
            tree[f"{as_sets_data[member]}"] = {}
            continue
        # print the as-sets. If already expanded, dont do it recursively
        if member in seen:
            tree[f"{as_sets_data[member]} - already expanded"] = {}
        else:
            member_tree: Dict[str, Any] = {}
            build_ascii_tree(member, as_sets_data, as_sets_members, member_tree, seen)
            tree[f"{as_sets_data[member]}"] = member_tree


def build_irrtree_content(
    metadata: IrrRunData, as_sets_data: ASDataType, as_sets_members: ASMembersType
) -> str:
    """
    Returns the same text as the irrtree from the metadata, as_sets_data and
    as_sets_members
    """
    seen: Set[str] = set()
    ascii_tree: Dict[str, Any] = {}
    ascii_tree[f"{as_sets_data[metadata.as_set]}"] = {}
    build_ascii_tree(
        metadata.as_set,
        as_sets_data,
        as_sets_members,
        ascii_tree[f"{as_sets_data[metadata.as_set]}"],
        seen,
    )
    tr = asciitree.LeftAligned()
    text = tr(ascii_tree)
    return f"{metadata}\n{text}"
