from typing import Optional, Dict, Any, Tuple, List, Set
from dataclasses import dataclass, asdict
from .datamodels import (
    IRRAsciiTreeData,
    ASSetTree,
    IrrRunData,
)

from .parse_ascii_tree import parse_ascii_tree, ParseException


@dataclass
class AsSetSummaryData:
    """
    Holds IRR data for a single as-set.
    """

    as_set: str
    asn_count: Optional[int]
    pfx_count: Optional[int]

    def same_as_compare(self, other):
        if self.as_set != other.as_set:
            raise ParseException(
                f"Comparing two as-sets with differnet names, {self}->{other}"
            )
        if self == other:
            pass

    def __str__(self):
        """
        >>> str(AsSetSummaryData(as_set='AS-ALSARD-SET', asn_count=22462, pfx_count=865733))
        'AS-ALSARD-SET (22462 ASNs, 865733 pfxs)'
        >>> str(AsSetSummaryData(as_set='AS-ALSARD-SET', asn_count=0, pfx_count=0))
        'AS-ALSARD-SET (0 ASNs, 0 pfxs)'
        >>> str(AsSetSummaryData(as_set='AS-ALSARD-SET', asn_count=None, pfx_count=865733))
        'AS-ALSARD-SET (865733 pfxs)'
        >>> str(AsSetSummaryData(as_set='AS-ALSARD-SET', asn_count=1, pfx_count=None))
        'AS-ALSARD-SET (1 ASNs)'
        """
        parenthesis = []
        if self.asn_count is not None:
            parenthesis.append(f"{self.asn_count} ASNs")
        if self.pfx_count is not None:
            parenthesis.append(f"{self.pfx_count} pfxs")
        return f"{self.as_set} ({', '.join(parenthesis)})"

    @classmethod
    def parse_as_data(cls, as_data_strin: str) -> "AsSetSummaryData":
        """
        Gets the AsSetSummaryData from text
        >>> AsSetSummaryData.parse_as_data("AS-CIVITELE (95 ASNs, 1848 pfxs)")
        AsSetSummaryData(as_set='AS-CIVITELE', asn_count=95, pfx_count=1848)
        >>> AsSetSummaryData.parse_as_data("AS-CIVITELE (95 ASNs)")
        AsSetSummaryData(as_set='AS-CIVITELE', asn_count=95, pfx_count=None)
        >>> AsSetSummaryData.parse_as_data("AS-CIVITELE (1848 pfxs)")
        AsSetSummaryData(as_set='AS-CIVITELE', asn_count=None, pfx_count=1848)
        >>> AsSetSummaryData.parse_as_data("AS-CIVITELE (1848 pfxs)")
        AsSetSummaryData(as_set='AS-CIVITELE', asn_count=None, pfx_count=1848)
        >>> AsSetSummaryData.parse_as_data("AS-CIVITELE")
        AsSetSummaryData(as_set='AS-CIVITELE', asn_count=None, pfx_count=None)
        """
        asn_count: Optional[int] = None
        pfx_count: Optional[int] = None

        opening_bracket_pos = as_data_strin.find("(")

        if opening_bracket_pos >= 0:
            closening_bracket_pos = as_data_strin.find(")")
            if closening_bracket_pos < 0:
                raise ParseException("as-set data opening parenthesis but not closing")
            as_set = as_data_strin[: opening_bracket_pos - 1]
            parenthesis_txt = as_data_strin[
                opening_bracket_pos + 1 : closening_bracket_pos
            ]

            for pieces in parenthesis_txt.split(","):
                pieces = pieces.strip()
                splitted = pieces.split(" ")
                if len(splitted) != 2:
                    raise ParseException(
                        f"Problem parsing pieces in {as_data_strin}. Did not found a number and a text"
                    )
                number_txt, txt = splitted
                try:
                    number = int(number_txt)
                except Exception:
                    raise ParseException(
                        f"Problem parsing pieces in {as_data_strin}. first piece of {pieces} is not a number"
                    )
                if txt == "pfxs":
                    pfx_count = number
                elif txt == "ASNs":
                    asn_count = number
                else:
                    raise ParseException(
                        f"Problem parsing pieces in {as_data_strin}. Cannot recognize txt in {pieces}"
                    )

        else:
            as_set = as_data_strin.strip()
        return cls(as_set=as_set, asn_count=asn_count, pfx_count=pfx_count)


# The information coming from a IRRTREE consists of:
# AS-SET data ... data per as-set
# AS-SET members, the members per AS
# The metadata of the irrthree, IrrRunData, which contains the parent AS-SET
# TODO: These are TypeAlias, but only available directly in 3.10
ASDataType = Dict[str, AsSetSummaryData]
ASMembersType = Dict[str, Set[str]]


def _get_minimumlevels(
    level: int,
    level_parents: Set[str],
    level_parent_data: Dict[str, Tuple[int, Optional[str]]],
    members_per_asset: ASMembersType,
):
    new_parents = set()
    for parent in sorted(level_parents):
        for member in members_per_asset[parent]:
            # if there is data, we are done
            if member in level_parent_data:
                continue
            level_parent_data[member] = (level, parent)
            if "-" in member:
                new_parents.add(member)
    if new_parents:
        _get_minimumlevels(level + 1, new_parents, level_parent_data, members_per_asset)


def get_minimum_levels(
    root_as_set: str, members_per_asset: ASMembersType
) -> Dict[str, Tuple[int, Optional[str]]]:
    level_parent_data: Dict[str, Tuple[int, Optional[str]]] = {root_as_set: (0, None)}
    _get_minimumlevels(0, set([root_as_set]), level_parent_data, members_per_asset)
    return level_parent_data


def _get_levels(
    as_set: str,
    members_per_asset: ASMembersType,
    level_parent_data: Dict[str, Set[Tuple[int, Optional[str]]]],
    parents: Set[str],
    parent: Optional[str],
    level: int,
    keep_minimum: bool = True,
) -> None:
    if as_set not in level_parent_data:
        level_parent_data[as_set] = set()
    level_parent_data[as_set].add((level, parent))

    new_parents = parents | set([as_set])
    new_level = level + 1

    for member in members_per_asset[as_set]:
        if "-" not in member:
            if member not in level_parent_data:
                level_parent_data[member] = set()
            level_parent_data[member].add((new_level, as_set))
            continue
        if member in parents:
            continue

        _get_levels(
            member, members_per_asset, level_parent_data, new_parents, as_set, new_level
        )


def get_levels(
    root_as_set: str, members_per_asset: ASMembersType
) -> Dict[str, Set[Tuple[int, Optional[str]]]]:
    """
    For each object in members (both asset and autnum) returns the levels
    (without recursivity) and parents on each level
    """
    level_parent_data: Dict[str, Set[Tuple[int, Optional[str]]]] = {}
    _get_levels(
        root_as_set,
        members_per_asset,
        level_parent_data,
        parents=set(),
        parent=None,
        level=0,
    )
    return level_parent_data


def get_as_data(
    metadata: IrrRunData,
    as_members: ASMembersType,
    as_sets_data: ASDataType,
    keep_minimum_level=True,
) -> List[Dict[str, Any]]:
    df_one_data = []
    if not keep_minimum_level:
        levels = get_levels(metadata.as_set, as_members)
        for as_set, as_data in as_sets_data.items():
            as_data_dict = asdict(as_data)
            for level in levels[as_set]:
                this_dict = dict(as_data_dict)
                this_dict["level"] = level[0]
                this_dict["parent"] = level[1]
                df_one_data.append(this_dict)
    else:
        min_levels = get_minimum_levels(metadata.as_set, as_members)
        for as_set, as_data in as_sets_data.items():
            as_data_dict = asdict(as_data)
            min_level = min_levels[as_set]
            as_data_dict["level"] = min_level[0]
            as_data_dict["parent"] = min_level[1]
            df_one_data.append(as_data_dict)
    return df_one_data


def parse_key(tree_key: str) -> Tuple[AsSetSummaryData, bool]:
    """
    Parses the key of each as-set (e.g. AS-ALSARD-SET (865733 pfxs)) , gettting
    the data in a AsSetSummaryData
    It retuns a second value with True if the AS was expanded before on the tree
    """
    parenthesis_location = tree_key.find(")")
    text_before_parenthesis = tree_key[: parenthesis_location + 1]
    text_after_parenthesis = tree_key[parenthesis_location + 1 :].strip()
    if not text_after_parenthesis:
        already = False
    else:
        if text_after_parenthesis != "- already expanded":
            raise ParseException(
                f"Parsing a key {tree_key} with a text after ) different than - already expanded"
            )
        already = True
    data = AsSetSummaryData.parse_as_data(text_before_parenthesis)

    if already and "-" not in data.as_set:
        raise ParseException("Found already expanded in a non as-set object")

    return data, already


def get_irr_tree_data(
    parsed_tree: Dict[str, Any],
    parent_element: Optional[str],
    asset_data: ASDataType,
    asset_members: ASMembersType,
    level: int,
) -> None:
    """
    Parses an individual part of the ascii irrtree.
    Adds the information to the asset_data and asset_members
    Recursive, calls the same function on the children, if needed.
    It does not return anything
    """

    for n, element in enumerate(parsed_tree):
        # get the data, and check if it has been expanded
        as_data, already_expanded = parse_key(element)
        if parent_element is not None:
            # we should not be revisiting as-sets, since the irrtree takes care of this.
            if as_data.as_set in asset_members[parent_element]:
                raise ParseException(
                    f"Re-adding {as_data.as_set} to {parent_element}. This should not happen"
                )
            asset_members[parent_element].add(as_data.as_set)
        # we it is expanded, we check the data is the same.
        # this part is just for validation
        if already_expanded:
            if as_data.as_set not in asset_data:
                raise ParseException(
                    f"AS in key {element} says it is expanded, but it is not"
                )
            # test that the data is the same
            assert (
                as_data == asset_data[as_data.as_set]
            ), f"Found different data for as_set {as_data.as_set} in {element}"
            if parsed_tree[element]:
                raise ParseException(
                    f"Element {element} states it has been expanded, but it has elements"
                )
        else:
            # if it is an as-set, and it says it is not expanded, it should nto be there
            if as_data.as_set in asset_data:
                if "-" in as_data.as_set:
                    raise ParseException(
                        f"AS in key {element} says seems to be not expanded, but it has"
                    )
                else:
                    # if it is an AUTNUM, the data should bte the same
                    if not as_data == asset_data[as_data.as_set]:
                        raise ParseException(
                            f"AUTNUM {as_data.as_set} has differnet stats in different parts of the tree: {as_data} and {asset_data[as_data.as_set]}"
                        )
                continue

            # if the as-set is not expanded, start the data for this member and visit it.
            asset_members[as_data.as_set] = set()
            asset_data[as_data.as_set] = as_data
            get_irr_tree_data(
                parsed_tree[element],
                as_data.as_set,
                asset_data,
                asset_members,
                level + 1,
            )


def parse_irrtree(text: str) -> Tuple[IrrRunData, ASDataType, ASMembersType]:
    """
    Parses a text containing a ascii tree with the output of the irrtree app.
    """
    if not text:
        raise ParseException("Empty text provided to parse_irrtree")

    text = text.strip()

    try:
        first_line, irr_tree_ascii = text.split("\n", 1)
    except Exception:
        raise ParseException(
            f"Problem getting first line of file of '{text}'. File with a single line?"
        )

    irr_tree_ascii = irr_tree_ascii.strip()

    #  The metadata is the first line
    metadata = IrrRunData.parse_first_line(first_line)

    # we need to deal with the other two optional lines, for now we ignore them
    if irr_tree_ascii.startswith("IRRTree extra options:"):
        try:
            _, irr_tree_ascii = irr_tree_ascii.split("\n", 1)
        except Exception:
            raise ParseException(
                f"Problem getting extra options line of file of '{text}'. No more lines?"
            )
        irr_tree_ascii = irr_tree_ascii.strip()

    if irr_tree_ascii.startswith("IRRTree printing options:"):
        try:
            _, irr_tree_ascii = irr_tree_ascii.split("\n", 1)
        except Exception:
            raise ParseException(
                f"Problem getting printing options line of file of '{text}'. No more lines?"
            )
        irr_tree_ascii = irr_tree_ascii.strip()

    # we obtain the tree using the parse_ascii_tree function
    # and process it with get_irr_tree_data to obtain
    # the as_sets_members (members for each as-set)
    # and as_sets_data (the infor of pfx and prefixes per asn)
    as_sets_data: ASDataType = {}
    as_sets_members: ASMembersType = {}
    ascii_tree = parse_ascii_tree(irr_tree_ascii)
    get_irr_tree_data(ascii_tree, None, as_sets_data, as_sets_members, 0)
    # autnum should not have any members, check that and delete them
    for member in set(as_sets_members):
        if "-" in member:
            continue
        if as_sets_members[member]:
            raise ParseException(
                f"AUTNUM {member} has memmbers: {as_sets_members[member]}, this makes no sense"
            )
        del as_sets_members[member]

    return metadata, as_sets_data, as_sets_members


def convert_to_irrasciitree(
    metadata: IrrRunData, as_sets_data: ASDataType, as_sets_members: ASMembersType
) -> IRRAsciiTreeData:
    """
    I build the parser before the refactoring of irrtree so this function
    converts these two data structures
    """
    as_set_tree = ASSetTree(
        root_as_set=metadata.as_set, members_per_asset=as_sets_members
    )
    number_prefixes_per_asn: Dict[str, int] = {}
    # this is sort of a cache, since it can be obtained directly from as_set_tree
    number_origin_asn_per_asset: Dict[str, int] = {}
    number_prefixes_per_asset: Dict[str, int] = {}

    for element, data in as_sets_data.items():
        if "-" not in element:
            assert not data.asn_count, f"{element} si AUTNUM object with origin asns"
            number_prefixes_per_asn[element] = (
                data.pfx_count if data.pfx_count is not None else 0
            )
            continue
        # this is an asset
        assert data.pfx_count is not None
        assert data.asn_count is not None
        number_prefixes_per_asset[element] = data.pfx_count
        number_origin_asn_per_asset[element] = data.asn_count

    return IRRAsciiTreeData(
        as_set_tree=as_set_tree,
        number_prefixes_per_asn=number_prefixes_per_asn,
        number_origin_asn_per_asset=number_origin_asn_per_asset,
        number_prefixes_per_asset=number_prefixes_per_asset,
    )


def parse_irrtree_return_irrasciitreedata(
    text: str,
) -> Tuple[IrrRunData, IRRAsciiTreeData]:
    metadata, as_sets_data, as_sets_members = parse_irrtree(text)
    return metadata, convert_to_irrasciitree(metadata, as_sets_data, as_sets_members)
