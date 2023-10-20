import re
from datetime import datetime
from dataclasses import dataclass, asdict

from enum import Enum

from typing import Optional, Set, Dict, Iterable, Tuple, Any, List
import typing_extensions


class ParseException(Exception):
    pass


# TypeAlias
IRRTree = Dict[str, "IRRTree"]

re_asn = re.compile(r"^[aA][sS]\d+")
re_starts_asn = re.compile(r"^[aA][sS]")
re_as_set = re.compile(r"^[aA][sS]-.*")

TIME_FORMAT = "%Y-%m-%d %H:%M"


def is_as_set(as_set_text: str) -> bool:
    if not re_starts_asn.match(as_set_text):
        return False
    if "-" in as_set_text:
        return True
    return False


# from https://github.com/pydantic/pydantic/blob/0c54c35ba61901d355734f9af3e4395af561f88c/pydantic/_internal/_repr.py#L37
ReprArgs: typing_extensions.TypeAlias = Iterable[Tuple[Optional[str], Any]]


@dataclass
class IRRServerOptions:
    """
    Sets the optiosn to connect and fetch data from the irr server, and process
    the data
    """

    irr_host: str
    irr_port: int
    afi: int
    workers: int  # it should be conint(ge=1) if using pydantic
    sources_list: Optional[str] = None

    max_restarts: int = 3  # it should be conint(ge=1) if using pydantic
    date: Optional[datetime] = None

    # as-sets to remove from the irrtree
    filters: Optional[Set[str]] = None
    search: Optional[str] = None
    # remove cycles
    remove_recursivity: bool = False


@dataclass
class ASSetTree:
    """
    Contains the root object and the members.  It should be traversed
    recusively to get the tree (get the member of the root object, then the
    member of its members, etc).
    Members include other AS-SETs and AUTNUM (e.g. ASNs)
    """

    root_as_set: str
    members_per_asset: Dict[str, Set[str]]


class MembersSorting(str, Enum):
    by_prefix_count = "by_prefix_count"
    by_name = "by_name"


@dataclass
class IRRAsciiTreeOptions:
    """
    Groups the options for building the asciitre object used to build the tree
    """

    sorting_option: MembersSorting
    filter_less_prefixes_than: Optional[int] = None
    top_level: Optional[int] = None  #  it should be conint(ge=1) if using pydantic
    show_autnum: bool = True


@dataclass
class IRRAsciiTreeData:
    """
    Data needed to build an irrasciitree (plus the build options grouped in
    IRRAsciiTreeOptions)
    """

    as_set_tree: ASSetTree
    number_prefixes_per_asn: Dict[str, int]
    # this is sort of a cache, since it can be obtained directly from as_set_tree
    number_origin_asn_per_asset: Dict[str, int]
    number_prefixes_per_asset: Dict[str, int]
    prefix_approximation: bool = False

    def num_prefixes_per_object(self, obj: str) -> int:
        if "-" in obj:
            return self.number_prefixes_per_asset[obj]
        return self.number_prefixes_per_asn[obj]


# RE for parsing the first line of the irrtree
first_line_parser = re.compile(
    r"^IRRTree \((.*)\) report for '(.*)' \((.*)\), using (.*) at (.*)$"
)


@dataclass
class IrrRunData:
    """
    Class for holding the metadata for a irrtree run
    """

    irr_version: str
    as_set: str
    ipversion: int
    server: str
    date: datetime

    def compare(self, other: "IrrRunData") -> Tuple[List[str], List[str]]:
        """
        Compare two pieces of metadata.
        Returns two lists with informational msgs, and a second one with warnings
        """
        warnings: List[str] = []
        msgs: List[str] = []
        results = (msgs, warnings)
        if self == other:
            warnings.append("The files seem to be equal")
            return results

        self_dict = asdict(self)
        other_dict = asdict(other)

        del self_dict["date"]
        del other_dict["date"]

        if self_dict != other_dict:
            warnings.append(
                f"Some of the core metadata is different from one file to the other, from {self_dict} to {other_dict}"
            )
            msgs.append(f"Comparing {repr(self)} to {repr(other)}")
        else:
            msgs.append(
                f"Comparing {self.as_set} from date {self.date} to {other.date}"
            )

        return results

    @classmethod
    def parse_first_line(cls, line: str) -> "IrrRunData":
        """
        Returns IrrRunData from text
        """
        result = first_line_parser.match(line)
        if result is None:
            raise ParseException(f"Error parsing irr data from line '{line}'")
        groups = result.groups()
        irr_version = groups[0]
        as_set = groups[1]
        version_text = groups[2]
        if version_text == "IPv4":
            version = 4
        elif version_text == "IPv6":
            version = 6
        else:
            raise ParseException(f"Version text {version_text} not recognized")
        server = groups[3]
        date = datetime.strptime(groups[4], TIME_FORMAT)
        return cls(
            irr_version=irr_version,
            as_set=as_set,
            ipversion=version,
            server=server,
            date=date,
        )

    def __str__(self):
        """
        >>> t = IrrRunData.parse_first_line("IRRTree (1.4.0) report for 'AS-ALSARD-SET' (IPv4), using rr.ntt.net at 2022-08-20 02:57")
        >>> str(t)
        "IRRTree (1.4.0) report for 'AS-ALSARD-SET' (IPv4), using rr.ntt.net at 2022-08-20 02:57"
        """
        return f"IRRTree ({self.irr_version}) report for '{self.as_set}' (IPv{self.ipversion}), using {self.server} at {self.date.strftime('%Y-%m-%d %H:%M')}"
