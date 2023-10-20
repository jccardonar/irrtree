import argparse
from typing import Any

from .datamodels import IRRAsciiTreeOptions, re_asn, MembersSorting


def validate_asn(asn_text: str) -> str:
    if not re_asn.match(asn_text):
        raise argparse.ArgumentTypeError(f"{asn_text} is not a valid asnum")
    return asn_text


def validate_positive_int(value) -> int:
    """
    Positive int validator for argparse
    """
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return ivalue


def add_args_for_tree_options(parser):
    tree_printing_options = parser.add_argument_group(
        "tree_printing_options", "Tree printing options"
    )

    # Mutually exclusive groups for sorting
    sorting_tree = tree_printing_options.add_mutually_exclusive_group()
    sorting_tree.add_argument(
        "--sort_by_name",
        action="store_const",
        dest="sorting",
        const="n",
        help="Sorts objects by name instead of number of prefixes.",
    )
    sorting_tree.add_argument(
        "--sort_by_prefixes",
        action="store_const",
        dest="sorting",
        const="p",
        help="Sorts objects by number of prefixes. (default)",
    )
    parser.set_defaults(sorting="p")

    tree_printing_options.add_argument(
        "--top_print_level",
        type=validate_positive_int,
        help="Limit levels to print in file",
    )

    tree_printing_options.add_argument(
        "--hide_autnum",
        action="store_true",
        help="Hides autnum objects from the tree",
    )

    tree_printing_options.add_argument(
        "--print_limit_number_prefixes",
        type=validate_positive_int,
        help=(
            "Limits the number of printed objects to those with more prefixes than the"
            " provided number (default: disabled)."
        ),
    )


def build_irr_treeoptions(args: Any) -> IRRAsciiTreeOptions:
    """
    Builds the IRRAsciiTreeOptions based on args. A bit loose on the types
    here, since it is too much work to define it for args
    """
    # find the filter sorting
    sorting = None
    if args.sorting == "n":
        sorting = MembersSorting.by_name
    elif args.sorting == "p":
        sorting = MembersSorting.by_prefix_count
    if not sorting:
        raise Exception(f"Sorting option {args.cons} not recognized")

    irr_treeoptions = IRRAsciiTreeOptions(
        sorting_option=sorting,
        filter_less_prefixes_than=args.print_limit_number_prefixes,
        top_level=args.top_print_level,
        show_autnum=not args.hide_autnum,
    )

    return irr_treeoptions
