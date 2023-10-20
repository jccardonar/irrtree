import pytest
from pathlib import Path
import irrtree
from irrtree.irrtree_parser import parse_irrtree, parse_irrtree_return_irrasciitreedata
from irrtree.irrtree_builder import build_irrtree_content
from irrtree.analyze_functions import get_irr_output
from irrtree.irrtree_print import print_asset_tree
from irrtree.datamodels import (
    IRRServerOptions,
    IRRAsciiTreeOptions,
    MembersSorting,
)

CURRENT_FOLDER = Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    "file_path",
    [
        CURRENT_FOLDER / "example_file.txt",
        CURRENT_FOLDER / "example_33763v4",
        CURRENT_FOLDER / "example_19518v4",
        # CURRENT_FOLDER / "example_60068v4", #this one fails, but from what I have seen, it is in the irrtree output
    ],
)
def test_files(file_path: Path):
    metadata, as_sets_data, as_sets_members = parse_irrtree(file_path.read_text())
    new_irr_text = build_irrtree_content(metadata, as_sets_data, as_sets_members)

    metadata2, as_sets_data2, as_sets_members2 = parse_irrtree(new_irr_text)

    assert metadata == metadata2
    assert as_sets_data == as_sets_data2
    assert as_sets_members == as_sets_members2

    # get irr_tree output
    irrtree_db = get_irr_output(metadata, as_sets_members)
    for as_set in as_sets_data:
        as_count = (
            0
            if as_sets_data[as_set].asn_count is None
            else as_sets_data[as_set].asn_count
        )
        if "-" not in as_set:
            assert as_count == 0
            continue
        assert as_count == len(irrtree_db[as_set])


@pytest.mark.parametrize(
    "file_path",
    [
        CURRENT_FOLDER / "example_file.txt",
        CURRENT_FOLDER / "example_33763v4",
        CURRENT_FOLDER / "example_19518v4",
        # CURRENT_FOLDER / "example_60068v4", #this one fails, but from what I have seen, it is in the irrtree output
    ],
)
def test_build_cycle(file_path: Path):
    metadata, original_irrtreeascii_data = parse_irrtree_return_irrasciitreedata(
        file_path.read_text()
    )
    metadata.irr_version = irrtree.__version__

    irr_server_options = IRRServerOptions(
        irr_host=metadata.server,
        irr_port=9999,
        afi=metadata.ipversion,
        workers=1,
        date=metadata.date,
    )

    irr_treeoptions = IRRAsciiTreeOptions(sorting_option=MembersSorting.by_prefix_count)

    new_irr_text = print_asset_tree(
        original_irrtreeascii_data, irr_server_options, irr_treeoptions
    )

    metadata2, irrtreeascii_data2 = parse_irrtree_return_irrasciitreedata(new_irr_text)

    assert metadata == metadata2
    assert irrtreeascii_data2 == original_irrtreeascii_data

    IRRServerOptions(
        irr_host=metadata2.server,
        irr_port=9999,
        afi=metadata2.ipversion,
        workers=1,
        date=metadata2.date,
    )

    # let us do it a thrid time, with the same options, text should be the same

    third_text = print_asset_tree(
        irrtreeascii_data2, irr_server_options, irr_treeoptions
    )

    assert third_text.strip() == new_irr_text.strip()
