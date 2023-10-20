import pytest
from irrtree.parse_ascii_tree import parse_ascii_tree
import asciitree
from pathlib import Path

EXAMPLE = """asciitree
 +-- sometimes
 |   +-- you
 +-- just
 |   +-- want
 |       +-- to
 |       +-- draw
 +-- trees
 +-- in
     +-- your
         +-- terminal
"""

CURRENT_FOLDER = Path(__file__).parent.resolve()


def test_example():
    tree = parse_ascii_tree(EXAMPLE)
    tr = asciitree.LeftAligned()
    assert tr(tree).strip() == EXAMPLE.strip()


@pytest.mark.parametrize("file_path", [CURRENT_FOLDER / "example__ascii_tree_file.txt"])
def test_files(file_path: Path):
    text = file_path.read_text()
    tree = parse_ascii_tree(text)
    tr = asciitree.LeftAligned()
    assert tr(tree).strip() == text.strip()
