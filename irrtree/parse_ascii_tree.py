from .datamodels import IRRTree, ParseException


ASCII_POINTER = "+--"
DIVISOR = len(ASCII_POINTER) + 1


def parse_ascii_tree(text: str) -> IRRTree:
    """
    Parses an ascii tree formated with default values
    of the ascii tree LeftAligned() object
    """
    tree: IRRTree = {}
    current_hierarchy = []
    for n, line in enumerate(text.split("\n")):
        if not line:
            continue
        if n == 0:
            # head of the tree
            key = line
            tree[line] = {}
            current_hierarchy.append(tree[line])
            continue
        else:
            if ASCII_POINTER not in line:
                raise ParseException(f"Line {n} does not contain {ASCII_POINTER}")
            # the +1 at the end avoids a space
            key = line[line.find(ASCII_POINTER) + len(ASCII_POINTER) + 1 :]
            element_till_pointer = line[: line.find(ASCII_POINTER)]
            elements_minus_space = len(element_till_pointer) - 1
            if elements_minus_space % DIVISOR:
                raise ParseException(
                    f"Line {n} does not contain a number of chrs dividible by {DIVISOR} after removing the first space ({elements_minus_space})"
                )
            hierarchy = elements_minus_space // DIVISOR
            current_tree = current_hierarchy[hierarchy]
            current_tree[key] = {}
            current_hierarchy = current_hierarchy[: hierarchy + 1] + [current_tree[key]]
    return tree
