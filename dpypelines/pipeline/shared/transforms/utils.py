import re

from unidecode import unidecode


def pathify(label):
    """
    Convert a label into something that can be used in a URI path segment.
    """
    return re.sub(
        r"-$", "", re.sub(r"-+", "-", re.sub(r"[^\w/]", "-", unidecode(label).lower()))
    )


def set_key(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = value
    elif isinstance(dictionary[key], list):
        dictionary[key].append(value)
    else:
        dictionary[key] = [dictionary[key], value]
