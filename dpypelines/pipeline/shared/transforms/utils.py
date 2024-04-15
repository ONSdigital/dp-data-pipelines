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
     elif type(dictionary[key]) == list:
         dictionary[key].append(value)
     else:
         dictionary[key] = [dictionary[key], value]

def set_key(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = value
    elif isinstance(dictionary[key], list):
        dictionary[key].append(value)
    else:
        dictionary[key] = [dictionary[key], value]


def flatten_dict(dd, separator=" ", prefix=""):
    """
    Flattens a nested dictionary. dd is the input nested data dictionary.
    The input values of seperator and prefix above are the default values.
    """
    return (
        {
            (
                prefix + separator + k.replace("message:", "").replace("common:", "")
                if prefix
                else k.replace("message:", "")
            ): v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dd, dict)
        else {prefix: dd}
    )


def convert(tup, di):
    """
    Converts each record of tuple dictionary tup to a flat 'concatenated' dictionary di which
     can easily be converted to a dataframe. The function returns a concatenated dictionary di.
    """
    for k, v in tup.items():
        di.setdefault(k, []).append(v)
    return di
