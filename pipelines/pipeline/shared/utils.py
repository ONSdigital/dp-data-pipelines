import string

def pathify_func(text: str):
    """This function was made to pathify a string and return it"""

    no_punctuation = text.translate(str.maketrans('','', string.punctuation))

    pathified = no_punctuation.replace(' ', '-')

    return pathified.lower()