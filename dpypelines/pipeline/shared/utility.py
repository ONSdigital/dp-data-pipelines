import os


def enrich_online(message_creating_function):
    def wrapper(*args, **kwargs):
        msg: str = message_creating_function(*args, **kwargs)

        # Get the enviormentl variable
        enrich_message = os.environ.get("ENRICH_OUTGOING_MESSAGES", None)
        # If there is a value stored add (enrich) message otherwise return original message
        if enrich_message is not None:
            return msg + " " + enrich_message
        else:
            return msg

    return wrapper


# devnote: not using strtobool from disutils as that
# package is being depreciate from the standard
# library in python >3.12
def str_to_bool(should_be_bool: str) -> bool:
    """
    Take a string that should represent a boolean
    and convert it to bool.

    Raise if we've an unexpected value.
    """

    assert isinstance(
        should_be_bool, str
    ), f"Function str_to_bool only accepts strings, got {type(should_be_bool)}"

    consistant_should_be_bool = should_be_bool.strip().lower()

    if consistant_should_be_bool == "true":
        return True
    elif consistant_should_be_bool == "false":
        return False
    else:
        raise ValueError(
            f"A str value representing a boolean should be one of 'True', 'true', 'False', 'false'. Got '{should_be_bool}'"
        )
