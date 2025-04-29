import hashlib
from uuid import uuid5, NAMESPACE_URL


def create_deterministic_md5(text):
    """Creates a deterministic hash based on a string

    Parameters
    ----------
    text : str
        Any text

    Returns
    -------
    str
        A hash of the text
    """
    return hashlib.md5(text.encode()).hexdigest()


def make_uuid(text: str, namespace_name: str, return_hex=False):
    namespace_uuid = uuid5(NAMESPACE_URL, namespace_name)
    uuid_obj = uuid5(namespace_uuid, name=text)
    if return_hex:
        return uuid_obj.hex
    return uuid_obj
