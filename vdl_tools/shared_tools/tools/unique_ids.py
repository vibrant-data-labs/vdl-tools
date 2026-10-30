import hashlib
import json
from uuid import NAMESPACE_URL, uuid5


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


def create_deterministic_sha256(value):
    """SHA-256 of ``value`` serialized as canonical JSON (sorted keys, ``str`` fallback).

    Parameters
    ----------
    value : Any
        Strings, numbers, lists, tuples or dicts (nested), or anything whose
        leaves ``str()`` acceptably. Dict order does not affect the hash.

    Returns
    -------
    str
        64 hex characters.
    """
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()
