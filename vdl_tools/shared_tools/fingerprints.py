"""Stable JSON identity shared by extraction and mapping."""

import hashlib
import json


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()
