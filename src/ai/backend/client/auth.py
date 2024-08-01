import base64
import enum
import hashlib
import hmac
import secrets
from datetime import datetime
from typing import Mapping, Tuple

import attrs
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from yarl import URL

__all__ = (
    "AuthToken",
    "AuthTokenTypes",
    "generate_signature",
)


_iv_dict = [
    bytes([char]) for char in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
]


class AuthTokenTypes(enum.Enum):
    KEYPAIR = "keypair"
    JWT = "jwt"


@attrs.define()
class AuthToken:
    type = attrs.field(default=AuthTokenTypes.KEYPAIR)
    content = attrs.field(default=None)


def generate_signature(
    *,
    method: str,
    version: str,
    endpoint: URL,
    date: datetime,
    rel_url: str,
    content_type: str,
    access_key: str,
    secret_key: str,
    hash_type: str,
) -> Tuple[Mapping[str, str], str]:
    """
    Generates the API request signature from the given parameters.
    """
    hash_type = hash_type
    hostname = endpoint._val.netloc
    body_hash = hashlib.new(hash_type, b"").hexdigest()