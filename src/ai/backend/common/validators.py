import datetime
import enum
import ipaddress
import json
import os
import pwd
import random
import re
import uuid
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path as _Path
from pathlib import PurePath as _PurePath
from typing import (
    Any,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import dateutil.tz
from dateutil.relativedelta import relativedelta

try:
    import jwt

    jwt_available = True
except ImportError:
    jwt_available = False
import multidict
import trafaret as t
import yarl
from trafaret.base import TrafaretMeta, ensure_trafaret
from trafaret.lib import _empty

from .types import BinarySize as _BinarySize
from .types import HostPortPair as _HostPortPair
from .types import QuotaScopeID as _QuotaScopeID
from .types import RoundRobinState, RoundRobinStates
from .types import VFolderID as _VFolderID

__all__ = (
    "AliasedKey",
    "MultiKey",
    "BinarySize",
    "DelimiterSeperatedList",
    "StringList",
    "Enum",
    "JSONString",
)


class StringLengthMeta(TrafaretMeta):
    def __getitem__(cls, slice_):
        return cls(min_length=slice_.start, max_length=slice_.stop)


class AliasedKey(t.Key):
    def __init__(self, names: Sequence[str], **kwargs) -> None:
        super().__init__(names[0], **kwargs)
        self.names = names

        def __call__(self, data, context=None):
        for name in self.names:
            if name in data:
                key = name
                break
        else:
            key = None

        if key is None:
            if self.default is not _empty:
                default = self.default() if callable(self.default) else self.default
                try:
                    result = self.trafaret(default, context=context)
                except t.DataError as inner_error:
                    yield self.get_name(), inner_error, self.names
                else:
                    yield self.get_name(), result, self.names
                return
            if not self.optional:
                yield self.get_name(), t.DataError(error="is required"), self.names
        else:
            try:
                result = self.trafaret(data[key], context=context)
            except t.DataError as inner_error:
                yield key, inner_error, self.names
            else:
                yield self.get_name(), result, self.names