from __future__ import annotations

import enum
from typing import Any, Callable, MutableMapping

from ai.backend.cli.types import Undefined, undefined

__all__ = (
    "Sentinel",
    "sentinel",
)


def set_if_set(
    target_dict: MutableMapping[str, Any],
    key: str,
    value: Any | Undefined,
    *,
    clean_func: Callable[[Any], Any] | None = None,
) -> None:
    if value is not undefined:
        if clean_func is not None:
            value = clean_func(value)
        target_dict[key] = value


class Sentinel(enum.Enum):

    TOKEN = 0

    def __bool__(self) -> bool:
        return False


sentinel = Sentinel.TOKEN