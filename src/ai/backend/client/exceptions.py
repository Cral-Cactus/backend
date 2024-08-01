import json
from typing import Any

__all__ = (
    "BackendError",
    "BackendAPIError",
    "BackendClientError",
    "APIVersionWarning",
)


class BackendError(Exception):
    def __str__(self):
        return repr(self)


class BackendAPIError(BackendError):
    """Exceptions returned by the API gateway."""

    def __init__(self, status: int, reason: str, data: Any):
        if isinstance(data, (str, bytes)):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                data = {
                    "title": "Generic Error (could not parse error string)",
                    "content": data,
                }
        super().__init__(status, reason, data)

    @property
    def status(self) -> int:
        return self.args[0]

    @property
    def reason(self) -> str:
        return self.args[1]

    @property
    def data(self) -> Any:
        return self.args[2]
