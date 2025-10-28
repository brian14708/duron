from __future__ import annotations

from asyncio import CancelledError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from duron.log._entry import ErrorInfo


def encode_error(error: Exception | CancelledError) -> ErrorInfo:
    if isinstance(error, CancelledError):
        return {"code": -2, "message": repr(error)}
    return {"code": -1, "message": repr(error)}


def decode_error(error_info: ErrorInfo) -> Exception | CancelledError:
    if error_info["code"] == -2:
        return CancelledError()
    return Exception(f"[{error_info['code']}] {error_info['message']}")
