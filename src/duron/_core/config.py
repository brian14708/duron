from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from duron.codec import DefaultCodec

if TYPE_CHECKING:
    from duron.codec import Codec


@dataclass(slots=True)
class _Config:
    codec: Codec
    debug: bool


config = _Config(
    codec=DefaultCodec(),
    debug=os.getenv("DURON_DEBUG", "0").lower() in {"1", "true"},
)


def set_config(
    *,
    codec: Codec | None = None,
    debug: bool | None = None,
) -> None:
    if codec is not None:
        config.codec = codec
    if debug is not None:
        config.debug = debug
