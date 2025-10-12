from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from duron.codec import DefaultCodec

if TYPE_CHECKING:
    from duron.codec import Codec


@dataclass(slots=True)
class _Config:
    codec: Codec


config = _Config(
    codec=DefaultCodec(),
)


def set_config(
    *,
    codec: Codec | None = None,
) -> None:
    if codec is not None:
        config.codec = codec
