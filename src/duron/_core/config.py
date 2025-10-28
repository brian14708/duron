from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from duron.codec._base import DefaultCodec

if TYPE_CHECKING:
    from duron.codec import Codec


@dataclass(slots=True)
class _Config:
    codec: Codec


config = _Config(codec=DefaultCodec)


def set_config(*, codec: Codec | None = None) -> None:
    """Set global configuration for Duron.

    Args:
        codec: The codec to use for serializing and deserializing data.

    """
    if codec is not None:
        config.codec = codec
