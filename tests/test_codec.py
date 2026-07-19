from __future__ import annotations

import math

import pytest

from duron.codec import DefaultCodec


def test_default_codec_rejects_nested_nonfinite_values() -> None:
    with pytest.raises(TypeError, match="not JSON-serializable"):
        DefaultCodec.encode_json({"values": [math.nan]}, object)
