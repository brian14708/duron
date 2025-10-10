import pytest

from duron.log import (
    decode_id,
    derive_id,
    encode_id,
    random_id,
)


def test_generates_unique_ids() -> None:
    ids = {random_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_derive_id_deterministic() -> None:
    base = b"test base"
    key = b"test key"

    id1 = derive_id(base, key=key)
    id2 = derive_id(base, key=key)
    assert id1 == id2
    assert decode_id(encode_id(id1)) == id2


@pytest.mark.benchmark
def test_bench_derive_id() -> None:
    _ = decode_id(encode_id(derive_id(b"hello", context=b"key")))
