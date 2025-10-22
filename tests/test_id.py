import pytest

from duron.loop import derive_id, random_id


def test_generates_unique_ids() -> None:
    ids = {random_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_derive_id_deterministic() -> None:
    base = random_id()
    key = b"test key"

    id1 = derive_id(base, key=key)
    id2 = derive_id(base, key=key)
    assert id1 == id2
    assert id1 != base


@pytest.mark.benchmark
def test_bench_derive_id() -> None:
    _ = derive_id(random_id(), context=b"key")
