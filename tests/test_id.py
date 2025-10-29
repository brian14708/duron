import pytest

from duron.loop import _derive_id, _random_id  # pyright: ignore[reportPrivateUsage]


def test_generates_unique_ids() -> None:
    ids = {_random_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_derive_id_deterministic() -> None:
    base = _random_id()
    key = b"test key"

    id1 = _derive_id(base, key=key)
    id2 = _derive_id(base, key=key)
    assert id1 == id2
    assert id1 != base


@pytest.mark.benchmark
def test_bench_derive_id() -> None:
    _ = _derive_id(_random_id(), context=b"key")
