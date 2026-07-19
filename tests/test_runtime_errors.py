from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

import duron.errors
from duron._runtime.execution.errors import decode_error, encode_error
from duron._runtime.execution.models import (
    EffectIdentity,
    ExecutionIdentity,
    InvocationIdentity,
)
from duron._runtime.storage.helper import validate_error

if TYPE_CHECKING:
    from duron._runtime.storage.entry import ErrorInfo


def unknown_outcome() -> duron.errors.EffectOutcomeUnknownError:
    return duron.errors.EffectOutcomeUnknownError(
        ExecutionIdentity(
            "seed", "operation", EffectIdentity("effect", "1").effect_id, "input"
        ),
        InvocationIdentity(2, 5, 3),
    )


def test_unknown_outcome_reconstructs_independently_of_declared_effect_errors() -> None:
    original = unknown_outcome()
    original.__cause__ = ValueError("cause")
    encoded = encode_error(original)
    wire = cast("ErrorInfo", json.loads(json.dumps(encoded)))
    for declarations in ((), (ValueError,)):
        replayed = decode_error(wire, declarations)
        assert type(replayed) is duron.errors.EffectOutcomeUnknownError
        assert replayed.identity == original.identity
        assert replayed.invocation == original.invocation
        assert replayed.logical_attempt == 2
        assert replayed.invocation_number == 5
        assert replayed.recovery_count == 3
        assert replayed.args == original.args
        assert str(replayed) == str(original)
        assert encode_error(replayed) == encoded


@pytest.mark.parametrize(
    "change", ["kind", "version", "counter_gap", "missing_discriminator"]
)
def test_unknown_outcome_rejects_unknown_discriminators_and_malformed_fields(
    change: str,
) -> None:
    value = cast("dict[str, object]", encode_error(unknown_outcome()))
    runtime = cast("dict[str, object]", value["runtime"])
    if change == "kind":
        runtime["kind"] = "import.arbitrary.Exception"
    elif change == "version":
        runtime["version"] = 2
    elif change == "counter_gap":
        cast("dict[str, object]", runtime["invocation"])["invocation_number"] = 7
    else:
        del value["runtime"]
    with pytest.raises(duron.errors.CorruptLogError) as caught:
        validate_error(value, 42)
    assert caught.value.offset == 42
    with pytest.raises(duron.errors.CorruptLogError):
        decode_error(cast("ErrorInfo", value))
