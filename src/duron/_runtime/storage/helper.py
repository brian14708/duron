from __future__ import annotations

import binascii
import json
import os
from typing import TYPE_CHECKING, TypeGuard, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

    from duron._runtime.storage.entry import BaseEntry, Entry
    from duron.types import JSONValue

from duron._runtime.execution.handle_data import HandleReference
from duron._runtime.execution.models import canonical_json, digest
from duron._runtime.storage.backend import HistoryRecord
from duron._runtime.storage.entry import POST_ACTION_TARGETS, CorruptLogError
from duron.codec import is_json_value
from duron.errors import InvalidHandleError

MAX_POST_KEY_BYTES = 1024


def encode_entry(entry: Entry) -> HistoryRecord:
    """Validate and encode one runtime entry for opaque storage.

    Returns:
        The immutable serialized record.

    Raises:
        CorruptLogError: If the entry cannot be encoded.

    """
    validated = validate_entry(entry, 0)
    try:
        payload = json.dumps(validated, allow_nan=False, separators=(",", ":")).encode(
            "utf-8"
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise CorruptLogError(0, str(exc)) from exc
    return HistoryRecord(validated["id"], payload, _entry_stream_id(validated))


def decode_record(record: HistoryRecord, offset: int) -> Entry:
    """Decode one opaque record and verify its stored identity.

    Returns:
        The validated runtime entry.

    Raises:
        CorruptLogError: If the payload or stored identity is invalid.

    """
    try:
        value = json.loads(record.payload)
    except (UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise CorruptLogError(offset, f"invalid history payload: {exc}") from exc
    entry = validate_entry(value, offset)
    if entry["id"] != record.id:
        raise CorruptLogError(offset, "history record id does not match its payload")
    if _entry_stream_id(entry) != record.stream_id:
        raise CorruptLogError(offset, "history stream id does not match its payload")
    return entry


def _entry_stream_id(entry: Entry) -> str | None:
    if entry["type"] == "stream.create":
        return entry["id"]
    if entry["type"] == "stream.emit" or entry["type"] == "stream.complete":
        return entry["stream_id"]
    return None


_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "promise.create": (),
    "promise.complete": ("promise_id",),
    "stream.create": ("name",),
    "stream.emit": ("stream_id", "value"),
    "stream.complete": ("stream_id",),
    "signal.scope.exit": ("signal_id",),
    "signal.ack": (
        "signal_id",
        "message_id",
        "message_offset",
        "receiver_id",
        "invocation",
    ),
    "barrier": (),
    "post.receipt": (
        "format_version",
        "scope",
        "key",
        "action",
        "target",
        "payload",
        "payload_digest",
        "event_ids",
    ),
    "effect.attempt.start": ("promise_id", "invocation", "session_id"),
    "effect.retry.scheduled": (
        "promise_id",
        "invocation",
        "error",
        "classification",
        "not_before_us",
    ),
}


def set_metadata(entry: Entry, metadata: Mapping[str, JSONValue]) -> None:
    if metadata:
        m = entry.get("metadata")
        if m is None:
            entry["metadata"] = {**metadata}
        else:
            m.update(metadata)


def is_entry(entry: Entry | BaseEntry) -> TypeGuard[Entry]:
    # Derived from the required-fields registry so the set of valid entry types
    # has exactly one definition: a type added there is visible to every host
    # reader that gates on this predicate.
    type_name = entry.get("type")
    return isinstance(type_name, str) and type_name in _REQUIRED_FIELDS


def validate_error(value: object, offset: int) -> None:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "error must be an object")
    error = cast("dict[str, object]", value)
    for field, expected in (
        ("type", str),
        ("module", str),
        ("message", str),
        ("args", list),
        ("cancelled", bool),
        ("key", str),
    ):
        if not isinstance(error.get(field), expected):
            raise CorruptLogError(offset, f"missing or invalid error {field!r}")
    if "cause" in error:
        validate_error(error["cause"], offset)
    if (
        error["key"] == "duron.errors:EffectOutcomeUnknownError"
        and "runtime" not in error
    ):
        raise CorruptLogError(
            offset, "runtime error requires a versioned discriminator"
        )
    if "runtime" in error:
        _validate_runtime_error(error["runtime"], offset)
        if (
            error["key"] != "duron.errors:EffectOutcomeUnknownError"
            or error["module"] != "duron.errors"
            or error["type"] != "EffectOutcomeUnknownError"
            or error["cancelled"] is not False
        ):
            raise CorruptLogError(
                offset, "runtime error discriminator conflicts with error identity"
            )


def _validate_runtime_error(value: object, offset: int) -> None:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "runtime error discriminator must be an object")
    runtime = cast("dict[str, object]", value)
    if (
        set(runtime) != {"version", "kind", "identity", "invocation"}
        or type(runtime.get("version")) is not int
        or runtime["version"] != 1
        or runtime.get("kind") != "effect_outcome_unknown"
    ):
        raise CorruptLogError(offset, "unknown runtime error discriminator or version")
    identity_value = runtime["identity"]
    if not isinstance(identity_value, dict):
        raise CorruptLogError(offset, "runtime error identity must be an object")
    identity = cast("dict[str, object]", identity_value)
    if (
        set(identity)
        != {"format_version", "run_seed", "operation_id", "effect_id", "input_digest"}
        or type(identity.get("format_version")) is not int
        or identity["format_version"] != 1
        or any(
            not isinstance(identity.get(k), str) or not identity[k]
            for k in ("run_seed", "operation_id", "effect_id", "input_digest")
        )
    ):
        raise CorruptLogError(offset, "invalid runtime execution identity")
    _validate_invocation(runtime["invocation"], offset)


def _validate_invocation(value: object, offset: int) -> None:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "effect invocation must be an object")
    fields = cast("dict[str, object]", value)
    if (
        set(fields)
        != {"format_version", "logical_attempt", "invocation_number", "recovery_count"}
        or type(fields.get("format_version")) is not int
        or fields["format_version"] != 1
    ):
        raise CorruptLogError(offset, "invalid invocation identity version or fields")
    for key, minimum in (
        ("logical_attempt", 1),
        ("invocation_number", 1),
        ("recovery_count", 0),
    ):
        number = fields[key]
        if type(number) is not int or number < minimum:
            raise CorruptLogError(offset, "invalid invocation counter")
    if fields["invocation_number"] != cast("int", fields["logical_attempt"]) + cast(
        "int", fields["recovery_count"]
    ):
        raise CorruptLogError(offset, "inconsistent invocation counters")


def validate_post_fields(
    key: object, action: object, target: object, payload: object, *, offset: int
) -> None:
    """Validate serialized semantic actions before constructing typed facades.

    Raises:
        CorruptLogError: If a posting key, target, action, or payload is invalid.

    """
    if not isinstance(key, str):
        raise CorruptLogError(offset, "posting key must be a string")
    try:
        key_size = len(key.encode("utf-8"))
    except UnicodeError as error:
        raise CorruptLogError(offset, "posting key must be valid UTF-8") from error
    if not 0 < key_size <= MAX_POST_KEY_BYTES:
        raise CorruptLogError(offset, "posting key is empty or oversized")
    if not isinstance(action, str) or action not in POST_ACTION_TARGETS:
        raise CorruptLogError(offset, "unknown posting action")
    if not isinstance(target, dict):
        raise CorruptLogError(offset, "posting target must be an object")
    fields = cast("dict[str, object]", target)
    if (
        set(fields) != {"kind", "id"}
        or fields.get("kind") != POST_ACTION_TARGETS[action]
        or not isinstance(fields.get("id"), str)
        or not fields["id"]
    ):
        raise CorruptLogError(offset, "posting action has an invalid target")
    if not is_json_value(payload):
        raise CorruptLogError(
            offset, "posted payload must contain only finite JSON values"
        )
    if action in {"stream.close", "execution.cancel"} and payload is not None:
        raise CorruptLogError(
            offset, "close and cancellation posts require a null payload"
        )
    if action == "signal.ack" and (not isinstance(payload, str) or not payload):
        raise CorruptLogError(
            offset, "signal acknowledgement requires a message identity"
        )
    if action == "execution.fail":
        validate_error(payload, offset)
        if cast("dict[str, object]", payload)["cancelled"]:
            raise CorruptLogError(
                offset, "cancellation requires its separate posting action"
            )
    try:
        _ = canonical_json(payload)
    except (UnicodeError, ValueError) as error:
        raise CorruptLogError(offset, "posted payload must be valid UTF-8") from error


def _validate_post_receipt(entry: dict[str, object], offset: int) -> None:
    if (
        entry["source"] != "effect"
        or not entry["id"]
        or cast("int", entry["ts"]) < 0
        or type(entry["format_version"]) is not int
        or entry["format_version"] != 1
    ):
        raise CorruptLogError(offset, "unsupported posting receipt format or source")
    scope_value = entry["scope"]
    if not isinstance(scope_value, dict):
        raise CorruptLogError(offset, "posting scope must be an object")
    scope = cast("dict[str, object]", scope_value)
    kind = scope.get("kind")
    if (
        set(scope) != {"format_version", "run_seed", "kind", "id"}
        or type(scope.get("format_version")) is not int
        or scope["format_version"] != 1
        or not isinstance(kind, str)
        or kind not in {"execution", "control"}
        or any(
            not isinstance(scope.get(key), str) or not scope[key]
            for key in ("run_seed", "id")
        )
    ):
        raise CorruptLogError(offset, "invalid posting scope")
    validate_post_fields(
        entry["key"], entry["action"], entry["target"], entry["payload"], offset=offset
    )
    if scope["kind"] == "control" and entry["action"] != "signal.send":
        raise CorruptLogError(offset, "control sender scope only permits signal sends")
    if entry["payload_digest"] != digest(
        "duron.post-payload.v1", cast("JSONValue", entry["payload"])
    ):
        raise CorruptLogError(offset, "posting receipt payload digest is inconsistent")
    ids = entry["event_ids"]
    if not isinstance(ids, list):
        raise CorruptLogError(offset, "posting receipt requires mutation identifiers")
    event_ids = cast("list[object]", ids)
    if not event_ids or any(not isinstance(id_, str) or not id_ for id_ in event_ids):
        raise CorruptLogError(
            offset, "posting receipt mutation identifiers are invalid"
        )
    if (
        len(set(cast("list[str]", event_ids))) != len(event_ids)
        or entry["id"] in event_ids
    ):
        raise CorruptLogError(
            offset, "posting receipt mutation identifiers must be unique"
        )


def validate_entry(value: object, offset: int) -> Entry:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "entry must be a JSON object")
    entry = cast("dict[str, object]", value)
    if not is_json_value(cast("object", entry)):
        raise CorruptLogError(offset, "entry must contain only finite JSON values")
    for field, expected_type in (("id", str), ("ts", int), ("source", str)):
        if not isinstance(entry.get(field), expected_type) or (
            field == "ts" and isinstance(entry.get(field), bool)
        ):
            raise CorruptLogError(offset, f"missing or invalid {field!r} field")
    if entry["source"] not in {"task", "effect"}:
        raise CorruptLogError(offset, f"invalid source {entry['source']!r}")
    if not is_entry(cast("BaseEntry", entry)):
        raise CorruptLogError(offset, f"unknown entry type {entry.get('type')!r}")
    type_name = cast("str", entry["type"])
    for field in _REQUIRED_FIELDS[type_name]:
        if field not in entry:
            raise CorruptLogError(offset, f"missing {field!r} for {type_name}")
    if type_name == "post.receipt":
        _validate_post_receipt(entry, offset)
    if type_name == "signal.scope.exit" and (
        entry["source"] != "task"
        or not isinstance(entry["signal_id"], str)
        or not entry["signal_id"]
    ):
        raise CorruptLogError(offset, "invalid signal scope exit")
    if type_name == "signal.ack":
        if (
            entry["source"] != "effect"
            or any(
                not isinstance(entry[name], str) or not entry[name]
                for name in ("signal_id", "message_id", "receiver_id")
            )
            or type(entry["message_offset"]) is not int
            or entry["message_offset"] < 0
        ):
            raise CorruptLogError(offset, "invalid signal acknowledgement identity")
        _validate_invocation(entry["invocation"], offset)
    if type_name in {"effect.attempt.start", "effect.retry.scheduled"}:
        if entry["source"] != "effect" or not isinstance(entry["promise_id"], str):
            raise CorruptLogError(
                offset, "effect record requires effect source and promise_id"
            )
        _validate_invocation(entry["invocation"], offset)
        if type_name == "effect.attempt.start":
            if not isinstance(entry["session_id"], str) or not entry["session_id"]:
                raise CorruptLogError(offset, "invalid effect session identity")
        else:
            validate_error(entry["error"], offset)
            if (
                entry["classification"] != "retryable"
                or type(entry["not_before_us"]) is not int
                or entry["not_before_us"] < cast("int", entry["ts"])
            ):
                raise CorruptLogError(
                    offset, "invalid retry classification or deadline"
                )
    if "effect_call" in entry:
        data = entry["effect_call"]
        if type_name != "promise.create" or not isinstance(data, dict):
            raise CorruptLogError(
                offset, "effect call descriptor requires promise.create"
            )
        fields = cast("dict[str, object]", data)
        if (
            set(fields)
            != {"format_version", "descriptor", "input_digest", "run_seed", "inputs"}
            or type(fields.get("format_version")) is not int
            or fields["format_version"] != 1
            or not isinstance(fields["descriptor"], dict)
            or any(
                not isinstance(fields[k], str) or not fields[k]
                for k in ("input_digest", "run_seed")
            )
            or not isinstance(fields["inputs"], dict)
            or digest("duron.inputs.v1", cast("JSONValue", fields["inputs"]))
            != fields["input_digest"]
        ):
            raise CorruptLogError(offset, "invalid effect call descriptor")
    if (
        type_name == "stream.create"
        and entry["name"] is not None
        and not isinstance(entry["name"], str)
    ):
        raise CorruptLogError(offset, "stream name must be a string or null")
    if type_name == "stream.create" and "owned" in entry:
        try:
            reference = HandleReference.from_fields(entry["owned"])
        except InvalidHandleError as error:
            raise CorruptLogError(offset, "invalid owned stream descriptor") from error
        if reference.channel_id != entry["id"] or reference.direction not in {
            "stream.sender",
            "signal.receiver",
        }:
            raise CorruptLogError(offset, "owned stream descriptor identity mismatch")
    if type_name in {"stream.emit", "stream.complete"} and not isinstance(
        entry["stream_id"], str
    ):
        raise CorruptLogError(offset, "stream_id must be a string")
    if type_name in {"promise.complete", "stream.complete"} and "error" in entry:
        validate_error(entry["error"], offset)
    if type_name == "promise.complete":
        if not isinstance(entry["promise_id"], str):
            raise CorruptLogError(offset, "promise_id must be a string")
        has_result = "result" in entry
        has_error = "error" in entry
        if has_result == has_error:
            raise CorruptLogError(
                offset, "promise.complete must contain exactly one result or error"
            )
    if "metadata" in entry and not isinstance(entry["metadata"], dict):
        raise CorruptLogError(offset, "metadata must be a JSON object")
    has_effect_identity = "effect_name" in entry or "effect_version" in entry
    if has_effect_identity and (
        not isinstance(entry.get("effect_name"), str)
        or not isinstance(entry.get("effect_version"), str)
    ):
        raise CorruptLogError(offset, "effect identity must contain string fields")
    return cast("Entry", entry)


def random_id() -> str:
    """Return a random 12-byte token id (log entries and host-side op ids).

    Returns:
        A fresh base64-encoded random token.

    """
    return binascii.b2a_base64(os.urandom(12), newline=False).decode()
