# pyright: reportPrivateUsage=false

from __future__ import annotations

import binascii
import json
import os
from typing import TYPE_CHECKING, TypeGuard, cast

if TYPE_CHECKING:
    from duron._hints import JSONValue
    from duron._history.entries import BaseEntry, Entry, PostTargetData

from duron._execution.models import HandleReference, canonical_json, digest
from duron._history.entries import POST_ACTION_TARGETS, CorruptLogError
from duron.codec import _is_json_value
from duron.errors import InvalidHandleError
from duron.storage._protocol import Event

MAX_POST_KEY_BYTES = 1024


def encode_entry(entry: Entry) -> Event:
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
    return Event(validated["id"], payload, _entry_stream_id(validated))


def decode_record(record: Event, offset: int) -> Entry:
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
    if entry["type"] == "input.accept":
        return "__input__:" + entry["port"]
    if entry["type"] == "stream.create":
        return entry["id"]
    if (
        entry["type"] == "stream.emit"
        or entry["type"] == "stream.complete"
        or entry["type"] == "stream.scope.exit"
    ):
        return entry["stream_id"]
    return None


_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "run.create": ("result",),
    "input.accept": ("port", "value", "closed", "retry_id"),
    "run.finish": ("cancelled",),
    "promise.create": (),
    "promise.complete": ("promise_id",),
    "stream.create": ("name",),
    "stream.emit": ("stream_id", "value"),
    "stream.complete": ("stream_id",),
    "stream.scope.exit": ("stream_id",),
    "barrier": (),
    "effect.attempt.start": ("promise_id", "invocation", "session_id"),
}


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


def _validate_invocation(value: object, offset: int) -> None:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "effect invocation must be an object")
    fields = cast("dict[str, object]", value)
    if (
        set(fields) != {"format_version", "invocation_number", "recovery_count"}
        or type(fields.get("format_version")) is not int
        or fields["format_version"] != 0
    ):
        raise CorruptLogError(offset, "invalid invocation identity version or fields")
    for key, minimum in (("invocation_number", 1), ("recovery_count", 0)):
        number = fields[key]
        if type(number) is not int or number < minimum:
            raise CorruptLogError(offset, "invalid invocation counter")
    if fields["invocation_number"] != 1 + cast("int", fields["recovery_count"]):
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
    if not _is_json_value(payload):
        raise CorruptLogError(
            offset, "posted payload must contain only finite JSON values"
        )
    if action in {"stream.close", "execution.cancel"} and payload is not None:
        raise CorruptLogError(
            offset, "close and cancellation posts require a null payload"
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


def post_semantics(entry: Entry, offset: int) -> tuple[PostTargetData, JSONValue]:
    """Derive the semantic target and payload of an annotated mutation.

    Returns:
        The target identity and semantic JSON payload.

    Raises:
        CorruptLogError: If the action does not describe the mutation.

    """
    post = entry.get("post")
    if post is None:
        raise CorruptLogError(offset, "mutation has no post annotation")
    action = post["action"]
    if action in {"stream.send", "signal.send"} and entry["type"] == "stream.emit":
        return (
            {
                "kind": "signal" if action == "signal.send" else "stream",
                "id": entry["stream_id"],
            },
            entry["value"],
        )
    if (
        action == "stream.close"
        and entry["type"] == "stream.complete"
        and "error" not in entry
    ):
        return {"kind": "stream", "id": entry["stream_id"]}, None
    if entry["type"] == "promise.complete":
        target: PostTargetData = {"kind": "execution", "id": entry["promise_id"]}
        if action == "execution.complete" and "result" in entry:
            return target, entry["result"]
        if "error" in entry:
            if action == "execution.fail" and not entry["error"]["cancelled"]:
                return target, cast("JSONValue", entry["error"])
            if action == "execution.cancel" and entry["error"]["cancelled"]:
                return target, None
    raise CorruptLogError(offset, "posting action does not describe its mutation")


def _validate_post(entry: dict[str, object], offset: int) -> None:
    if entry["source"] != "effect" or not entry["id"] or cast("int", entry["ts"]) < 0:
        raise CorruptLogError(offset, "invalid posted mutation source or identity")
    value = entry["post"]
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "invalid post annotation")
    post = cast("dict[str, object]", value)
    if set(post) != {"scope", "key", "action"}:
        raise CorruptLogError(offset, "invalid post annotation")
    if not isinstance(post["action"], str) or post["action"] not in POST_ACTION_TARGETS:
        raise CorruptLogError(offset, "unknown posting action")
    scope_value = post["scope"]
    if not isinstance(scope_value, dict):
        raise CorruptLogError(offset, "posting scope must be an object")
    scope = cast("dict[str, object]", scope_value)
    kind = scope.get("kind")
    if (
        set(scope) != {"format_version", "run_seed", "kind", "id"}
        or type(scope.get("format_version")) is not int
        or scope["format_version"] != 0
        or not isinstance(kind, str)
        or kind not in {"execution", "control"}
        or any(
            not isinstance(scope.get(key), str) or not scope[key]
            for key in ("run_seed", "id")
        )
    ):
        raise CorruptLogError(offset, "invalid posting scope")
    target, payload = post_semantics(cast("Entry", entry), offset)
    validate_post_fields(post["key"], post["action"], target, payload, offset=offset)
    if scope["kind"] == "control" and (
        post["action"] != "signal.send" or scope["id"] != target["id"]
    ):
        raise CorruptLogError(offset, "control sender scope only permits signal sends")
    if target["kind"] == "execution" and (
        scope["kind"] != "execution" or scope["id"] != target["id"]
    ):
        raise CorruptLogError(offset, "execution post scope does not match its target")


def validate_entry(value: object, offset: int) -> Entry:
    if not isinstance(value, dict):
        raise CorruptLogError(offset, "entry must be a JSON object")
    entry = cast("dict[str, object]", value)
    if not _is_json_value(cast("object", entry)):
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
    if type_name == "input.accept" and (
        entry["source"] != "effect"
        or not isinstance(entry["port"], str)
        or not entry["port"]
        or type(entry["closed"]) is not bool
        or (entry["closed"] and entry["value"] is not None)
        or (
            entry["retry_id"] is not None
            and (not isinstance(entry["retry_id"], str) or not entry["retry_id"])
        )
    ):
        raise CorruptLogError(offset, "invalid input acceptance")
    if type_name == "run.finish":
        if entry["source"] != "effect" or type(entry["cancelled"]) is not bool:
            raise CorruptLogError(offset, "invalid terminal outcome")
        if entry["cancelled"]:
            if "result" in entry or "error" in entry:
                raise CorruptLogError(offset, "cancellation carries a result or error")
        elif ("result" in entry) == ("error" in entry):
            raise CorruptLogError(offset, "outcome requires one result or error")
        if "error" in entry:
            validate_error(entry["error"], offset)
    if type_name == "stream.scope.exit" and (
        entry["source"] != "task"
        or not isinstance(entry["stream_id"], str)
        or not entry["stream_id"]
        or "error" in entry
    ):
        raise CorruptLogError(offset, "invalid stream scope exit")
    if type_name == "effect.attempt.start":
        if entry["source"] != "effect" or not isinstance(entry["promise_id"], str):
            raise CorruptLogError(
                offset, "effect record requires effect source and promise_id"
            )
        _validate_invocation(entry["invocation"], offset)
        if not isinstance(entry["session_id"], str) or not entry["session_id"]:
            raise CorruptLogError(offset, "invalid effect session identity")
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
            or fields["format_version"] != 0
            or not isinstance(fields["descriptor"], dict)
            or any(
                not isinstance(fields[k], str) or not fields[k]
                for k in ("input_digest", "run_seed")
            )
            or not isinstance(fields["inputs"], dict)
            or digest("duron.inputs.v0", cast("JSONValue", fields["inputs"]))
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
    if "post" in entry:
        _validate_post(entry, offset)
    return cast("Entry", entry)


def random_id() -> str:
    """Return a random 12-byte token id (log entries and host-side op ids).

    Returns:
        A fresh base64-encoded random token.

    """
    return binascii.b2a_base64(os.urandom(12), newline=False).decode()
