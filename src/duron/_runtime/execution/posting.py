"""Durable semantic posts shared by local and external invocation reporters."""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, cast
from typing_extensions import Protocol

from duron._hints import UnspecifiedType
from duron._runtime.execution.models import canonical_json, digest
from duron._runtime.storage.backend import validate_run_id
from duron._runtime.storage.helper import (
    MAX_POST_KEY_BYTES,
    random_id,
    validate_entry,
    validate_post_fields,
)
from duron.codec import DefaultCodec
from duron.errors import (
    CorruptLogError,
    ExecutionCompletedError,
    InvalidPostError,
    PostConflictError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Sequence

    from duron._runtime.execution.models import InvocationIdentity
    from duron._runtime.storage.entry import (
        Entry,
        PostAction,
        PostReceiptEntry,
        PostScopeData,
        PostTargetData,
    )
    from duron.types import JSONValue


def validate_post_key(key: str) -> None:
    """Validate a bounded UTF-8 key before any authority lookup or mutation.

    Raises:
        InvalidPostError: If the key is empty, oversized, or invalid UTF-8.

    """
    if not isinstance(cast("object", key), str):
        msg = "a posting key must be a nonempty UTF-8 string"
        raise InvalidPostError(msg)
    try:
        size = len(key.encode("utf-8"))
    except UnicodeError as error:
        msg = "a posting key must be valid UTF-8"
        raise InvalidPostError(msg) from error
    if not 0 < size <= MAX_POST_KEY_BYTES:
        msg = f"a posting key must contain 1 through {MAX_POST_KEY_BYTES} UTF-8 bytes"
        raise InvalidPostError(msg)


@dataclass(frozen=True, slots=True)
class PostScope:
    """Stable deduplication scope, independent of a physical invocation."""

    run_seed: str
    kind: Literal["execution", "control"]
    id: str

    def __post_init__(self) -> None:
        if self.kind not in {"execution", "control"} or any(
            not isinstance(value, str) or not value
            for value in cast("tuple[object, object]", (self.run_seed, self.id))
        ):
            msg = "a posting scope requires a run seed, scope kind, and identity"
            raise InvalidPostError(msg)

    def fields(self) -> PostScopeData:
        return {
            "format_version": 1,
            "run_seed": self.run_seed,
            "kind": self.kind,
            "id": self.id,
        }


@dataclass(frozen=True, slots=True)
class PostTarget:
    kind: Literal["execution", "stream", "signal"]
    id: str

    def __post_init__(self) -> None:
        if (
            self.kind not in {"execution", "stream", "signal"}
            or not isinstance(cast("object", self.id), str)
            or not self.id
        ):
            msg = "a posting target requires a valid kind and identity"
            raise InvalidPostError(msg)

    def fields(self) -> PostTargetData:
        return {"kind": self.kind, "id": self.id}


@dataclass(frozen=True, slots=True)
class PostAuthority:
    """Opaque owner-issued authority; never contains a backend writer lease."""

    scope: PostScope
    capability: str
    invocation: InvocationIdentity | None
    receipt_only: bool = False

    def __post_init__(self) -> None:
        if (
            not isinstance(cast("object", self.capability), str)
            or not self.capability
            or type(self.receipt_only) is not bool
        ):
            msg = "posting authority requires an opaque capability and receipt mode"
            raise InvalidPostError(msg)
        if (self.scope.kind == "execution") != (self.invocation is not None):
            msg = "only execution posting authority carries invocation counters"
            raise InvalidPostError(msg)


@dataclass(frozen=True, slots=True)
class SemanticPost:
    """Detached canonical payload for one semantic mutation."""

    key: str
    action: PostAction
    target: PostTarget
    encoded_payload: bytes
    _payload_digest: str = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            payload = DefaultCodec.encode_json(
                json.loads(self.encoded_payload), UnspecifiedType
            )
            validate_post_fields(
                self.key, self.action, self.target.fields(), payload, offset=0
            )
            if canonical_json(payload) != self.encoded_payload:
                msg = "posting payload must use the canonical JSON encoding"
                raise InvalidPostError(msg)
        except (TypeError, ValueError) as error:
            msg = "posting action, target, or canonical payload is invalid"
            raise InvalidPostError(msg) from error
        object.__setattr__(
            self, "_payload_digest", digest("duron.post-payload.v1", payload)
        )

    @classmethod
    def create(
        cls, key: str, action: PostAction, target: PostTarget, payload: JSONValue
    ) -> SemanticPost:
        validate_post_key(key)
        try:
            encoded = canonical_json(DefaultCodec.encode_json(payload, UnspecifiedType))
        except (TypeError, ValueError) as error:
            msg = "a posted payload must be finite UTF-8 JSON"
            raise InvalidPostError(msg) from error
        return cls(key, action, target, encoded)

    @property
    def payload(self) -> JSONValue:
        return cast("JSONValue", json.loads(self.encoded_payload))

    @property
    def payload_digest(self) -> str:
        return self._payload_digest


@dataclass(frozen=True, slots=True)
class Receipt:
    """Committed receipt offset, retained durably with its semantic event."""

    offset: int
    duplicate: bool


class PostingOwner(Protocol):
    """Session-owned validation and staging; none of these calls publish state."""

    async def authorize(
        self, authority: PostAuthority, post: SemanticPost, /
    ) -> None: ...
    def check_lifecycle(
        self, authority: PostAuthority, post: SemanticPost, /
    ) -> None: ...
    def stage(
        self, authority: PostAuthority, post: SemanticPost, /
    ) -> Sequence[Entry]: ...


@dataclass(slots=True)
class _Gate:
    lock: asyncio.Lock
    users: int = 0


class PostingWriter(Protocol):
    """Atomic append operations required by the posting journal."""

    def check_open(self) -> None: ...
    async def append(self, entries: Sequence[Entry]) -> tuple[int, ...]: ...


class PostJournal:
    """Serialize semantic mutations and append their receipts atomically.

    Disposable receipt state is populated by the session's single replay pass.
    """

    def __init__(
        self,
        writer: PostingWriter,
        owner: PostingOwner,
        *,
        run_id: str,
        page_size: int = 256,
        now_us: Callable[[], int] | None = None,
    ) -> None:
        validate_run_id(run_id)
        if type(page_size) is not int or page_size <= 0:
            msg = "posting receipt page_size must be a positive integer"
            raise ValueError(msg)
        self._writer = writer
        self._owner = owner
        self._receipts: dict[
            tuple[str, str, str, str], tuple[int, PostReceiptEntry]
        ] = {}
        self._mutations: list[Entry] = []
        self._now_us = now_us if now_us is not None else lambda: time.time_ns() // 1_000
        self._gates: dict[PostScope, _Gate] = {}

    @asynccontextmanager
    async def gate(self, scope: PostScope) -> AsyncGenerator[None]:
        """Share this gate with invocation fencing and channel EOF transitions."""
        gate = self._gates.get(scope)
        if gate is None:
            gate = _Gate(asyncio.Lock())
            self._gates[scope] = gate
        gate.users += 1
        try:
            async with gate.lock:
                yield
        finally:
            gate.users -= 1
            if not gate.users:
                del self._gates[scope]

    async def post(self, authority: PostAuthority, post: SemanticPost) -> Receipt:
        """Commit one event or recover its original durable receipt.

        Returns:
            A receipt whose offset is the atomic group's final record.

        Raises:
            PostConflictError: If the key belongs to another semantic event.
            ExecutionCompletedError: If receipt-only authority tries a new mutation.
            InvalidPostError: If the owner stages no mutation.

        """
        validate_post_key(post.key)
        if authority.scope.kind == "control" and post.action != "signal.send":
            msg = "control sender authority only permits signal sends"
            raise InvalidPostError(msg)
        async with self.gate(authority.scope):
            self._writer.check_open()
            await self._owner.authorize(authority, post)
            existing = self._receipts.get((
                authority.scope.run_seed,
                authority.scope.kind,
                authority.scope.id,
                post.key,
            ))
            self._writer.check_open()
            if existing is not None:
                offset, recorded = existing
                if (
                    recorded["action"] != post.action
                    or recorded["target"] != post.target.fields()
                    or recorded["payload_digest"] != post.payload_digest
                    or canonical_json(recorded["payload"]) != post.encoded_payload
                ):
                    msg = (
                        "posting key was already committed for another action, "
                        "target, or payload"
                    )
                    raise PostConflictError(msg)
                return Receipt(offset, duplicate=True)
            if authority.receipt_only:
                msg = "receipt-only authority cannot create a new mutation"
                raise ExecutionCompletedError(msg)
            self._owner.check_lifecycle(authority, post)
            mutations = tuple(self._owner.stage(authority, post))
            if not mutations:
                msg = "a semantic post must stage at least one mutation"
                raise InvalidPostError(msg)
            receipt: PostReceiptEntry = {
                "id": random_id(),
                "ts": self._now_us(),
                "source": "effect",
                "type": "post.receipt",
                "format_version": 1,
                "scope": authority.scope.fields(),
                "key": post.key,
                "action": post.action,
                "target": post.target.fields(),
                "payload": post.payload,
                "payload_digest": post.payload_digest,
                "event_ids": [entry["id"] for entry in mutations],
            }
            _ = validate_entry(receipt, 0)
            offsets = await self._writer.append((*mutations, receipt))
            return Receipt(offsets[-1], duplicate=False)

    def _validate_mutations(
        self, receipt_offset: int, receipt: PostReceiptEntry
    ) -> None:
        expected = receipt["event_ids"]
        preceding = self._mutations[-len(expected) :] if expected else []
        if [entry["id"] for entry in preceding] != expected:
            msg = "posting receipt does not follow its recorded mutation group"
            raise CorruptLogError(receipt_offset, msg)
        mutation = preceding[-1]
        action = receipt["action"]
        target = receipt["target"]["id"]
        payload = receipt["payload"]
        valid = False
        if action in {"stream.send", "signal.send"}:
            valid = (
                mutation["type"] == "stream.emit"
                and mutation["stream_id"] == target
                and canonical_json(mutation["value"]) == canonical_json(payload)
            )
        elif action == "stream.close":
            valid = (
                mutation["type"] == "stream.complete"
                and mutation["stream_id"] == target
            )
        elif action == "signal.ack":
            valid = (
                mutation["type"] == "signal.ack"
                and mutation["signal_id"] == target
                and mutation["message_id"] == payload
                and mutation["receiver_id"] == receipt["scope"]["id"]
            )
        elif action == "execution.complete":
            valid = (
                mutation["type"] == "promise.complete"
                and mutation["promise_id"] == target
                and "result" in mutation
                and canonical_json(mutation["result"]) == canonical_json(payload)
            )
        elif action == "execution.fail":
            valid = (
                (
                    mutation["type"] == "promise.complete"
                    or mutation["type"] == "effect.retry.scheduled"
                )
                and mutation["promise_id"] == target
                and "error" in mutation
                and canonical_json(cast("JSONValue", mutation["error"]))
                == canonical_json(payload)
            )
        elif action == "execution.cancel":
            valid = (
                mutation["type"] == "promise.complete"
                and mutation["promise_id"] == target
                and "error" in mutation
                and mutation["error"]["cancelled"]
            )
        if not valid:
            msg = "posting receipt does not describe its committed mutation"
            raise CorruptLogError(receipt_offset, msg)

    def apply(self, offset: int, entry: Entry) -> None:
        """Index committed entries and validate receipt mutation associations.

        Raises:
            CorruptLogError: If the receipt has no unique matching mutation group.

        """
        if entry["type"] != "post.receipt":
            self._mutations.append(entry)
            return
        fields = entry["scope"]
        key = (fields["run_seed"], fields["kind"], fields["id"], entry["key"])
        if key in self._receipts:
            msg = "posting key has more than one committed receipt"
            raise CorruptLogError(offset, msg)
        self._validate_mutations(offset, entry)
        self._receipts[key] = (offset, entry)
        self._mutations.clear()
