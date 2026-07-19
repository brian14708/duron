"""Durable semantic posts shared by local and external invocation reporters."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast
from typing_extensions import Protocol

from duron._execution.models import canonical_json
from duron._hints import UnspecifiedType
from duron._history.encoding import (
    MAX_POST_KEY_BYTES,
    post_semantics,
    validate_entry,
    validate_post_fields,
)
from duron.codec import DefaultCodec
from duron.errors import CorruptLogError, InvalidPostError, PostConflictError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Sequence

    from duron._execution.models import InvocationIdentity
    from duron._hints import JSONValue
    from duron._history.entries import (
        Entry,
        PostAction,
        PostData,
        PostScopeData,
        PostTargetData,
    )


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
            "format_version": 0,
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

    def __post_init__(self) -> None:
        if not isinstance(cast("object", self.capability), str) or not self.capability:
            msg = "posting authority requires an opaque capability"
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
    """Serialize semantic mutations with inline idempotency identities.

    Disposable receipt state is populated by the session's single replay pass.
    """

    def __init__(self, writer: PostingWriter, owner: PostingOwner) -> None:
        self._writer = writer
        self._owner = owner
        self._receipts: dict[tuple[str, str, str, str], tuple[int, SemanticPost]] = {}
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
                if recorded != post:
                    msg = (
                        "posting key was already committed for another action, "
                        "target, or payload"
                    )
                    raise PostConflictError(msg)
                return Receipt(offset, duplicate=True)
            self._owner.check_lifecycle(authority, post)
            mutations = tuple(self._owner.stage(authority, post))
            if not mutations:
                msg = "a semantic post must stage at least one mutation"
                raise InvalidPostError(msg)
            mutation = mutations[-1].copy()
            annotation: PostData = {
                "scope": authority.scope.fields(),
                "key": post.key,
                "action": post.action,
            }
            mutation["post"] = annotation
            _ = validate_entry(mutation, 0)
            target, payload = post_semantics(mutation, 0)
            if (
                target != post.target.fields()
                or canonical_json(payload) != post.encoded_payload
            ):
                msg = "staged mutation does not match its semantic post"
                raise InvalidPostError(msg)
            offsets = await self._writer.append((*mutations[:-1], mutation))
            return Receipt(offsets[-1], duplicate=False)

    def apply(self, offset: int, entry: Entry) -> None:
        """Rebuild the deduplication index directly from annotated mutations.

        Raises:
            CorruptLogError: If a posting key has multiple committed mutations.

        """
        if "post" not in entry:
            return
        post = entry["post"]
        fields = post["scope"]
        key = (fields["run_seed"], fields["kind"], fields["id"], post["key"])
        if key in self._receipts:
            msg = "posting key has more than one committed mutation"
            raise CorruptLogError(offset, msg)
        target, payload = post_semantics(entry, offset)
        self._receipts[key] = (
            offset,
            SemanticPost.create(
                post["key"],
                post["action"],
                PostTarget(target["kind"], target["id"]),
                payload,
            ),
        )
