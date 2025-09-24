from __future__ import annotations

import base64
import importlib
import pickle
from typing import TYPE_CHECKING, cast, final

from typing_extensions import Protocol, override

BaseModel: type[_pydantic.BaseModel] | None = None
try:
    import pydantic as _pydantic

    BaseModel = _pydantic.BaseModel
except ImportError:
    pass

if TYPE_CHECKING:
    from duron.log.entry import JSONValue

__all__ = ["Codec", "DEFAULT_CODEC"]


class Codec(Protocol):
    def encode_json(self, result: object) -> JSONValue: ...
    def decode_json(self, encoded: JSONValue) -> object: ...

    def encode_state(self, obj: object) -> str: ...
    def decode_state(self, state: str) -> object: ...


@final
class _DefaultCodec(Codec):
    def __init__(self) -> None:
        self._type_cache: dict[str, type] = {}

    def _lookup_model(self, qual: str) -> type:
        cls = self._type_cache.get(qual)
        if cls is not None:
            return cls

        mod, name = qual.split(":", 1)
        obj: object = importlib.import_module(mod)
        for part in name.split("."):
            obj = getattr(obj, part)
        if isinstance(obj, type):
            self._type_cache[qual] = obj
            return obj
        else:
            raise TypeError(f"Imported object is not a type: {obj!r}")

    @override
    def encode_json(self, result: object) -> JSONValue:
        if BaseModel and isinstance(result, BaseModel):
            cls = result.__class__
            model = result.model_dump(mode="json")
            model["_duron.pydantic"] = f"{cls.__module__}:{cls.__qualname__}"
            return model
        return cast("JSONValue", result)

    @override
    def decode_json(self, encoded: JSONValue) -> object:
        if isinstance(encoded, dict) and "_duron.pydantic" in encoded:
            model = self._lookup_model(cast("str", encoded["_duron.pydantic"]))
            if not BaseModel or not issubclass(model, BaseModel):
                raise TypeError(f"Decoded class is not a BaseModel subclass: {model}")
            return model.model_validate(
                {k: v for k, v in encoded.items() if not k.startswith("_duron.")}
            )
        return encoded

    @override
    def encode_state(self, obj: object) -> str:
        return base64.b64encode(pickle.dumps(obj, protocol=5)).decode("ascii")

    @override
    def decode_state(self, state: str) -> object:
        return pickle.loads(base64.b64decode(state.encode()))


DEFAULT_CODEC = _DefaultCodec()
