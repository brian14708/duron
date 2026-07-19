# Codec types

Import `JSONValue` and `TypeHint` from `duron.codec`.

`JSONValue` describes the recursive values a codec can persist: JSON scalars,
lists, and dictionaries with string keys. Floats must also be finite; this is
a runtime requirement that the type alias cannot express.

`TypeHint[T]` represents a runtime annotation for values of type `T`, such as
`int` or `list[str]`. It appears in custom codecs and scoped channel APIs; it
does not validate a value by itself. See the [codec reference](codec.md).

Port payloads support `Literal`, `Annotated`, and `TypeAliasType` aliases,
including generic and recursive aliases. Native aliases declared with the
`type` statement are supported on Python 3.12 and later. Aliases do not add
serialization support: tuples and other non-JSON Python values still need a
custom codec.

## ::: duron.codec.JSONValue

## ::: duron.codec.TypeHint
