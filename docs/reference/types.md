# `duron.types` module

`JSONValue` describes the recursive values a codec can persist: JSON scalars,
lists, and dictionaries with string keys. Floats must also be finite; this is
a runtime requirement that the type alias cannot express.

`TypeHint[T]` represents a runtime annotation for values of type `T`, such as
`int` or `list[str]`. It appears in custom codecs and scoped channel APIs; it
does not validate a value by itself. See the [codec reference](codec.md).

## ::: duron.types
