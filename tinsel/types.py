from typing import TypeVar, Generic

NoneType = type(None)
byte = TypeVar("byte")
short = TypeVar("short")
long = TypeVar("long")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class FunctorLike(Generic[T_co]):
    __slots__ = ()
