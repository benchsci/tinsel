from typing import TypeVar, Generic, Dict

NoneType = type(None)
byte = TypeVar("byte")
short = TypeVar("short")
long = TypeVar("long")
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

__type_cache: Dict[str, type] = {}


class BoundDecimal:
    __slots__ = '__constraints__',


class FunctorLike(Generic[T_co]):
    __slots__ = ()


def decimal(prec: int, rounding: int) -> type:
    name = f'BoundDecimal_{prec}_{rounding}'
    if name in __type_cache:
        return __type_cache[name]
    else:
        cls = type(name, (BoundDecimal, ), {})
        cls.__constraints__ = (prec, rounding)
        __type_cache[name] = cls
        return cls
