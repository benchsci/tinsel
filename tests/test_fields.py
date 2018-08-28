from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Optional, Union, Callable, NamedTuple

import pytest
from pyspark.sql import types as t

from tinsel.lib import infer_spark_type, transform_field, maybe_unlift_optional, struct
from tinsel.types import NoneType, long, short, byte

DEFAULT_NAME = "some_field"


@struct
class Dummy(NamedTuple):
    pass


PRIMITIVES = [
    (NoneType, t.NullType()),
    (int, t.IntegerType()),
    (float, t.DoubleType()),
    (str, t.StringType()),
    (bytes, t.BinaryType()),
    (bytearray, t.BinaryType()),
    (bool, t.BooleanType()),
]

SYNTHETIC_PRIMITIVES = [
    (long, t.LongType()),
    (short, t.ShortType()),
    (byte, t.ByteType()),
]

DATE_TYPES = [
    (date, t.DateType()),
    (datetime, t.TimestampType()),
]

OPTIONALS_PY = [
    (str, (False, str)),
    (List[str], (False, List[str])),
    (Optional[str], (True, str)),
    (Optional[List[str]], (True, List[str])),
]

NULLABLE_TYPES = [
    (
        Optional[str],
        lambda name: t.StructField(name, t.StringType(), True)
    ),
    (
        Optional[List[str]],
        lambda name: t.StructField(name, t.ArrayType(t.StringType(), False), True)
    ),
    (
        List[Optional[str]],
        lambda name: t.StructField(name, t.ArrayType(t.StringType(), True), False)
    ),
    (
        Optional[List[Optional[str]]],
        lambda name: t.StructField(name, t.ArrayType(t.StringType(), True), True)
    ),
    (
        Optional[Dict[str, int]],
        lambda name: t.StructField(name, t.MapType(t.StringType(), t.IntegerType(), False), True)
    ),
    (
        Dict[str, Optional[int]],
        lambda name: t.StructField(name, t.MapType(t.StringType(), t.IntegerType(), True), False)
    ),
    (
        Optional[Dict[str, Optional[int]]],
        lambda name: t.StructField(name, t.MapType(t.StringType(), t.IntegerType(), True), True)
    ),
]


@pytest.mark.skip
def check(py_type, spark_type): assert infer_spark_type(py_type) == spark_type


@pytest.mark.parametrize(["py_type", "spark_type"], PRIMITIVES)
def test_primitive_types(py_type, spark_type): check(py_type, spark_type)


@pytest.mark.parametrize(["py_type", "spark_type"], SYNTHETIC_PRIMITIVES)
def test_synthetic_types(py_type, spark_type): check(py_type, spark_type)


@pytest.mark.parametrize(["py_type", "spark_type"], DATE_TYPES)
def test_date_types(py_type, spark_type): check(py_type, spark_type)


def test_decimal(): check(Decimal, t.DecimalType(precision=36, scale=6))


def test_list(): check(List[str], t.ArrayType(t.StringType(), False))


def test_dict(): check(Dict[int, str], t.MapType(t.IntegerType(), t.StringType(), False))


def test_struct(): check(Dummy, t.StructType([]))


def test_invalid_dict_keys():
    with pytest.raises(TypeError):
        infer_spark_type(Dict[Optional[int], str])


def test_invalid_type():
    with pytest.raises(TypeError):
        infer_spark_type(object)


def test_invalid_union():
    with pytest.raises(TypeError):
        infer_spark_type(Union[int, str])


def test_invalid_complex_type():
    with pytest.raises(TypeError):
        infer_spark_type(Callable[[int], str])


def test_field_transformer():
    assert transform_field(DEFAULT_NAME, str) == t.StructField(DEFAULT_NAME, t.StringType(), False)


@pytest.mark.parametrize(["raw", "expected"], OPTIONALS_PY)
def test_optionals_unlifting(raw, expected):
    assert maybe_unlift_optional(raw) == expected


@pytest.mark.parametrize(["raw", "field_factory"], NULLABLE_TYPES)
def test_nullable_fields(raw: type, field_factory: Callable[[str], t.StructField]):
    # noinspection PyTypeChecker
    assert transform_field(DEFAULT_NAME, raw) == field_factory(DEFAULT_NAME)
