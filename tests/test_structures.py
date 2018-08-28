import sys
import pytest
from tinsel.lib import struct, transform
from pyspark.sql import types as t
from typing import NamedTuple, Optional, Dict, List

skip_dc = pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason="Requires python3.7 or higher to test dataclasses"
)


@struct
class Plain(NamedTuple):
    a: str
    b: int
    c: bool


@struct
class Box(NamedTuple):
    d: float
    e: Plain
    f: None


@struct
class Complex(NamedTuple):
    g: Optional[Plain]
    h: List[Dict[str, Optional[Box]]]
    i: Dict[Box, Optional[List[Dict[int, Plain]]]]


COMPLEX_STRUCT = t.StructType([
    t.StructField(
        "g",
        t.StructType([
            t.StructField("a", t.StringType(), False),
            t.StructField("b", t.IntegerType(), False),
            t.StructField("c", t.BooleanType(), False)
        ]),
        True
    ),
    t.StructField(
        "h",
        t.ArrayType(
            t.MapType(
                t.StringType(),
                t.StructType([
                    t.StructField("d", t.DoubleType(), False),
                    t.StructField("e", t.StructType([
                        t.StructField("a", t.StringType(), False),
                        t.StructField("b", t.IntegerType(), False),
                        t.StructField("c", t.BooleanType(), False)
                    ]), False),
                    t.StructField("f", t.NullType(), False)
                ]), True),
            False
        ),
        False
    ),
    t.StructField(
        "i",
        t.MapType(
            t.StructType([
                t.StructField("d", t.DoubleType(), False),
                t.StructField("e", t.StructType([
                    t.StructField("a", t.StringType(), False),
                    t.StructField("b", t.IntegerType(), False),
                    t.StructField("c", t.BooleanType(), False)
                ]), False),
                t.StructField("f", t.NullType(), False)
            ]),
            t.ArrayType(
                t.MapType(
                    t.IntegerType(),
                    t.StructType([
                        t.StructField("a", t.StringType(), False),
                        t.StructField("b", t.IntegerType(), False),
                        t.StructField("c", t.BooleanType(), False)
                    ]),
                    False
                ),
                False
            ),
            True
        ),
        False
    )
])


def test_preserve_fields_order():
    assert tuple(transform(Plain).names) == Plain._fields


def test_parse_complex_type():
    assert transform(Complex) == COMPLEX_STRUCT


@skip_dc
def test_preserve_fields_order_dc():
    from dataclasses import dataclass

    @struct
    @dataclass
    class PlainDC:
        a: str
        b: int
        c: bool

    assert tuple(transform(PlainDC).names) == tuple(PlainDC.__dataclass_fields__.keys())


@skip_dc
def test_parse_complex_type_dc():
    from dataclasses import dataclass

    @struct
    @dataclass
    class PlainDC:
        a: str
        b: int
        c: bool

    @struct
    @dataclass
    class BoxDC:
        d: float
        e: PlainDC
        f: None

    @struct
    @dataclass
    class ComplexDC:
        g: Optional[PlainDC]
        h: List[Dict[str, Optional[BoxDC]]]
        i: Dict[Box, Optional[List[Dict[int, PlainDC]]]]

    assert transform(ComplexDC) == COMPLEX_STRUCT
    assert transform(Complex) == transform(ComplexDC)
