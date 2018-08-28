import pytest

from collections import namedtuple
from typing import NamedTuple
from tinsel.lib import (
    struct,
    is_pyspark_class,
    is_container,
    check_pyspark_struct,
)


class Plain:
    pass


class Box(NamedTuple):
    pass


OldBox = namedtuple("OldBox", ["a"])


def test_struct_applicability():
    with pytest.raises(ValueError):
        struct(Plain)
    with pytest.raises(ValueError):
        struct(OldBox)
    assert is_pyspark_class(struct(Box))


def test_namedtuple_heuristics():
    assert is_container(Box)
    assert not is_container(OldBox)
    assert not is_container(tuple)


def test_pyspark_struct():
    AugmentedBox = struct(Box)
    with pytest.raises(TypeError):
        check_pyspark_struct(AugmentedBox())
    with pytest.raises(ValueError):
        check_pyspark_struct(Box)
    with pytest.raises(ValueError):
        check_pyspark_struct(Plain)
