tinsel
======

*Your data IS your schema*

.. image:: https://img.shields.io/pypi/pyversions/tinsel.svg
    :target: https://pypi.python.org/pypi/tinsel
.. image:: https://img.shields.io/pypi/v/tinsel.svg
    :target: https://pypi.python.org/pypi/tinsel
.. image:: https://coveralls.io/repos/github/Orhideous/tinsel/badge.svg?branch=master
    :target: https://coveralls.io/github/Orhideous/tinsel?branch=master
.. image:: https://img.shields.io/travis/Orhideous/tinsel.svg
    :target: https://travis-ci.org/Orhideous/tinsel
.. image:: https://pyup.io/repos/github/Orhideous/tinsel/shield.svg
    :target: https://pyup.io/repos/github/Orhideous/tinsel/

This tiny library helps to overcome excessive complexity in hand-written pyspark
dataframe schemas.

How?
----

Shape your data as ``NamedTuple`` or dataclasses - they can freely mix::

    from dataclasses import dataclass
    from tinsel import struct, transform
    from typing import NamedTuple, Optional, Dict, List

    @struct
    @dataclass
    class UserInfo:
        hobby: List[str]
        last_seen: Optional[int]
        pet_ages: Dict[str, int]


    @struct
    class User(NamedTuple):
        login: str
        age: int
        active: bool
        info: Optional[UserInfo]


Transform root node (``User`` in our case) into schema::

    schema = transform(User)


Create some data, if necessary::

    data = [
        User(
            login="Ben",
            age=18,
            active=False,
            info=None
        ),
        User(
            login="Tom",
            age=32,
            active=True,
            info=UserInfo(
                hobby=["pets", "flowers"],
                last_seen=16,
                pet_ages={"Jack": 2, "Sunshine": 6}
            )
        )
    ]

And… voilà!::

    from pyspark.sql import SparkSession

    sc = SparkSession.builder.master('local').getOrCreate()

    df = sc.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

This will output::

    root
     |-- login: string (nullable = false)
     |-- age: integer (nullable = false)
     |-- active: boolean (nullable = false)
     |-- info: struct (nullable = true)
     |    |-- hobby: array (nullable = false)
     |    |    |-- element: string (containsNull = false)
     |    |-- last_seen: integer (nullable = true)
     |    |-- pet_ages: map (nullable = false)
     |    |    |-- key: string
     |    |    |-- value: integer (valueContainsNull = false)


    +-----+---+------+----------------------------------------------+
    |login|age|active|info                                          |
    +-----+---+------+----------------------------------------------+
    |Ben  |18 |false |null                                          |
    |Tom  |32 |true  |[[pets, flowers],, [Jack -> 2, Sunshine -> 6]]|
    +-----+---+------+----------------------------------------------+

Features
--------
* use native python types; no extra DSL, no cryptic API — just plain Python;
* small and fast;
* provide type shims for some types absent in Python, like ``long`` or ``short``;
* nullable fields naturally fits into schema definition;

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
