import pytest
from pyspark.sql import functions as F

from brasileirao.utils.temporada import (
    derivar_temporada,
    partidas_esperadas,
    rebaixados_por_temporada,
)


@pytest.mark.parametrize(
    ("data", "temporada"),
    [
        ("27/04/2019", 2019),
        ("29/03/2003", 2003),
        ("05/12/2020", 2020),
        ("10/01/2021", 2020),
        ("25/02/2021", 2020),
        ("31/03/2021", 2020),
        ("10/04/2021", 2021),
        ("05/12/2021", 2021),
        ("15/01/2022", 2022),
    ],
)
def test_derivar_temporada(spark, data, temporada):
    df = spark.createDataFrame([(data,)], "data string")

    resultado = df.select(derivar_temporada(F.col("data")).alias("temporada")).first()

    assert resultado["temporada"] == temporada


def test_temporada_e_int(spark):
    df = spark.createDataFrame([("10/01/2021",)], "data string")

    assert dict(df.select(derivar_temporada(F.col("data")).alias("t")).dtypes) == {"t": "int"}


@pytest.mark.parametrize(
    ("temporada", "esperado"),
    [(2003, 552), (2004, 552), (2005, 462), (2006, 380), (2016, 379), (2020, 380), (2025, 380)],
)
def test_partidas_esperadas(temporada, esperado):
    assert partidas_esperadas(temporada) == esperado


@pytest.mark.parametrize(("temporada", "esperado"), [(2003, 2), (2004, 4), (2025, 4)])
def test_rebaixados_por_temporada(temporada, esperado):
    assert rebaixados_por_temporada(temporada) == esperado


def test_rebaixados_antes_de_2003():
    with pytest.raises(ValueError, match="2002"):
        rebaixados_por_temporada(2002)
