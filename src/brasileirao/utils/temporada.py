from pyspark.sql import Column
from pyspark.sql import functions as F

FORMATO_DATA = "dd/MM/yyyy"


def derivar_temporada(data_texto: Column) -> Column:
    """Temporada a partir da data `dd/MM/yyyy` da fonte.

    A temporada 2020 terminou em fevereiro de 2021 por causa da pandemia: partidas de
    jan–mar/2021 pertencem a 2020.
    """
    data = F.to_date(data_texto, FORMATO_DATA)
    ano = F.year(data)
    return F.when((ano == 2021) & (F.month(data) <= 3), F.lit(2020)).otherwise(ano).cast("int")


def partidas_esperadas(temporada: int) -> int:
    """Nº de partidas de uma temporada completa. 2016 tem 379: Chapecoense × Atlético-MG
    não foi disputada depois do acidente aéreo."""
    if temporada in (2003, 2004):
        return 552
    if temporada == 2005:
        return 462
    if temporada == 2016:
        return 379
    return 380


def rebaixados_por_temporada(temporada: int) -> int:
    """Nº de rebaixados: 2 em 2003, 4 a partir de 2004."""
    if temporada < 2003:
        raise ValueError(f"Temporada {temporada} fora da fonte, que começa em 2003")
    return 2 if temporada == 2003 else 4
