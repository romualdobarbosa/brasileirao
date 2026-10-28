import pytest

from brasileirao.aggregate.artilharia import calcular_artilharia
from brasileirao.aggregate.classificacao import calcular_classificacao
from brasileirao.aggregate.resumo import calcular_resumo

COLUNAS = [
    ("temporada", "int"),
    ("times", "int"),
    ("partidas", "int"),
    ("gols", "int"),
    ("media_gols", "double"),
    ("pct_vitoria_mandante", "double"),
    ("pct_empate", "double"),
    ("pct_vitoria_visitante", "double"),
    ("campeao", "string"),
    ("vice", "string"),
    ("rebaixados", "string"),
    ("artilheiro", "string"),
    ("gols_artilheiro", "int"),
]


@pytest.fixture
def resumo(spark, silver_partidas, silver_gols):
    """O minicampeonato em 2003 (2 rebaixados, ajuste de −8 no Atlas, sem gols na silver) e
    em 2019 (4 rebaixados, sem ajuste, com gols)."""
    partidas = silver_partidas(temporada=2003).unionByName(silver_partidas())
    ajustes = spark.createDataFrame(
        [(2003, "Atlas", -8, "punição de teste", "https://exemplo")],
        "temporada int, clube string, pontos int, motivo string, fonte string",
    )
    classificacao = calcular_classificacao(partidas, ajustes)
    return calcular_resumo(partidas, classificacao, calcular_artilharia(silver_gols()))


def test_resumo_por_temporada(resumo):
    assert resumo.dtypes == COLUNAS
    # 12 partidas e 21 gols: 4 vitórias do mandante, 5 empates e 3 do visitante.
    totais = (4, 12, 21, 1.75, 0.3333, 0.4167, 0.25)
    assert [tuple(r) for r in resumo.orderBy("temporada").collect()] == [
        # Com o ajuste, a final de 2003 é Bravo, Cometa, Atlas, Delta.
        (2003, *totais, "Bravo", "Cometa", "Atlas, Delta", None, None),
        # Ari e Caio empatam na artilharia de 2019 com 3 gols.
        (2019, *totais, "Atlas", "Bravo", "Atlas, Bravo, Cometa, Delta", "Ari, Caio", 3),
    ]
    assert sum(totais[-3:]) == pytest.approx(1, abs=1e-3)
