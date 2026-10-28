"""Minicampeonato de 4 times em turno e returno, reaproveitado pelas suítes do gold.

Tabela final de campo (3/1/0):

    clube   P  J  V  E  D  GP GC  SG
    Atlas  14  6  4  2  0  11  4  +7   nunca perde
    Bravo   7  6  2  1  3   2  6  -4   à frente do Cometa por vitórias
    Cometa  7  6  1  4  1   6  5  +1
    Delta   3  6  0  3  3   2  6  -4

Com o ajuste do Atlas (−10 e +2 = −8) na última rodada, a final fica Bravo, Cometa, Atlas,
Delta. Atlas × Cometa da rodada 2 foi jogada depois da rodada 4. Nas rodadas 3 e 4, Cometa
e Delta empatam em pontos e vitórias e o saldo decide.
"""

from datetime import date

import pytest

TEMPORADA = 2019
SCHEMA_PARTIDAS = (
    "partida_id int, temporada int, rodada int, data date, hora string, mandante string, "
    "visitante string, placar_mandante int, placar_visitante int, vencedor string, "
    "arena string, uf_mandante string, uf_visitante string, tecnico_mandante string, "
    "tecnico_visitante string, formacao_mandante string, formacao_visitante string, "
    "arrecadacao decimal(14,2)"
)
# (partida_id, rodada, data, mandante, visitante, placar_mandante, placar_visitante)
MINICAMPEONATO = [
    (1, 1, date(2019, 5, 1), "Atlas", "Bravo", 2, 0),
    (2, 1, date(2019, 5, 1), "Cometa", "Delta", 1, 1),
    (3, 2, date(2019, 5, 25), "Atlas", "Cometa", 1, 1),  # adiada: depois da rodada 4
    (4, 2, date(2019, 5, 8), "Bravo", "Delta", 0, 0),
    (5, 3, date(2019, 5, 15), "Atlas", "Delta", 3, 1),
    (6, 3, date(2019, 5, 15), "Bravo", "Cometa", 1, 0),
    (7, 4, date(2019, 5, 22), "Bravo", "Atlas", 0, 2),
    (8, 4, date(2019, 5, 22), "Delta", "Cometa", 0, 0),
    (9, 5, date(2019, 6, 1), "Cometa", "Atlas", 2, 2),
    (10, 5, date(2019, 6, 1), "Delta", "Bravo", 0, 1),
    (11, 6, date(2019, 6, 8), "Delta", "Atlas", 0, 1),
    (12, 6, date(2019, 6, 8), "Cometa", "Bravo", 2, 0),
]
AJUSTES = [
    (TEMPORADA, "Atlas", -10, "punição de teste", "https://exemplo"),
    (TEMPORADA, "Atlas", 2, "pontos devolvidos de teste", "https://exemplo"),
]


COLUNAS_PARTIDAS = [c.split()[0] for c in SCHEMA_PARTIDAS.split(", ")]


def _linha_silver(partida_id, rodada, data, mandante, visitante, gm, gv, temporada=TEMPORADA):
    vencedor = mandante if gm > gv else visitante if gv > gm else None
    return dict.fromkeys(COLUNAS_PARTIDAS) | {
        "partida_id": partida_id,
        "temporada": temporada,
        "rodada": rodada,
        "data": data,
        "hora": "16:00",
        "mandante": mandante,
        "visitante": visitante,
        "placar_mandante": gm,
        "placar_visitante": gv,
        "vencedor": vencedor,
    }


@pytest.fixture
def silver_partidas(spark):
    """Fábrica no schema de `silver/partidas`; sem argumentos, o minicampeonato."""

    def fabricar(partidas=MINICAMPEONATO, temporada=TEMPORADA):
        linhas = [_linha_silver(*p, temporada=temporada) for p in partidas]
        return spark.createDataFrame(linhas, SCHEMA_PARTIDAS)

    return fabricar


@pytest.fixture
def ajustes(spark):
    return spark.createDataFrame(
        AJUSTES, "temporada int, clube string, pontos int, motivo string, fonte string"
    )


@pytest.fixture
def sem_ajustes(spark):
    return spark.createDataFrame(
        [], "temporada int, clube string, pontos int, motivo string, fonte string"
    )
