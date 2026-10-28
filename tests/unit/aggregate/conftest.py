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


# Alguns gols do minicampeonato (não todos), com os casos de borda das tabelas de gols.
# (partida_id, clube, atleta, minuto, acrescimo, tipo_de_gol)
GOLS = [
    (1, "Atlas", "Ari", 0, None, None),
    (1, "Atlas", "Ari", 45, 2, "Penalty"),
    (5, "Atlas", "Ari", 46, None, None),
    (5, "Atlas", "Beto", 90, 3, None),
    (5, "Atlas", "Dino", 30, None, "Gol Contra"),  # Dino é do Delta: conta para o Atlas
    (5, "Delta", "Dino", 60, None, None),
    (6, "Bravo", "Caio", 15, None, None),
    (12, "Cometa", "Caio", 16, None, None),  # Caio trocou de clube na temporada
    (12, "Cometa", "Caio", 75, None, "Penalty"),
    (9, "Atlas", "Beto", 31, None, None),
    (9, "Cometa", "Edu", 76, None, None),
    (9, "Cometa", "Edu", 90, None, None),
]
# (partida_id, clube, cartao, atleta). O Delta não leva cartão.
CARTOES = [
    (1, "Bravo", "Amarelo", "Bia"),
    (1, "Bravo", "Vermelho", "Bia"),
    (3, "Atlas", "Amarelo", "Ari"),
    (6, "Cometa", "Amarelo", "Caio"),
    (6, "Cometa", "Amarelo", "Edu"),
    (9, "Cometa", "Vermelho", "Edu"),
]
# (partida_id, clube, cartao_amarelo, cartao_vermelho)
ESTATISTICAS = [
    (1, "Atlas", 2, 0),
    (1, "Bravo", 3, 1),
    (2, "Cometa", 1, 0),
    (2, "Delta", 0, 0),
]
SCHEMA_GOLS = (
    "partida_id int, temporada int, rodada int, clube string, atleta string, minuto int, "
    "acrescimo int, tipo_de_gol string"
)
SCHEMA_CARTOES = (
    "partida_id int, temporada int, rodada int, clube string, cartao string, atleta string, "
    "num_camisa int, posicao string, minuto int, acrescimo int"
)
SCHEMA_ESTATISTICAS = (
    "partida_id int, temporada int, rodada int, clube string, chutes int, chutes_no_alvo int, "
    "posse_de_bola double, passes int, precisao_passes double, faltas int, "
    "cartao_amarelo int, cartao_vermelho int, impedimentos int, escanteios int"
)
RODADA = {p[0]: p[1] for p in MINICAMPEONATO}


def _colunas(schema):
    return [c.split()[0] for c in schema.split(", ")]


def _fabrica_filha(spark, schema, campos):
    """Fábrica de uma silver filha: cada linha preenche `campos`, a rodada sai do
    `partida_id` do minicampeonato e as outras colunas ficam nulas."""
    colunas = _colunas(schema)

    def fabricar(linhas, temporada=TEMPORADA):
        dicts = [
            dict.fromkeys(colunas)
            | dict(zip(campos, linha, strict=True))
            | {"temporada": temporada, "rodada": RODADA.get(linha[0])}
            for linha in linhas
        ]
        return spark.createDataFrame(dicts, schema)

    return fabricar


@pytest.fixture
def silver_gols(spark):
    """Fábrica no schema de `silver/gols`; sem argumentos, `GOLS` em 2019."""
    fabricar = _fabrica_filha(
        spark,
        SCHEMA_GOLS,
        ["partida_id", "clube", "atleta", "minuto", "acrescimo", "tipo_de_gol"],
    )
    return lambda gols=GOLS, temporada=TEMPORADA: fabricar(gols, temporada)


@pytest.fixture
def silver_cartoes(spark):
    """Fábrica no schema de `silver/cartoes`; sem argumentos, `CARTOES` em 2019."""
    fabricar = _fabrica_filha(spark, SCHEMA_CARTOES, ["partida_id", "clube", "cartao", "atleta"])
    return lambda cartoes=CARTOES, temporada=TEMPORADA: fabricar(cartoes, temporada)


@pytest.fixture
def silver_estatisticas(spark):
    """Fábrica no schema de `silver/estatisticas`; sem argumentos, `ESTATISTICAS` em 2019."""
    fabricar = _fabrica_filha(
        spark,
        SCHEMA_ESTATISTICAS,
        ["partida_id", "clube", "cartao_amarelo", "cartao_vermelho"],
    )
    return lambda linhas=ESTATISTICAS, temporada=TEMPORADA: fabricar(linhas, temporada)
