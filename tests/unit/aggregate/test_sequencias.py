from datetime import date

from pyspark.sql import functions as F

from brasileirao.aggregate.sequencias import calcular_sequencias

from .conftest import MINICAMPEONATO

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("tipo", "string"),
    ("jogos", "int"),
    ("data_inicio", "date"),
    ("data_fim", "date"),
    ("em_aberto", "int"),
]


def _d(mes, dia):
    return date(2019, mes, dia)


# Resultados em ordem de data (Atlas × Cometa da rodada 2 é em 25/05):
#   Atlas   V V V E E V      Bravo  D E V D V D
#   Cometa  E D E E E V      Delta  E E D E D D
ESPERADO = {
    ("Atlas", "invencibilidade"): (6, _d(5, 1), _d(6, 8), 1),
    ("Atlas", "vitorias"): (3, _d(5, 1), _d(5, 22), 0),
    ("Atlas", "jejum"): (2, _d(5, 25), _d(6, 1), 0),
    ("Atlas", "derrotas"): (0, None, None, 0),
    ("Bravo", "invencibilidade"): (2, _d(5, 8), _d(5, 15), 0),
    ("Bravo", "vitorias"): (1, _d(5, 15), _d(5, 15), 0),
    ("Bravo", "jejum"): (2, _d(5, 1), _d(5, 8), 0),
    ("Bravo", "derrotas"): (1, _d(5, 1), _d(5, 1), 0),
    ("Cometa", "invencibilidade"): (4, _d(5, 22), _d(6, 8), 1),
    ("Cometa", "vitorias"): (1, _d(6, 8), _d(6, 8), 1),
    ("Cometa", "jejum"): (5, _d(5, 1), _d(6, 1), 0),
    ("Cometa", "derrotas"): (1, _d(5, 15), _d(5, 15), 0),
    ("Delta", "invencibilidade"): (2, _d(5, 1), _d(5, 8), 0),
    ("Delta", "vitorias"): (0, None, None, 0),
    ("Delta", "jejum"): (6, _d(5, 1), _d(6, 8), 1),
    ("Delta", "derrotas"): (2, _d(6, 1), _d(6, 8), 1),
}


def _por_clube_e_tipo(df):
    return {(r["clube"], r["tipo"]): tuple(r)[3:] for r in df.collect()}


def test_schema_e_quatro_linhas_por_clube(silver_partidas):
    df = calcular_sequencias(silver_partidas())

    assert df.dtypes == COLUNAS
    assert df.count() == 16
    assert {r["temporada"] for r in df.collect()} == {2019}


def test_sequencias_do_minicampeonato(silver_partidas):
    assert _por_clube_e_tipo(calcular_sequencias(silver_partidas())) == ESPERADO


def test_partida_adiada_entra_pela_data(silver_partidas):
    # Pela rodada, o Atlas faria V E V V E V e a maior série de vitórias teria 2 jogos.
    # Pela data, o empate com o Cometa (rodada 2) cai em 25/05 e a série vai a 3.
    sequencias = _por_clube_e_tipo(calcular_sequencias(silver_partidas()))

    assert sequencias[("Atlas", "vitorias")][0] == 3


def test_empate_de_tamanho_fica_com_a_mais_antiga(silver_partidas):
    # Bravo tem três derrotas isoladas; a de 08/06 chega ao fim, mas a de 01/05 é a escolhida.
    sequencias = _por_clube_e_tipo(calcular_sequencias(silver_partidas()))

    assert sequencias[("Bravo", "derrotas")] == (1, _d(5, 1), _d(5, 1), 0)


def test_hora_e_partida_id_desempatam_o_mesmo_dia(silver_partidas):
    # Atlas joga duas vezes em 01/05: a vitória sobre o Bravo às 16:00 e uma derrota às 11:00
    # para o Eco. A derrota vem antes, e a invencibilidade não é quebrada no meio.
    extra = (13, 1, _d(5, 1), "Eco", "Atlas", 1, 0)
    partidas = silver_partidas([*MINICAMPEONATO, extra]).withColumn(
        "hora", F.when(F.col("partida_id") == 13, "11:00").otherwise(F.col("hora"))
    )

    atlas = _por_clube_e_tipo(calcular_sequencias(partidas))
    assert atlas[("Atlas", "invencibilidade")] == (6, _d(5, 1), _d(6, 8), 1)
    assert atlas[("Atlas", "derrotas")] == (1, _d(5, 1), _d(5, 1), 0)


def test_temporadas_separadas(silver_partidas):
    duas = silver_partidas().unionByName(silver_partidas(temporada=2020))

    df = calcular_sequencias(duas)

    assert df.count() == 32
    atlas_2020 = df.where("temporada = 2020 and clube = 'Atlas' and tipo = 'invencibilidade'")
    assert atlas_2020.first()["jogos"] == 6
