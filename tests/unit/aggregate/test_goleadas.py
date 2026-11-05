from datetime import date

from pyspark.sql import functions as F

from brasileirao.aggregate.goleadas import calcular_goleadas, checar_goleadas

COLUNAS = [
    ("partida_id", "int"),
    ("temporada", "int"),
    ("rodada", "int"),
    ("data", "date"),
    ("mandante", "string"),
    ("visitante", "string"),
    ("placar_mandante", "int"),
    ("placar_visitante", "int"),
    ("diferenca", "int"),
    ("total_gols", "int"),
    ("arena", "string"),
]
D = date(2019, 5, 1)
PARTIDAS = [
    (1, 1, D, "Atlas", "Bravo", 3, 0),  # diferença 3, 3 gols: fora
    (2, 1, D, "Cometa", "Delta", 4, 0),  # diferença 4: entra
    (3, 2, D, "Atlas", "Cometa", 0, 5),  # goleada do visitante: entra
    (4, 2, D, "Bravo", "Delta", 4, 3),  # 7 gols: fora
    (5, 3, D, "Atlas", "Delta", 5, 3),  # diferença 2, 8 gols: entra
    (6, 3, D, "Bravo", "Cometa", 4, 4),  # empate, 8 gols: entra
]


def test_schema(silver_partidas):
    assert calcular_goleadas(silver_partidas(PARTIDAS)).dtypes == COLUNAS


def test_criterio_nas_duas_bordas(silver_partidas):
    linhas = calcular_goleadas(silver_partidas(PARTIDAS)).collect()
    assert {r["partida_id"] for r in linhas} == {2, 3, 5, 6}


def test_diferenca_e_total_com_goleada_do_visitante(silver_partidas):
    linha = calcular_goleadas(silver_partidas(PARTIDAS)).where("partida_id = 3").first()
    assert (linha["diferenca"], linha["total_gols"]) == (5, 5)


def test_checar_passa_com_a_tabela_certa(silver_partidas):
    assert checar_goleadas(calcular_goleadas(silver_partidas(PARTIDAS))) == []


def test_checar_acusa_repetida_e_fora_do_criterio(silver_partidas):
    df = calcular_goleadas(silver_partidas(PARTIDAS))
    repetida = df.where("partida_id = 2").unionByName(df)
    assert any("repetida: 2" in p for p in checar_goleadas(repetida))
    fora = df.withColumn("diferenca", F.lit(1)).withColumn("total_gols", F.lit(3))
    assert any("fora do critério" in p for p in checar_goleadas(fora))
