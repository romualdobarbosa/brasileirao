import csv
from datetime import date
from importlib import resources

from pyspark.sql import functions as F

from brasileirao.aggregate.tecnicos import (
    SCHEMA_SEED,
    ULTIMA_TEMPORADA,
    calcular_passagens_tecnicos,
    carregar_seed_tecnicos,
    checar_passagens_tecnicos,
)

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("ordem", "int"),
    ("tecnico", "string"),
    ("data_inicio", "date"),
    ("data_fim", "date"),
    ("jogos", "int"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("pontos", "int"),
    ("aproveitamento", "double"),
]


def _d(mes, dia):
    return date(2019, mes, dia)


# Atlas em ordem de data: partidas 1, 5, 7, 3 (a adiada, em 25/05), 9, 11.
TODOS_DO_ATLAS = (1, 5, 7, 3, 9, 11)


def _com_tecnicos(spark, df, do_atlas, resto="Outro"):
    """Põe `do_atlas[partida_id]` no lado do Atlas e `resto` em todos os outros lados."""
    mapa = spark.createDataFrame(
        [(k, v) for k, v in do_atlas.items()], "partida_id int, _tecnico string"
    )
    return (
        df.join(F.broadcast(mapa), on="partida_id", how="left")
        .withColumn(
            "tecnico_mandante",
            F.when(F.col("mandante") == "Atlas", F.col("_tecnico")).otherwise(F.lit(resto)),
        )
        .withColumn(
            "tecnico_visitante",
            F.when(F.col("visitante") == "Atlas", F.col("_tecnico")).otherwise(F.lit(resto)),
        )
        .drop("_tecnico")
    )


def _seed(spark, linhas=()):
    return spark.createDataFrame(list(linhas), SCHEMA_SEED)


def _passagens(df):
    return [
        (r["ordem"], r["tecnico"], r["jogos"])
        for r in df.where("clube = 'Atlas'").orderBy("ordem").collect()
    ]


def test_schema(spark, silver_partidas):
    df = _com_tecnicos(spark, silver_partidas(), dict.fromkeys(TODOS_DO_ATLAS, "Ari"))
    assert calcular_passagens_tecnicos(df, _seed(spark)).dtypes == COLUNAS


def test_uma_passagem_com_as_metricas(spark, silver_partidas):
    df = _com_tecnicos(spark, silver_partidas(), dict.fromkeys(TODOS_DO_ATLAS, "Ari"))
    linha = calcular_passagens_tecnicos(df, _seed(spark)).where("clube = 'Atlas'").first()
    assert (linha["ordem"], linha["tecnico"], linha["jogos"]) == (1, "Ari", 6)
    assert (linha["vitorias"], linha["empates"], linha["derrotas"]) == (4, 2, 0)
    assert (linha["gols_pro"], linha["gols_contra"]) == (11, 4)
    assert linha["pontos"] == 14
    assert linha["aproveitamento"] == 0.7778
    assert (linha["data_inicio"], linha["data_fim"]) == (_d(5, 1), _d(6, 8))


def test_sai_e_volta_gera_duas_passagens(spark, silver_partidas):
    tecnicos = {1: "Ari", 5: "Ari", 7: "Bia", 3: "Bia", 9: "Ari", 11: "Ari"}
    df = _com_tecnicos(spark, silver_partidas(), tecnicos)
    assert _passagens(calcular_passagens_tecnicos(df, _seed(spark))) == [
        (1, "Ari", 2),
        (2, "Bia", 2),
        (3, "Ari", 2),
    ]


def test_segue_a_data_e_nao_a_rodada(spark, silver_partidas):
    # Na ordem de rodada (1, 3, 5, 7, 9, 11), a partida 3 cortaria o Ari em dois.
    tecnicos = {1: "Ari", 5: "Ari", 7: "Ari", 3: "Bia", 9: "Bia", 11: "Bia"}
    df = _com_tecnicos(spark, silver_partidas(), tecnicos)
    assert _passagens(calcular_passagens_tecnicos(df, _seed(spark))) == [
        (1, "Ari", 3),
        (2, "Bia", 3),
    ]


def test_tecnico_nulo_fica_de_fora_e_nao_interrompe(spark, silver_partidas):
    tecnicos = {1: "Ari", 5: "Ari", 7: None, 3: "Ari", 9: "Ari", 11: "Ari"}
    df = _com_tecnicos(spark, silver_partidas(), tecnicos)
    assert _passagens(calcular_passagens_tecnicos(df, _seed(spark))) == [(1, "Ari", 5)]


def test_seed_unifica_grafias_antes_das_ilhas(spark, silver_partidas):
    tecnicos = {1: "A. Ari", 5: "Ari Silva", 7: "A. Ari", 3: "Ari Silva", 9: "A. Ari", 11: "A. Ari"}
    df = _com_tecnicos(spark, silver_partidas(), tecnicos)
    seed = _seed(spark, [("A. Ari", None, None, "Ari"), ("Ari Silva", None, None, "Ari")])
    assert _passagens(calcular_passagens_tecnicos(df, seed)) == [(1, "Ari", 6)]


def test_seed_vale_a_linha_mais_especifica(spark, silver_partidas):
    df = _com_tecnicos(spark, silver_partidas(), dict.fromkeys(TODOS_DO_ATLAS, "A. Ari"), "A. Ari")
    geral = [("A. Ari", None, None, "Geral")]
    do_clube = [("A. Ari", "Atlas", None, "Do clube")]
    da_temporada = [("A. Ari", "Atlas", 2019, "Da temporada")]
    outra_temporada = [("A. Ari", "Atlas", 2018, "Outra temporada")]

    def nomes(linhas):
        df_final = calcular_passagens_tecnicos(df, _seed(spark, linhas))
        return {r["clube"]: r["tecnico"] for r in df_final.collect()}

    assert nomes(geral)["Atlas"] == "Geral"
    assert nomes(geral + do_clube)["Atlas"] == "Do clube"
    # O seed de um clube não vaza para os outros clubes com o mesmo nome na fonte.
    assert nomes(geral + do_clube)["Bravo"] == "Geral"
    assert nomes(geral + do_clube + da_temporada)["Atlas"] == "Da temporada"
    assert nomes(geral + do_clube + outra_temporada)["Atlas"] == "Do clube"


def test_sem_seed_fica_o_nome_da_fonte(spark, silver_partidas):
    df = _com_tecnicos(spark, silver_partidas(), dict.fromkeys(TODOS_DO_ATLAS, "A. Ari"))
    seed = _seed(spark, [("Outro nome", None, None, "Alguém")])
    assert _passagens(calcular_passagens_tecnicos(df, seed)) == [(1, "A. Ari", 6)]


def test_temporada_depois_do_corte_fica_de_fora(spark, silver_partidas):
    depois = ULTIMA_TEMPORADA + 1
    df = _com_tecnicos(
        spark,
        silver_partidas().unionByName(silver_partidas(temporada=depois)),
        dict.fromkeys(TODOS_DO_ATLAS, "Ari"),
    )
    temporadas = {r["temporada"] for r in calcular_passagens_tecnicos(df, _seed(spark)).collect()}
    assert temporadas == {2019}


def test_checar_passa_e_acusa_diferenca(spark, silver_partidas):
    tecnicos = {1: "Ari", 5: "Ari", 7: None, 3: "Ari", 9: "Ari", 11: "Ari"}
    partidas = _com_tecnicos(spark, silver_partidas(), tecnicos)
    df = calcular_passagens_tecnicos(partidas, _seed(spark))
    assert checar_passagens_tecnicos(df, partidas) == []
    # 12 partidas × 2 lados − o jogo nulo do Atlas = 23; sem o Atlas (5 jogos), 18.
    problemas = checar_passagens_tecnicos(df.where("clube != 'Atlas'"), partidas)
    assert problemas == [
        "passagens_tecnicos: jogos ≠ jogos de clube com técnico: 2019: 18 na tabela, 23 na silver"
    ]


def _linhas_do_seed():
    texto = resources.files("brasileirao.reference").joinpath("tecnicos.csv").read_text("utf-8")
    return list(csv.DictReader(texto.splitlines()))


def test_seed_real_carrega(spark):
    seed = carregar_seed_tecnicos(spark)
    assert seed.count() == len(_linhas_do_seed())
    armadilha = seed.where("nome_fonte = 'A. Ferreira' and clube = 'Palmeiras'").first()
    assert armadilha["nome_popular"] == "Abel Ferreira"


def test_seed_real_sem_chave_repetida_nem_nome_vazio():
    linhas = _linhas_do_seed()
    chaves = [(r["nome_fonte"], r["clube"], r["temporada"]) for r in linhas]
    assert len(chaves) == len(set(chaves))
    assert all(r["nome_popular"] for r in linhas)


def test_seed_real_tem_clubes_canonicos():
    texto = resources.files("brasileirao.reference").joinpath("times.csv").read_text("utf-8")
    canonicos = {r["nome_canonico"] for r in csv.DictReader(texto.splitlines())}
    assert {r["clube"] for r in _linhas_do_seed() if r["clube"]} <= canonicos


def test_seed_real_separa_renato_gaucho_de_renato_paiva():
    nomes = {r["nome_fonte"]: r["nome_popular"] for r in _linhas_do_seed()}
    assert nomes["Renato Portaluppi"] == "Renato Gaúcho"
    assert nomes["Renato Manuel Alves Paiva"] == "Renato Paiva"
