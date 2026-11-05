from pyspark.sql import functions as F

from brasileirao.aggregate.campanhas import calcular_campanhas, checar_campanhas
from brasileirao.aggregate.classificacao import calcular_classificacao

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("posicao", "int"),
    ("pontos", "int"),
    ("ajuste_pontos", "int"),
    ("jogos", "int"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("saldo", "int"),
    ("aproveitamento", "double"),
    ("situacao", "string"),
]
SCHEMA_RESUMO = "temporada int, campeao string, vice string, rebaixados string"


def _classificacao(spark, temporada, times):
    """Só a última rodada, com os times do melhor para o pior e o resto zerado."""
    linhas = [
        (temporada, 38, clube, posicao, 40 - posicao, 0, 38, 0, 0, 0, 0, 0, 0, 0.5, "VVVVV")
        for posicao, clube in enumerate(times, start=1)
    ]
    return spark.createDataFrame(
        linhas,
        "temporada int, rodada int, clube string, posicao int, pontos int, ajuste_pontos int, "
        "jogos int, vitorias int, empates int, derrotas int, gols_pro int, gols_contra int, "
        "saldo int, aproveitamento double, ultimos_5 string",
    )


def _situacoes(df):
    return {r["clube"]: r["situacao"] for r in df.collect()}


def test_schema_e_uma_linha_por_clube(silver_partidas, ajustes):
    df = calcular_campanhas(calcular_classificacao(silver_partidas(), ajustes))
    assert df.dtypes == COLUNAS
    assert df.count() == 4


def test_usa_a_ultima_rodada_com_o_ajuste(silver_partidas, ajustes):
    df = calcular_campanhas(calcular_classificacao(silver_partidas(), ajustes))
    atlas = df.where("clube = 'Atlas'").first()
    assert (atlas["posicao"], atlas["pontos"], atlas["ajuste_pontos"]) == (3, 6, -8)
    # O aproveitamento repete o da classificação: é de campo, sem a punição.
    assert atlas["aproveitamento"] == 0.7778


def test_situacao_com_4_rebaixados(spark):
    times = [f"T{n:02d}" for n in range(1, 8)]
    situacoes = _situacoes(calcular_campanhas(_classificacao(spark, 2019, times)))
    assert situacoes == {
        "T01": "campeao",
        "T02": "vice",
        "T03": "permaneceu",
        "T04": "rebaixado",
        "T05": "rebaixado",
        "T06": "rebaixado",
        "T07": "rebaixado",
    }


def test_situacao_com_2_rebaixados_em_2003(spark):
    times = [f"T{n:02d}" for n in range(1, 8)]
    situacoes = _situacoes(calcular_campanhas(_classificacao(spark, 2003, times)))
    assert [situacoes[t] for t in times] == [
        "campeao",
        "vice",
        "permaneceu",
        "permaneceu",
        "permaneceu",
        "rebaixado",
        "rebaixado",
    ]


def test_checar_compara_com_o_resumo(spark):
    times = [f"T{n:02d}" for n in range(1, 8)]
    df = calcular_campanhas(_classificacao(spark, 2019, times))
    certo = spark.createDataFrame([(2019, "T01", "T02", "T04, T05, T06, T07")], SCHEMA_RESUMO)
    assert checar_campanhas(df, certo) == []
    errado = certo.withColumn("rebaixados", F.lit("T04, T05, T07, T06"))
    assert checar_campanhas(df, errado) == [
        "campanhas: situação difere de resumo_temporada em: 2019"
    ]
