from pyspark.sql import functions as F

from brasileirao.aggregate.linha_corte import calcular_linha_corte, checar_linha_corte

COLUNAS = [
    ("temporada", "int"),
    ("times", "int"),
    ("rebaixados", "int"),
    ("ultimo_salvo", "string"),
    ("pontos_ultimo_salvo", "int"),
    ("aproveitamento_ultimo_salvo", "double"),
    ("primeiro_rebaixado", "string"),
    ("pontos_primeiro_rebaixado", "int"),
    ("aproveitamento_primeiro_rebaixado", "double"),
    ("pontos_campeao", "int"),
    ("pontos_vice", "int"),
]
# (temporada, clube, posicao, pontos, aproveitamento, situacao)
CAMPANHAS = [
    (2019, "Atlas", 1, 80, 0.70, "campeao"),
    (2019, "Bravo", 2, 75, 0.66, "vice"),
    (2019, "Cometa", 3, 45, 0.40, "permaneceu"),
    (2019, "Delta", 4, 42, 0.38, "rebaixado"),
    (2019, "Eco", 5, 30, 0.26, "rebaixado"),
    (2003, "Atlas", 1, 90, 0.65, "campeao"),
    (2003, "Bravo", 2, 85, 0.62, "vice"),
    (2003, "Delta", 3, 50, 0.36, "permaneceu"),
    (2003, "Eco", 4, 48, 0.35, "rebaixado"),
]


def _campanhas(spark, linhas=CAMPANHAS):
    return spark.createDataFrame(
        linhas,
        "temporada int, clube string, posicao int, pontos int, aproveitamento double, "
        "situacao string",
    )


def _linhas(df):
    return {r["temporada"]: r.asDict() for r in df.collect()}


def test_schema_e_uma_linha_por_temporada(spark):
    df = calcular_linha_corte(_campanhas(spark))
    assert df.dtypes == COLUNAS
    assert df.count() == 2


def test_linha_de_corte(spark):
    linha = _linhas(calcular_linha_corte(_campanhas(spark)))[2019]
    assert (linha["times"], linha["rebaixados"]) == (5, 2)
    assert (linha["ultimo_salvo"], linha["pontos_ultimo_salvo"]) == ("Cometa", 45)
    assert linha["aproveitamento_ultimo_salvo"] == 0.40
    assert (linha["primeiro_rebaixado"], linha["pontos_primeiro_rebaixado"]) == ("Delta", 42)
    assert linha["aproveitamento_primeiro_rebaixado"] == 0.38
    assert (linha["pontos_campeao"], linha["pontos_vice"]) == (80, 75)


def test_temporadas_nao_se_misturam(spark):
    linha = _linhas(calcular_linha_corte(_campanhas(spark)))[2003]
    assert (linha["times"], linha["rebaixados"]) == (4, 1)
    assert linha["ultimo_salvo"] == "Delta"
    assert linha["primeiro_rebaixado"] == "Eco"


def test_checar_passa_com_a_tabela_certa(spark):
    campanhas = _campanhas(spark)
    assert checar_linha_corte(calcular_linha_corte(campanhas), campanhas) == []


def test_checar_acusa_temporada_faltando_e_ordem_invertida(spark):
    campanhas = _campanhas(spark)
    df = calcular_linha_corte(campanhas)
    assert checar_linha_corte(df.where("temporada = 2019"), campanhas) == [
        "linha_corte: temporada faltando: 2003"
    ]
    invertida = df.withColumn("pontos_primeiro_rebaixado", F.lit(99))
    assert any("fora de ordem" in p for p in checar_linha_corte(invertida, campanhas))
