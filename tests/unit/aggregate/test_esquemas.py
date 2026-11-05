from pyspark.sql import functions as F

from brasileirao.aggregate.esquemas import calcular_esquemas_taticos, checar_esquemas_taticos

from .conftest import MINICAMPEONATO

COLUNAS = [
    ("temporada", "int"),
    ("esquema", "string"),
    ("jogos", "int"),
    ("uso_pct", "double"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("aproveitamento", "double"),
]


def _com_formacao(df, mandante="4-3-3", visitante="4-4-2"):
    return df.withColumn("formacao_mandante", F.lit(mandante)).withColumn(
        "formacao_visitante", F.lit(visitante)
    )


def _linhas(df):
    return {(r["temporada"], r["esquema"]): r.asDict() for r in df.collect()}


def test_schema(silver_partidas):
    df = calcular_esquemas_taticos(_com_formacao(silver_partidas()))
    assert df.dtypes == COLUNAS


def test_um_jogo_conta_para_cada_clube(silver_partidas):
    linhas = _linhas(calcular_esquemas_taticos(_com_formacao(silver_partidas())))
    assert linhas[(2019, "4-3-3")]["jogos"] == len(MINICAMPEONATO)
    assert linhas[(2019, "4-4-2")]["jogos"] == len(MINICAMPEONATO)
    assert linhas[(2019, "4-3-3")]["uso_pct"] == 0.5


def test_resultado_na_visao_de_quem_usou(silver_partidas):
    # Mandantes: 4 V (partidas 1, 5, 6, 12), 5 E, 3 D. Visitantes: o espelho.
    linhas = _linhas(calcular_esquemas_taticos(_com_formacao(silver_partidas())))
    mandante = linhas[(2019, "4-3-3")]
    assert (mandante["vitorias"], mandante["empates"], mandante["derrotas"]) == (4, 5, 3)
    assert mandante["aproveitamento"] == round((4 * 3 + 5) / (12 * 3), 4)
    visitante = linhas[(2019, "4-4-2")]
    assert (visitante["vitorias"], visitante["empates"], visitante["derrotas"]) == (3, 5, 4)


def test_2014_fica_de_fora(silver_partidas):
    df = _com_formacao(silver_partidas()).unionByName(
        _com_formacao(silver_partidas(temporada=2014))
    )
    assert {r["temporada"] for r in calcular_esquemas_taticos(df).collect()} == {2019}


def test_formacao_nula_fica_fora_tambem_do_uso(silver_partidas):
    # Metade dos clubes sem formação: o esquema informado fica com 100% de uso.
    df = _com_formacao(silver_partidas(), visitante=None)
    linhas = _linhas(calcular_esquemas_taticos(df))
    assert set(linhas) == {(2019, "4-3-3")}
    assert linhas[(2019, "4-3-3")]["uso_pct"] == 1.0


def test_checar_passa_com_a_tabela_certa(silver_partidas):
    df = calcular_esquemas_taticos(_com_formacao(silver_partidas()))
    assert checar_esquemas_taticos(df) == []


def test_checar_acusa_soma_e_temporada_antiga(silver_partidas):
    df = calcular_esquemas_taticos(_com_formacao(silver_partidas()))
    assert any("não soma 1" in p for p in checar_esquemas_taticos(df.where("esquema = '4-3-3'")))
    antiga = df.withColumn("temporada", F.lit(2014))
    assert any("anterior a 2015" in p for p in checar_esquemas_taticos(antiga))
