import pytest
from delta.tables import DeltaTable

from brasileirao import config, pipeline
from brasileirao.ingestion import bronze
from brasileirao.transform import qualidade


def _ler(spark, camada, tabela):
    return spark.read.format("delta").load(config.caminho_tabela(camada, tabela))


def test_bronze_da_filha_herda_a_temporada_da_partida(spark, raw):
    bronze.escrever_bronze(spark, "partidas", 2020)

    total = bronze.escrever_bronze(spark, "gols", 2020)

    df = _ler(spark, "bronze", "gols")
    assert total == 2
    assert {(r["partida_id"], r["temporada"]) for r in df.collect()} == {("21", 2020), ("22", 2020)}
    tipos = dict(df.dtypes)
    assert tipos.pop("temporada") == "int"
    assert tipos.pop("_ingested_at") == "timestamp"
    assert set(tipos.values()) == {"string"}
    assert df.first()["_source_file"].endswith("raw/campeonato-brasileiro-gols.csv")


def test_partida_repetida_na_fonte_nao_duplica_a_filha(spark, raw):
    bronze.escrever_bronze(spark, "partidas", 2019)

    bronze.escrever_bronze(spark, "gols", 2019)

    gols_da_12 = _ler(spark, "bronze", "gols").where("partida_id = '12'")
    assert gols_da_12.count() == 2


def test_entidade_desconhecida(spark, raw):
    with pytest.raises(ValueError, match="arbitros"):
        bronze.escrever_bronze(spark, "arbitros", 2019)


def test_silver_das_filhas(spark, raw):
    pipeline.processar_temporada(spark, 2019)

    linhas = _ler(spark, "silver", "gols").collect()
    gols = {(r["partida_id"], r["atleta"], r["minuto"]): r for r in linhas}
    assert len(gols) == 6
    assert gols[(10, "Pato", 45)]["acrescimo"] == 2
    assert gols[(10, "Pato", 45)]["tipo_de_gol"] == "Penalty"
    assert gols[(10, "Pato", 31)]["tipo_de_gol"] is None
    assert gols[(11, "Getúlio", 60)]["clube"] == "Avaí"

    cartoes = {r["atleta"]: r for r in _ler(spark, "silver", "cartoes").collect()}
    assert cartoes["Betão"]["posicao"] == "Zagueiro"
    assert cartoes["Gabriel"]["posicao"] is None
    assert cartoes["Gabriel"]["num_camisa"] is None
    assert (cartoes["Gabriel"]["minuto"], cartoes["Gabriel"]["acrescimo"]) == (90, 3)
    assert cartoes["Arboleda"]["clube"] == "São Paulo"

    estatisticas = {
        (r["partida_id"], r["clube"]): r for r in _ler(spark, "silver", "estatisticas").collect()
    }
    assert sorted(estatisticas) == [
        (10, "Botafogo-RJ"),
        (10, "São Paulo"),
        (11, "Atlético-MG"),
        (11, "Avaí"),
        (12, "Santos"),
    ]
    assert estatisticas[(10, "São Paulo")]["posse_de_bola"] == pytest.approx(0.55)
    assert estatisticas[(11, "Atlético-MG")]["posse_de_bola"] is None


def test_falha_na_filha_nao_grava_nenhuma_tabela(spark, raw):
    caminho = raw / "campeonato-brasileiro-gols.csv"
    texto = caminho.read_text(encoding="utf-8")
    caminho.write_text(texto.replace('"Avai","Getúlio"', '"Avai FC","Getúlio"'), encoding="utf-8")

    with pytest.raises(qualidade.QualidadeError, match="silver/gols.*\n.*Avai FC"):
        pipeline.processar_temporada(spark, 2019)

    for tabela in ("partidas", "gols", "cartoes", "estatisticas"):
        assert not DeltaTable.isDeltaTable(spark, config.caminho_tabela("silver", tabela))
