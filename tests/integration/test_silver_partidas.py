from datetime import date

import pytest
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from brasileirao import config
from brasileirao.ingestion import bronze
from brasileirao.transform import qualidade, silver

ARQUIVO = "campeonato-brasileiro-full.csv"


def _carregar(spark, temporada):
    bronze.escrever_bronze(spark, "partidas", temporada)
    silver.processar_silver(spark, temporada, ("partidas",))


def _silver(spark):
    return spark.read.format("delta").load(config.caminho_tabela("silver", "partidas"))


def _versao(spark):
    tabela = DeltaTable.forPath(spark, config.caminho_tabela("silver", "partidas"))
    return tabela.history(1).first()["version"]


def _editar_raw(raw, antigo, novo):
    caminho = raw / ARQUIVO
    texto = caminho.read_text(encoding="utf-8")
    assert antigo in texto
    caminho.write_text(texto.replace(antigo, novo), encoding="utf-8")


def test_primeira_carga(spark, raw):
    _carregar(spark, 2019)

    linhas = {r["partida_id"]: r for r in _silver(spark).collect()}
    assert sorted(linhas) == [10, 11, 12]
    assert linhas[10]["mandante"] == "São Paulo"
    assert linhas[10]["data"] == date(2019, 4, 27)
    assert linhas[11]["visitante"] == "Avaí"
    assert linhas[12]["vencedor"] == "Santos"


def test_regras_de_limpeza_nos_csvs(spark, raw):
    _carregar(spark, 2003)
    _carregar(spark, 2021)

    linhas = {r["partida_id"]: r for r in _silver(spark).collect()}
    assert linhas[1]["arena"] == "Brinco de Ouro"
    assert linhas[2]["vencedor"] is None
    assert linhas[2]["tecnico_mandante"] is None
    assert linhas[30]["arena"] is None
    assert linhas[30]["vencedor"] == "Náutico"


def test_recarga_atualiza_linha_alterada(spark, raw):
    _carregar(spark, 2019)
    _editar_raw(raw, '"Sao Paulo","Botafogo-RJ","4-2-3-1"', '"Sao Paulo","Botafogo-RJ","3-5-2"')

    _carregar(spark, 2019)

    df = _silver(spark)
    assert df.count() == 3
    assert df.where(F.col("partida_id") == 10).first()["formacao_mandante"] == "3-5-2"


def test_recarga_apaga_partida_que_saiu_da_fonte(spark, raw):
    _carregar(spark, 2019)
    _carregar(spark, 2020)
    _editar_raw(raw, '"11","1","27/04/2019"', '"11","1","27/04/2018"')

    _carregar(spark, 2019)

    ids = {r["partida_id"] for r in _silver(spark).collect()}
    assert ids == {10, 12, 20, 21, 22}


def test_recarga_sem_mudanca_nao_cria_versao(spark, raw):
    _carregar(spark, 2019)
    versao = _versao(spark)

    _carregar(spark, 2019)

    assert _versao(spark) == versao


def test_time_sem_mapa_falha_sem_criar_a_silver(spark, raw):
    _editar_raw(raw, '"Atletico-MG","Avai"', '"Atletico Mineiro","Avai"')

    with pytest.raises(qualidade.QualidadeError, match="Atletico Mineiro"):
        _carregar(spark, 2019)

    assert not DeltaTable.isDeltaTable(spark, config.caminho_tabela("silver", "partidas"))


def test_falha_deixa_a_silver_intacta(spark, raw):
    _carregar(spark, 2019)
    antes = _silver(spark).collect()
    versao = _versao(spark)
    _editar_raw(raw, '"Gremio","Santos","4-2-3-1"', '"Gremio","Santos FC","4-2-3-1"')
    _editar_raw(raw, '"2","0","SP","RJ"', '"-1","0","SP","RJ"')

    with pytest.raises(qualidade.QualidadeError) as erro:
        _carregar(spark, 2019)

    assert "Santos FC" in str(erro.value)
    assert "partidas: 10" in str(erro.value)
    assert _versao(spark) == versao
    assert _silver(spark).collect() == antes
