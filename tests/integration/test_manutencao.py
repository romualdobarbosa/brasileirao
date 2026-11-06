"""OPTIMIZE, VACUUM e time travel, cada teste com a própria tabela em `tmp_path`."""

import os

import pytest
from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.errors import IllegalArgumentException
from pyspark.sql import functions as F

from brasileirao import config, pipeline
from brasileirao.lake import TabelaInvalidaError, caminho_de
from brasileirao.lake.historico import (
    VersaoIndisponivelError,
    comparar_versoes,
    historico,
    imprimir_historico,
    ler_versao,
)
from brasileirao.lake.manutencao import ZORDER, manter_tabela, manutencao, medir, relatorio

TRAVA = "spark.databricks.delta.retentionDurationCheck.enabled"
SCHEMA = "partida_id int, temporada int, rodada int, clube string"


def _gravar_em_partes(spark, caminho, partes=3, particao=None):
    """Uma versão e um arquivo por parte."""
    for i in range(partes):
        escrita = (
            spark.createDataFrame([(i, 2019 + i % 2, i + 1, f"clube{i}")], SCHEMA)
            .coalesce(1)
            .write.format("delta")
            .mode("append")
        )
        if particao:
            escrita = escrita.partitionBy(particao)
        escrita.save(caminho)


@pytest.fixture
def tabela(spark, data_root):
    """`gold/teste`: três versões, um arquivo por versão."""
    _gravar_em_partes(spark, config.caminho_tabela("gold", "teste"))
    return "gold/teste"


@pytest.fixture
def sem_trava(spark):
    """Desliga a trava de retenção só durante o teste e devolve a sessão como estava."""
    anterior = spark.conf.get(TRAVA, None)
    spark.conf.set(TRAVA, "false")
    yield
    if anterior is None:
        spark.conf.unset(TRAVA)
    else:
        spark.conf.set(TRAVA, anterior)


def _operacoes(spark, tabela_nome):
    linhas = historico(spark, tabela_nome).orderBy("version").collect()
    return [linha["operation"] for linha in linhas]


def test_vacuum_remove_versoes_antigas_e_a_leitura_falha(spark, tabela, sem_trava):
    caminho = caminho_de(tabela)
    alvo = DeltaTable.forPath(spark, caminho)
    alvo.optimize().executeCompaction()
    assert ler_versao(spark, tabela, 0).collect()
    assert medir(spark, caminho).arquivos_disco == 4

    alvo.vacuum(0)

    assert medir(spark, caminho).arquivos_disco == 1
    with pytest.raises(Py4JJavaError, match="(?i)FileNotFound|FILE_NOT_FOUND|DELTA_"):
        ler_versao(spark, tabela, 0).collect()
    assert len(spark.read.format("delta").load(caminho).collect()) == 3


def test_count_da_versao_removida_nao_prova_nada(spark, tabela, sem_trava):
    """O `count` sai das estatísticas do log, sem abrir arquivo: responde mesmo com o arquivo
    apagado. Por isso o teste acima usa `collect`."""
    alvo = DeltaTable.forPath(spark, caminho_de(tabela))
    alvo.optimize().executeCompaction()
    alvo.vacuum(0)
    assert ler_versao(spark, tabela, 2).count() == 3


def test_versao_que_nunca_existiu(spark, tabela):
    with pytest.raises(VersaoIndisponivelError, match="999"):
        ler_versao(spark, tabela, 999)


def test_versao_alem_da_retencao_e_recusada_antes_de_ler(spark, tabela):
    """Com o tempo da retenção zerado na tabela, faz o papel da versão de mais de 7 dias."""
    caminho = os.path.abspath(caminho_de(tabela))
    spark.sql(
        f"ALTER TABLE delta.`{caminho}` "
        "SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 seconds')"
    )
    with pytest.raises(VersaoIndisponivelError):
        ler_versao(spark, tabela, 0)


def test_erro_de_leitura_que_nao_e_de_versao_passa_direto(spark, data_root):
    """Pasta sem tabela não é versão indisponível, é outro problema."""
    caminho = data_root / "gold" / "sem_delta"
    caminho.mkdir(parents=True)
    (caminho / "_delta_log").mkdir()
    with pytest.raises(TabelaInvalidaError):
        ler_versao(spark, "gold/sem_delta", 0)


def test_trava_ligada_recusa_retencao_curta(spark, tabela):
    """Roda depois dos testes com `sem_trava`: também prova que a sessão voltou ao normal."""
    assert spark.conf.get(TRAVA, None) is None
    with pytest.raises(IllegalArgumentException, match="DELTA_VACUUM_RETENTION_PERIOD_TOO_SHORT"):
        DeltaTable.forPath(spark, caminho_de(tabela)).vacuum(0)


def test_manutencao_padrao_compacta_e_guarda_as_versoes(spark, tabela):
    caminho = caminho_de(tabela)
    antes = sorted(map(repr, spark.read.format("delta").load(caminho).collect()))

    resultado = manter_tabela(spark, tabela)

    assert (resultado.antes.arquivos, resultado.depois.arquivos) == (3, 1)
    # Retenção de 7 dias: o VACUUM não apaga nada recente, e a versão antiga continua legível.
    assert (resultado.antes.arquivos_disco, resultado.depois.arquivos_disco) == (3, 4)
    assert ler_versao(spark, tabela, 0).collect()
    assert sorted(map(repr, spark.read.format("delta").load(caminho).collect())) == antes
    assert _operacoes(spark, tabela)[-3:] == ["OPTIMIZE", "VACUUM START", "VACUUM END"]
    assert spark.conf.get(TRAVA, None) is None


def test_manutencao_com_zorder(spark, tabela, monkeypatch):
    monkeypatch.setitem(ZORDER, tabela, ("temporada", "rodada"))
    resultado = manter_tabela(spark, tabela)
    assert resultado.zorder == ("temporada", "rodada")
    assert resultado.depois.arquivos == 1
    otimizacao = historico(spark, tabela).where(F.col("operation") == "OPTIMIZE").first()
    assert "temporada" in otimizacao["operationParameters"]["zOrderBy"]


def test_zorder_nao_aceita_coluna_de_particao(spark, data_root):
    caminho = config.caminho_tabela("silver", "teste")
    _gravar_em_partes(spark, caminho, particao="temporada")
    with pytest.raises(IllegalArgumentException, match="DELTA_ZORDERING_ON_PARTITION_COLUMN"):
        DeltaTable.forPath(spark, caminho).optimize().executeZOrderBy("temporada")


def test_manutencao_pula_a_tabela_ausente_e_imprime(spark, tabela, capsys, caplog):
    with caplog.at_level("WARNING"):
        resultados = manutencao(spark, (tabela, "gold/nao_existe"))
    assert [r.tabela for r in resultados] == [tabela]
    assert "gold/nao_existe" in caplog.text
    assert "gold/teste" in capsys.readouterr().out


def test_manutencao_sem_argumento_passa_pelo_registro(spark, data_root, caplog):
    with caplog.at_level("WARNING"):
        assert manutencao(spark) == []
    assert "silver/partidas" in caplog.text


def test_relatorio_mostra_antes_e_depois(spark, tabela):
    texto = relatorio([manter_tabela(spark, tabela)])
    assert "3 → 1" in texto
    assert "gold/teste" in texto


class TestCompararVersoes:
    def test_sem_chave(self, spark, tabela):
        comparacao = comparar_versoes(spark, tabela, 0, 2)
        assert (comparacao.linhas_a, comparacao.linhas_b) == (1, 3)
        assert comparacao.inseridas.count() == 2
        assert comparacao.removidas.count() == 0
        assert comparacao.alteradas is None
        assert "inseridas: 2" in comparacao.resumo()
        assert "alteradas" not in comparacao.resumo()

    def test_com_chave_separa_alterada_de_inserida_e_removida(self, spark, data_root):
        caminho = config.caminho_tabela("silver", "partidas_teste")
        linhas = [(1, 2019, 1, "a"), (2, 2019, 1, "b"), (3, 2019, 1, "c")]
        spark.createDataFrame(linhas, SCHEMA).write.format("delta").save(caminho)
        nova = [(1, 2019, 1, "a"), (2, 2019, 9, "b"), (4, 2019, 1, "d")]
        spark.createDataFrame(nova, SCHEMA).write.format("delta").mode("overwrite").save(caminho)

        comparacao = comparar_versoes(spark, "silver/partidas_teste", 0, 1, chave=["partida_id"])

        assert [r["partida_id"] for r in comparacao.inseridas.collect()] == [4]
        assert [r["partida_id"] for r in comparacao.removidas.collect()] == [3]
        alterada = comparacao.alteradas.collect()
        assert [(r["partida_id"], r["rodada"]) for r in alterada] == [(2, 9)]
        assert "alteradas: 1" in comparacao.resumo()

    def test_respeita_linhas_repetidas(self, spark, data_root):
        caminho = config.caminho_tabela("gold", "dup")
        linha = [(1, 2019, 1, "a")]
        spark.createDataFrame(linha, SCHEMA).write.format("delta").save(caminho)
        spark.createDataFrame(linha, SCHEMA).write.format("delta").mode("append").save(caminho)
        assert comparar_versoes(spark, "gold/dup", 0, 1).inseridas.count() == 1

    def test_coluna_nova_e_removida(self, spark, data_root):
        caminho = config.caminho_tabela("gold", "esquema")
        spark.createDataFrame([(1, "a")], "id int, velha string").write.format("delta").save(
            caminho
        )
        (
            spark.createDataFrame([(1, "b")], "id int, nova string")
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(caminho)
        )
        comparacao = comparar_versoes(spark, "gold/esquema", 0, 1)
        assert comparacao.colunas_adicionadas == ["nova"]
        assert comparacao.colunas_removidas == ["velha"]
        assert comparacao.inseridas.count() == 0
        resumo = comparacao.resumo()
        assert "colunas adicionadas: nova" in resumo
        assert "colunas removidas: velha" in resumo

    def test_versao_sem_diferenca(self, spark, tabela):
        comparacao = comparar_versoes(spark, tabela, 2, 2)
        assert comparacao.inseridas.count() == comparacao.removidas.count() == 0


class TestCli:
    def test_historico_lista_as_versoes(self, spark, tabela, capsys, monkeypatch):
        monkeypatch.setattr(pipeline, "get_spark", lambda **_: spark)
        pipeline.main(["historico", tabela])
        saida = capsys.readouterr().out
        assert "gold/teste: 3 versão(ões)" in saida
        assert "WRITE" in saida

    def test_historico_compara(self, spark, tabela, capsys, monkeypatch):
        monkeypatch.setattr(pipeline, "get_spark", lambda **_: spark)
        pipeline.main(["historico", tabela, "--comparar", "0", "1"])
        assert "inseridas: 1" in capsys.readouterr().out

    def test_manutencao(self, spark, tabela, capsys, monkeypatch):
        monkeypatch.setattr(pipeline, "get_spark", lambda **_: spark)
        monkeypatch.setattr(
            "brasileirao.lake.manutencao.CONSTRAINTS", {tabela: {"qualquer": "true"}}
        )
        pipeline.main(["manutencao"])
        assert "gold/teste" in capsys.readouterr().out

    @pytest.mark.parametrize("nome", ["partidas", "/partidas", "silver/", "prata/partidas"])
    def test_nome_invalido_sai_com_mensagem(self, spark, data_root, monkeypatch, nome):
        monkeypatch.setattr(pipeline, "get_spark", lambda **_: spark)
        with pytest.raises(SystemExit, match="inválida"):
            pipeline.main(["historico", nome])

    def test_tabela_que_nao_existe_sai_com_mensagem(self, spark, data_root, monkeypatch):
        monkeypatch.setattr(pipeline, "get_spark", lambda **_: spark)
        with pytest.raises(SystemExit, match="não é uma tabela Delta"):
            pipeline.main(["historico", "gold/nao_existe"])

    def test_imprimir_historico_sem_metricas(self, spark, tabela, capsys):
        DeltaTable.forPath(spark, caminho_de(tabela)).vacuum()
        imprimir_historico(spark, tabela)
        assert "VACUUM END" in capsys.readouterr().out


def test_nome_invalido_na_camada():
    with pytest.raises(TabelaInvalidaError):
        caminho_de("gold")
