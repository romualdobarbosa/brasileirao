"""Constraints num lake só deste módulo: as escritas inválidas e a troca de expressão mexem na
tabela, então o `lake_compartilhado` não serve."""

import json
from datetime import date
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from brasileirao import config, pipeline
from brasileirao.aggregate import gold
from brasileirao.lake import constraints as modulo
from brasileirao.lake.constraints import (
    CONSTRAINTS,
    ConstraintRecusadaError,
    aplicar_camada,
    aplicar_constraints,
    constraints_da_tabela,
)
from brasileirao.lake.manutencao import ZORDER, manter_tabela
from brasileirao.transform import silver as modulo_silver

CONSTRAINT_VIOLADA = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"


# O minicampeonato das fixtures não tem goleada nem formação: estas tabelas nascem vazias.
LINHAS_VALIDAS = {
    "gold/goleadas": {
        "partida_id": 1,
        "temporada": 2019,
        "rodada": 1,
        "data": date(2019, 5, 1),
        "mandante": "Santos",
        "visitante": "Grêmio",
        "placar_mandante": 5,
        "placar_visitante": 0,
        "diferenca": 5,
        "total_gols": 5,
        "arena": "Vila Belmiro",
    },
    "gold/esquemas_taticos": {
        "temporada": 2019,
        "esquema": "4-3-3",
        "jogos": 2,
        "uso_pct": 1.0,
        "vitorias": 1,
        "empates": 1,
        "derrotas": 0,
        "aproveitamento": 0.6667,
    },
}


def _ler(spark, tabela):
    camada, _, nome = tabela.partition("/")
    return spark.read.format("delta").load(config.caminho_tabela(camada, nome))


def _ordenadas(df):
    return sorted(map(repr, df.collect()))


def _caminho(tabela):
    camada, _, nome = tabela.partition("/")
    return config.caminho_tabela(camada, nome)


def _sem_espacos(regras):
    return {nome: "".join(expressao.split()) for nome, expressao in regras.items()}


def _constraints_gravadas(spark, tabela):
    return _sem_espacos(constraints_da_tabela(spark, _caminho(tabela)))


def _versao(spark, tabela):
    return DeltaTable.forPath(spark, _caminho(tabela)).history(1).first()["version"]


def _uma_linha(spark, tabela, **mudancas):
    """Uma linha válida da tabela com `mudancas` aplicadas."""
    atual = _ler(spark, tabela)
    df = atual.limit(1).localCheckpoint()
    if df.isEmpty():
        df = spark.createDataFrame([LINHAS_VALIDAS[tabela]], atual.schema)
    for coluna, valor in mudancas.items():
        df = df.withColumn(coluna, F.lit(valor).cast(df.schema[coluna].dataType))
    return df


def _sobrescrever(df, tabela):
    df.write.format("delta").mode("overwrite").save(_caminho(tabela))


def _acrescentar(df, tabela):
    df.write.format("delta").mode("append").save(_caminho(tabela))


def _recusada(spark, tabela, nome_constraint, escrita):
    """A escrita falha pela constraint e não muda nem o conteúdo nem a versão da tabela."""
    conteudo = _ordenadas(_ler(spark, tabela))
    versao = _versao(spark, tabela)
    with pytest.raises((Py4JJavaError, AnalysisException)) as erro:
        escrita()
    assert CONSTRAINT_VIOLADA in str(erro.value)
    assert nome_constraint in str(erro.value)
    assert _ordenadas(_ler(spark, tabela)) == conteudo
    assert _versao(spark, tabela) == versao


@pytest.mark.parametrize("tabela", list(CONSTRAINTS))
def test_cada_tabela_do_pipeline_tem_as_constraints(spark, lake_do_modulo, tabela):
    assert _constraints_gravadas(spark, tabela) == _sem_espacos(CONSTRAINTS[tabela])


def test_o_registro_cobre_toda_silver_e_gold():
    esperadas = {f"silver/{t}" for t in modulo_silver.TABELAS} | {f"gold/{t}" for t in gold.TABELAS}
    assert set(CONSTRAINTS) == esperadas


@pytest.mark.parametrize(
    ("tabela", "constraint", "mudanca"),
    [
        ("silver/partidas", "placar_mandante_valido", {"placar_mandante": -1}),
        ("silver/partidas", "placar_visitante_valido", {"placar_visitante": None}),
        ("silver/partidas", "rodada_valida", {"rodada": 47}),
        ("silver/partidas", "temporada_valida", {"temporada": 2002}),
        ("silver/partidas", "partida_id_nao_nulo", {"partida_id": None}),
        ("silver/gols", "minuto_valido", {"minuto": -5}),
        ("silver/cartoes", "cartao_valido", {"cartao": "Azul"}),
        ("silver/cartoes", "cartao_valido", {"cartao": None}),
        ("silver/estatisticas", "contagens_nao_negativas", {"chutes": -1}),
        ("silver/estatisticas", "posse_de_bola_valida", {"posse_de_bola": 55.0}),
        ("gold/classificacao_por_rodada", "jogos_fecham", {"jogos": 999}),
        ("gold/goleadas", "criterio_de_goleada", {"diferenca": 1, "total_gols": 1}),
        ("gold/linha_corte", "topo_em_ordem", {"pontos_vice": 1000}),
        ("gold/viradas", "venceu_a_partida", {"gols_pro": 0}),
        ("gold/esquemas_taticos", "temporada_valida", {"temporada": 2014}),
    ],
)
def test_linha_invalida_e_recusada(spark, lake_do_modulo, tabela, constraint, mudanca):
    invalida = _uma_linha(spark, tabela, **mudanca)
    _recusada(spark, tabela, constraint, lambda: _acrescentar(invalida, tabela))


def test_coluna_opcional_aceita_nulo(spark, lake_do_modulo):
    """`minuto` é opcional na silver: o `IS NULL OR` da regra é o que deixa passar."""
    tabela = "silver/gols"
    original = _ordenadas(_ler(spark, tabela))
    nula = _uma_linha(spark, tabela, minuto=None)
    _acrescentar(nula, tabela)
    alvo = DeltaTable.forPath(spark, _caminho(tabela))
    assert _ler(spark, tabela).where(F.col("minuto").isNull()).count() >= 1
    alvo.restoreToVersion(_versao(spark, tabela) - 1)
    assert _ordenadas(_ler(spark, tabela)) == original


class TestEscritasDeOutrasOrigens:
    """A mesma regra, por todo caminho de escrita. O `append` está no teste acima."""

    def test_overwrite_do_gold(self, spark, lake_do_modulo):
        invalida = _uma_linha(spark, "gold/goleadas", diferenca=0, total_gols=0)
        _recusada(
            spark,
            "gold/goleadas",
            "criterio_de_goleada",
            lambda: gold.gravar(invalida, "goleadas"),
        )

    def test_replace_where_da_silver(self, spark, lake_do_modulo):
        invalida = _uma_linha(spark, "silver/cartoes", cartao="Azul").withColumn(
            "temporada", F.lit(2019)
        )
        _recusada(
            spark,
            "silver/cartoes",
            "cartao_valido",
            lambda: modulo_silver.gravar_particao(invalida, "cartoes", 2019),
        )

    def test_merge_que_atualiza(self, spark, lake_do_modulo):
        invalida = _uma_linha(spark, "silver/partidas", placar_mandante=-3)
        _recusada(
            spark,
            "silver/partidas",
            "placar_mandante_valido",
            lambda: modulo_silver.gravar_merge(spark, invalida, "partidas", "partida_id", 2019),
        )

    def test_merge_que_insere(self, spark, lake_do_modulo):
        invalida = _uma_linha(spark, "silver/partidas", partida_id=999_999, placar_mandante=-3)
        _recusada(
            spark,
            "silver/partidas",
            "placar_mandante_valido",
            lambda: modulo_silver.gravar_merge(spark, invalida, "partidas", "partida_id", 2019),
        )

    def test_update_direto(self, spark, lake_do_modulo):
        alvo = DeltaTable.forPath(spark, _caminho("silver/partidas"))
        _recusada(
            spark,
            "silver/partidas",
            "rodada_valida",
            lambda: alvo.update(F.lit(True), {"rodada": F.lit(0)}),
        )

    def test_insert_em_sql(self, spark, lake_do_modulo):
        origem = _uma_linha(spark, "gold/artilharia", gols=0)
        origem.createOrReplaceTempView("origem_invalida")
        caminho = modulo._referencia_sql(_caminho("gold/artilharia"))
        _recusada(
            spark,
            "gold/artilharia",
            "gols_positivos",
            lambda: spark.sql(f"INSERT INTO {caminho} SELECT * FROM origem_invalida"),
        )

    def test_overwrite_com_troca_de_schema_ainda_recusa(self, spark, lake_do_modulo):
        """`overwriteSchema` não leva as constraints embora. O pipeline não usa essa opção."""
        tabela = "gold/gols_por_minuto"
        invalida = _uma_linha(spark, tabela, gols=-1)
        _recusada(
            spark,
            tabela,
            "gols_nao_negativos",
            lambda: (
                invalida.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(_caminho(tabela))
            ),
        )


def test_overwrite_valido_mantem_as_constraints(spark, lake_do_modulo):
    tabela = "gold/resumo_temporada"
    antes = constraints_da_tabela(spark, _caminho(tabela))
    gold.gravar(_ler(spark, tabela).localCheckpoint(), "resumo_temporada")
    assert constraints_da_tabela(spark, _caminho(tabela)) == antes


def test_pipeline_duas_vezes_nao_falha_nem_duplica(spark, lake_do_modulo):
    tabelas = list(CONSTRAINTS)
    adicoes = {
        t: DeltaTable.forPath(spark, _caminho(t))
        .history()
        .where(F.col("operation") == "ADD CONSTRAINT")
        .count()
        for t in tabelas
    }
    assert adicoes == {t: len(CONSTRAINTS[t]) for t in tabelas}
    versao_partidas = _versao(spark, "silver/partidas")

    assert pipeline.lakehouse(spark, None) == [2019]
    pipeline.executar_gold(spark)

    for tabela in tabelas:
        assert _constraints_gravadas(spark, tabela) == _sem_espacos(CONSTRAINTS[tabela])
        depois = (
            DeltaTable.forPath(spark, _caminho(tabela))
            .history()
            .where(F.col("operation") == "ADD CONSTRAINT")
            .count()
        )
        assert depois == adicoes[tabela]
    # O MERGE com o mesmo dado não cria versão, e a constraint também não pode criar.
    assert _versao(spark, "silver/partidas") == versao_partidas


def test_aplicar_camada_nao_muda_nada_quando_ja_aplicada(spark, lake_do_modulo):
    assert aplicar_camada(spark, "silver") == []
    assert aplicar_camada(spark, "gold") == []


def test_aplicar_camada_diz_o_que_mudou(spark, lake_do_modulo, monkeypatch):
    tabela = "gold/resumo_temporada"
    novas = {**CONSTRAINTS[tabela], "times_validos": "times >= 2"}
    monkeypatch.setitem(CONSTRAINTS, tabela, novas)
    assert aplicar_camada(spark, "gold") == ["gold/resumo_temporada.times_validos"]


@pytest.mark.parametrize("tabela", list(CONSTRAINTS))
def test_aplicar_de_novo_nao_cria_versao(spark, lake_do_modulo, tabela):
    """Inclui as regras com `IN`, cujo texto volta do Delta com outros espaços."""
    camada, _, nome = tabela.partition("/")
    versao = _versao(spark, tabela)
    assert aplicar_constraints(spark, camada, nome) == []
    assert _versao(spark, tabela) == versao


def test_adicionar_constraint_muda_o_protocolo(spark, lake_do_modulo):
    """A tabela nasce pedindo escritor 2; a primeira constraint sobe para 3 e fica."""
    log = Path(_caminho("gold/disciplina")) / "_delta_log" / f"{0:020d}.json"
    acoes = [json.loads(linha) for linha in log.read_text().splitlines()]
    protocolo = next(a["protocol"] for a in acoes if "protocol" in a)
    assert (protocolo["minReaderVersion"], protocolo["minWriterVersion"]) == (1, 2)
    detalhe = DeltaTable.forPath(spark, _caminho("gold/disciplina")).detail().first()
    assert (detalhe["minReaderVersion"], detalhe["minWriterVersion"]) == (1, 3)


@pytest.mark.parametrize("tabela", list(ZORDER))
def test_manutencao_no_gold_mantem_conteudo_e_constraints(spark, lake_do_modulo, tabela):
    assert tabela.startswith("gold/"), "a silver particiona por temporada, sem Z-Order"
    conteudo = _ordenadas(_ler(spark, tabela))
    resultado = manter_tabela(spark, tabela)
    assert resultado.zorder == ZORDER[tabela]
    assert _ordenadas(_ler(spark, tabela)) == conteudo
    assert _constraints_gravadas(spark, tabela) == _sem_espacos(CONSTRAINTS[tabela])
    assert resultado.depois.versao > resultado.antes.versao


def test_constraint_nova_sobre_dado_que_viola_e_recusada(spark, lake_do_modulo, monkeypatch):
    tabela = "gold/faltas_clubes"
    monkeypatch.setitem(CONSTRAINTS, tabela, {**CONSTRAINTS[tabela], "impossivel": "faltas < 0"})
    versao = _versao(spark, tabela)
    with pytest.raises(ConstraintRecusadaError, match="impossivel"):
        aplicar_constraints(spark, "gold", "faltas_clubes")
    assert "impossivel" not in constraints_da_tabela(spark, _caminho(tabela))
    assert _versao(spark, tabela) == versao


def test_expressao_diferente_e_trocada_sem_duplicar(spark, lake_do_modulo, monkeypatch):
    tabela = "gold/gols_decisivos"
    nova = {**CONSTRAINTS[tabela], "temporada_valida": "temporada >= 2010"}
    monkeypatch.setitem(CONSTRAINTS, tabela, nova)
    assert aplicar_constraints(spark, "gold", "gols_decisivos") == ["temporada_valida"]
    assert _constraints_gravadas(spark, tabela) == _sem_espacos(nova)


def test_troca_recusada_deixa_a_expressao_antiga(spark, lake_do_modulo, monkeypatch):
    tabela = "gold/cartoes_jogadores"
    antiga = CONSTRAINTS[tabela]
    nova = {**antiga, "temporada_valida": "temporada > 3000"}
    monkeypatch.setitem(CONSTRAINTS, tabela, nova)
    with pytest.raises(ConstraintRecusadaError, match="fica valendo"):
        aplicar_constraints(spark, "gold", "cartoes_jogadores")
    assert _constraints_gravadas(spark, tabela) == _sem_espacos(antiga)


def test_constraint_fora_do_registro_fica_com_aviso(spark, lake_do_modulo, monkeypatch, caplog):
    tabela = "gold/sequencias"
    restantes = {k: v for k, v in CONSTRAINTS[tabela].items() if k != "jogos_nao_negativos"}
    monkeypatch.setitem(CONSTRAINTS, tabela, restantes)
    with caplog.at_level("WARNING"):
        assert aplicar_constraints(spark, "gold", "sequencias") == []
    assert "jogos_nao_negativos" in caplog.text
    assert "jogos_nao_negativos" in constraints_da_tabela(spark, _caminho(tabela))


def test_outro_erro_do_delta_nao_vira_recusa(spark, lake_do_modulo, monkeypatch):
    tabela = "gold/campanhas"
    monkeypatch.setitem(CONSTRAINTS, tabela, {"coluna_inexistente": "nao_tem > 0"})
    with pytest.raises(AnalysisException):
        aplicar_constraints(spark, "gold", "campanhas")


def test_caminho_relativo_funciona_em_sql(spark, tmp_path, monkeypatch):
    """O padrão é `./data`, e `delta.`./data`` não é reconhecido pelo SQL do Delta."""
    monkeypatch.chdir(tmp_path)
    referencia = modulo._referencia_sql("./data/gold/x")
    assert referencia == f"delta.`{tmp_path}/data/gold/x`"
    assert modulo._referencia_sql("s3://balde/gold/x") == "delta.`s3://balde/gold/x`"
