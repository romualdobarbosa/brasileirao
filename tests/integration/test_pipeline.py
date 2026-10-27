import pytest
from chispa import assert_df_equality

from brasileirao import config, pipeline

METADADOS_INGESTAO = ["_ingested_at", "_source_file"]
TABELAS = ("partidas", "gols", "cartoes", "estatisticas")


def _ler(spark, camada, tabela):
    return spark.read.format("delta").load(config.caminho_tabela(camada, tabela))


def _estado(spark):
    """Conteúdo das tabelas sem os metadados de ingestão, que mudam a cada execução."""
    estado = {}
    for tabela in TABELAS:
        estado[f"bronze/{tabela}"] = _ler(spark, "bronze", tabela).drop(*METADADOS_INGESTAO)
        estado[f"silver/{tabela}"] = _ler(spark, "silver", tabela)
    return estado


def test_rodar_duas_vezes_e_igual_a_uma(spark, raw):
    pipeline.lakehouse(spark, 2019)
    uma_vez = {nome: df.localCheckpoint() for nome, df in _estado(spark).items()}

    pipeline.lakehouse(spark, 2019)

    for nome, df in _estado(spark).items():
        assert_df_equality(df, uma_vez[nome], ignore_row_order=True)


def test_todas_em_ordem_e_sem_a_temporada_em_andamento(spark, raw, caplog):
    assert pipeline.lakehouse(spark, None) == [2003, 2019, 2020, 2021]

    temporadas = {r["temporada"] for r in _ler(spark, "silver", "partidas").collect()}
    assert temporadas == {2003, 2019, 2020, 2021}
    for tabela in TABELAS:
        assert _ler(spark, "bronze", tabela).where("temporada = 2025").isEmpty()
    contagens = {
        tabela: {
            r["temporada"]: r["count"]
            for r in _ler(spark, "silver", tabela).groupBy("temporada").count().collect()
        }
        for tabela in ("gols", "cartoes", "estatisticas")
    }
    assert contagens == {
        "gols": {2019: 6, 2020: 2, 2021: 1},
        "cartoes": {2019: 4, 2020: 1, 2021: 1},
        "estatisticas": {2019: 5, 2020: 2},
    }
    orfas = [r.getMessage() for r in caplog.records if "partida_id inexistente" in r.getMessage()]
    assert orfas == [
        "raw/gols: 1 linha(s) com partida_id inexistente em partidas, fora de bronze e silver: 999",
        "raw/cartoes: 2 linha(s) com partida_id inexistente em partidas, fora de bronze e silver: "
        "998, 999",
        "raw/estatisticas: 1 linha(s) com partida_id inexistente em partidas, fora de bronze e "
        "silver: 999",
    ]


def test_cli_todas(spark, monkeypatch):
    # A carga de ponta a ponta de `--todas` é o teste acima; aqui só o despacho da CLI.
    chamadas = []
    monkeypatch.setattr(pipeline, "lakehouse", lambda sessao, t: chamadas.append((sessao, t)))

    pipeline.main(["lakehouse", "--todas"])

    assert chamadas == [(spark, None)]


def test_cli_recusa_temporada_em_andamento(spark, raw):
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["lakehouse", "--temporada", "2025"])

    assert "2025 está em andamento (1 de 380 partidas)" in str(saida.value.code)
    assert not (raw.parent / "bronze").exists()


def test_cli_exige_temporada_ou_todas():
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["lakehouse"])

    assert saida.value.code == 2
