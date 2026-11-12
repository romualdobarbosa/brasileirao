"""`ops/execucoes` gravada de verdade pela CLI do pipeline e da simulação, em Delta.

As etapas pesadas são dublês: aqui importa o registro, não o que a etapa calcula.
"""

import pytest

from brasileirao import execucoes, pipeline, simulacao
from brasileirao.aggregate import gold
from brasileirao.lake import constraints


def _registros(spark):
    return execucoes.ultimas(spark, 100)


def test_sucesso_grava_linhas_por_tabela(spark, data_root, monkeypatch):
    monkeypatch.setattr(gold, "processar_gold", lambda *_: {"campanhas": 4, "goleadas": 2})
    monkeypatch.setattr(constraints, "aplicar_camada", lambda *_: [])

    pipeline.main(["gold"])

    [registro] = _registros(spark)
    assert registro["status"] == "sucesso"
    assert registro["comando"] == "pipeline gold"
    assert registro["alvo"] == "local"
    assert registro["linhas"] == {"gold/campanhas": 4, "gold/goleadas": 2}
    assert registro["fim"] >= registro["inicio"]
    assert (data_root / "ops" / "execucoes" / "_delta_log").is_dir()


def test_falha_grava_com_o_erro(spark, data_root):
    with pytest.raises(SystemExit) as saida:
        pipeline.main(["gold"])

    assert "make lakehouse" in str(saida.value.code)
    [registro] = _registros(spark)
    assert registro["status"] == "falha"
    assert registro["linhas"] == {}
    assert any("make lakehouse" in aviso for aviso in registro["avisos"])


def test_falha_ao_gravar_nao_derruba_o_pipeline(spark, data_root, monkeypatch, caplog):
    monkeypatch.setattr(gold, "processar_gold", lambda *_: {"campanhas": 4})
    monkeypatch.setattr(constraints, "aplicar_camada", lambda *_: [])
    # Uma tabela Delta com outro schema no lugar: o append do registro é recusado.
    spark.range(1).write.format("delta").save(execucoes.caminho())

    pipeline.main(["gold"])

    assert "não registrada" in caplog.text


def test_simulacao_grava_no_lake_dela_inclusive_em_falha(spark, tmp_path, monkeypatch, capsys):
    destino = tmp_path / "data_sim"
    monkeypatch.setattr("brasileirao.utils.spark.get_spark", lambda **_: spark)

    def reprova(*_):
        raise gold.qualidade.QualidadeGoldError(["viradas: placar reconstruído ≠ placar"])

    monkeypatch.setattr(simulacao, "processar_gold", reprova)
    with pytest.raises(gold.qualidade.QualidadeGoldError):
        simulacao.main(["--destino", str(destino), "gold", "5"])
    monkeypatch.setattr(simulacao, "processar_gold", lambda *_: None)
    simulacao.main(["--destino", str(destino), "gold", "5"])

    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(destino))
    novo, velho = _registros(spark)
    assert (velho["status"], novo["status"]) == ("falha", "sucesso")
    assert velho["comando"] == novo["comando"] == f"simulacao --destino {destino} gold 5"
    assert any("viradas" in aviso for aviso in velho["avisos"])

    print(execucoes.formatar(_registros(spark)))
    texto = capsys.readouterr().out
    assert f"últimas 2 execução(ões) em {destino}/ops/execucoes" in texto
    assert "falha" in texto and "sucesso" in texto
