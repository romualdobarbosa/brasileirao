import pytest

from brasileirao import pipeline
from brasileirao.aggregate import explain


@pytest.fixture(scope="module")
def planos(spark, silver, tmp_path_factory):
    """Os planos gerados uma vez sobre a silver das fixtures, num diretório temporário."""
    destino = tmp_path_factory.mktemp("planos")
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(silver))
        arquivos = explain.gerar_planos(spark, str(destino))
    return destino, {a.name: a.read_text(encoding="utf-8") for a in arquivos}


def test_grava_um_plano_por_tabela(planos):
    destino, textos = planos

    assert (
        sorted(p.name for p in destino.iterdir())
        == sorted(textos)
        == [
            "classificacao_por_rodada.txt",
            "disciplina.txt",
            "sequencias.txt",
        ]
    )
    for texto in textos.values():
        assert "== Physical Plan ==" in texto
        assert "isFinalPlan=true" in texto


def test_plano_sem_caminho_da_maquina(planos, silver):
    _, textos = planos

    for texto in textos.values():
        assert str(silver) not in texto
        assert "$BRASILEIRAO_DATA_ROOT/silver/partidas" in texto


def test_disciplina_tem_broadcast(planos):
    _, textos = planos

    assert "BroadcastHashJoin" in textos["disciplina.txt"]


def test_cli_explain(spark, monkeypatch):
    chamadas = []
    monkeypatch.setattr(explain, "gerar_planos", lambda *args: chamadas.append(args))

    pipeline.main(["explain"])
    pipeline.main(["explain", "--destino", "outro/lugar"])

    assert chamadas == [(spark, explain.DESTINO_PADRAO), (spark, "outro/lugar")]
