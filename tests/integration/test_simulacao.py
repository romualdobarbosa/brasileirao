"""Simulação sobre as fixtures `raw_gold` (minicampeonato de 2019, 6 rodadas).

Um lake só para o módulo, montado rodada a rodada uma vez; os testes leem o resultado.
"""

import hashlib
import json
from pathlib import Path

import pytest
from delta.tables import DeltaTable

from brasileirao import config, pipeline, simulacao
from brasileirao.lake import cdf

FIXTURES_RAW_GOLD = Path(__file__).parent.parent / "fixtures" / "raw_gold"
APP_DADOS = Path(__file__).parent.parent.parent / "app" / "dados"
# Flamengo × Santos (rodada 2) só aparece na rodada 4; Flamengo × Palmeiras (rodada 1, 2 × 0)
# sai 1 × 0 até a correção da rodada 4.
EVENTOS = """tipo,partida_id,rodada_publicacao,placar_mandante,placar_visitante,descricao
adiado,3,4,,,"rodada 2, jogada depois da 4"
placar_corrigido,1,4,1,0,"publicado 1 x 0, real 2 x 0"
"""


def _impressao(pasta: Path) -> str:
    """Nome, tamanho e mtime de tudo na pasta: muda se alguém escrever nela."""
    itens = sorted(
        (str(p.relative_to(pasta)), p.stat().st_size, p.stat().st_mtime_ns)
        for p in pasta.rglob("*")
        if p.is_file()
    )
    return hashlib.sha256(json.dumps(itens).encode()).hexdigest()


def _versao(spark) -> int:
    caminho = config.caminho_tabela("silver", "partidas")
    return DeltaTable.forPath(spark, caminho).history(1).first()["version"]


class Simulado:
    def __init__(self, spark, raiz: Path) -> None:
        self.spark = spark
        self.raiz = raiz
        self.execucoes = {}
        self.versoes = {}


@pytest.fixture(scope="module")
def simulado(spark, tmp_path_factory):
    raiz = tmp_path_factory.mktemp("data_sim")
    eventos = simulacao.carregar_eventos(EVENTOS)
    app_antes = _impressao(APP_DADOS) if APP_DADOS.exists() else None
    with pytest.MonkeyPatch.context() as mp:
        # Como no lake compartilhado: com 4 times, rebaixa 1.
        mp.setenv("BRASILEIRAO_DATA_ROOT", str(raiz))
        for modulo in ("campanhas", "resumo"):
            mp.setattr(f"brasileirao.aggregate.{modulo}.rebaixados_por_temporada", lambda _: 1)
        sim = Simulado(spark, raiz)
        # Só silver, que é onde estão o MERGE e o feed: cada rodada custa um lakehouse, e o gold
        # custaria mais que todas elas. As checagens parciais do gold têm teste unitário, e a
        # ligação com o pipeline, `test_gold_recebe_a_temporada_em_andamento`.
        for rodada in (3, 4, 6):
            sim.execucoes[rodada] = simulacao.executar_rodada(
                spark, rodada, 2019, FIXTURES_RAW_GOLD, eventos, gold=False
            )
            sim.versoes[rodada] = _versao(spark)
        sim.relatorios = {r: simulacao.relatorio(spark, r, 2019) for r in (3, 4)}
        sim.feed_r4 = cdf.mudancas(
            spark, "silver/partidas", *_extremos(simulacao.versoes_da_rodada(spark, 4, 2019))
        ).collect()
        # A mesma rodada de novo: nada muda na silver.
        simulacao.executar_rodada(spark, 6, 2019, FIXTURES_RAW_GOLD, eventos, gold=False)
        sim.versao_repetida = _versao(spark)
        sim.silver = {
            t: spark.read.format("delta").load(config.caminho_tabela("silver", t)).collect()
            for t in ("partidas", "gols")
        }
        sim.checagem = simulacao.checar_rodada(spark, 6, 2019)
        # Retrato novo sem o lakehouse (a tarefa do meio que não rodou): a silver fica para trás.
        simulacao.gravar_retrato(4, 2019, FIXTURES_RAW_GOLD, eventos)
        sim.checagem_atrasada = simulacao.checar_rodada(spark, 6, 2019)
    sim.app_mudou = app_antes is not None and _impressao(APP_DADOS) != app_antes
    return sim


def _extremos(versoes: list[int]) -> tuple[int, int]:
    return versoes[0], versoes[-1]


def test_recusa_o_lake_normal(spark, monkeypatch):
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", "./data")

    with pytest.raises(simulacao.SimulacaoError, match="nunca escreve em ./data"):
        simulacao.executar_rodada(spark, 1, 2019, FIXTURES_RAW_GOLD, [])


def test_cli_recusa_o_lake_normal():
    with pytest.raises(SystemExit, match="nunca escreve em ./data"):
        simulacao.main(["--destino", "data", "rodada", "1"])


def test_app_dados_nao_e_escrito(simulado):
    assert not simulado.app_mudou


def test_retratos_seguem_os_eventos(simulado):
    r3 = simulado.execucoes[3].retrato
    assert (r3.partidas, r3.adiadas_pendentes, r3.placares_provisorios) == (5, ["3"], ["1"])
    r4 = simulado.execucoes[4].retrato
    assert (r4.partidas, r4.adiadas_pendentes, r4.placares_provisorios) == (8, [], [])


def test_gold_recebe_a_temporada_em_andamento(simulado, spark, monkeypatch):
    # A silver da simulação tem 2019 com 12 das 380 partidas: só com a opção ela vira a
    # temporada em andamento do gold; sem a opção, o gold recebe None e cobra a temporada inteira.
    monkeypatch.setenv("BRASILEIRAO_DATA_ROOT", str(simulado.raiz))
    recebidas = []
    monkeypatch.setattr(
        pipeline.gold, "processar_gold", lambda _, em_andamento: recebidas.append(em_andamento)
    )
    monkeypatch.setattr(pipeline.constraints, "aplicar_camada", lambda *_: [])

    pipeline.executar_gold(spark, aceitar_em_andamento=True)
    pipeline.executar_gold(spark)

    assert recebidas == [2019, None]


def test_merge_so_atualiza_a_linha_corrigida(simulado):
    tipos = sorted((r["_change_type"], r["partida_id"]) for r in simulado.feed_r4)

    assert tipos == [
        ("insert", 3),
        ("insert", 7),
        ("insert", 8),
        ("update_postimage", 1),
        ("update_preimage", 1),
    ]
    antes, depois = (
        next(r for r in simulado.feed_r4 if r["_change_type"] == t)
        for t in ("update_preimage", "update_postimage")
    )
    assert (antes["placar_mandante"], depois["placar_mandante"]) == (1, 2)


def test_relatorio_entre_rodadas(simulado):
    r4 = simulado.relatorios[4]

    assert "entraram 3 partida(s)" in r4
    assert "#3 rodada 2 Flamengo 1 x 1 Santos (de rodada anterior, chegou agora)" in r4
    assert "mudaram 1 partida(s)" in r4
    assert "placar_mandante 1 → 2" in r4
    assert "saíram 0 partida(s)" in r4
    assert "bate com o feed" in r4
    # A rodada 3 criou a tabela: tudo entrou, e a conferência é contra a própria versão.
    assert "entraram 5 partida(s)" in simulado.relatorios[3]
    assert "NÃO bate" not in "".join(simulado.relatorios.values())


def test_repetir_a_rodada_nao_cria_versao(simulado):
    assert simulado.versao_repetida == simulado.versoes[6]


def test_ultima_rodada_reproduz_a_silver_normal(simulado, lake_compartilhado, spark):
    normal = lake_compartilhado.silver
    for tabela, linhas in simulado.silver.items():
        esperado = spark.read.format("delta").load(str(normal / "silver" / tabela)).collect()
        assert sorted(map(str, linhas)) == sorted(map(str, esperado)), tabela


def test_checagens_da_rodada(simulado):
    assert simulado.checagem == []
    assert simulado.checagem_atrasada == [
        "silver/partidas tem 12 partida(s) de 2019 e o retrato em raw/ tem 8"
    ]


def test_copiar_para_lake_troca_a_pasta(spark, tmp_path):
    # O mesmo caminho do retrato no alvo aws (`s3a://`), aqui no disco local do Hadoop.
    origem = tmp_path / "origem"
    origem.mkdir()
    (origem / "a.csv").write_text("x\n1\n")
    (origem / "b.csv").write_text("y\n2\n")
    destino = tmp_path / "lake" / "raw"
    destino.mkdir(parents=True)
    (destino / "velho.csv").write_text("z\n")

    simulacao.copiar_para_lake(spark, origem, str(destino))

    assert sorted(p.name for p in destino.iterdir() if not p.name.startswith(".")) == [
        "a.csv",
        "b.csv",
    ]
    assert (destino / "b.csv").read_text() == "y\n2\n"
