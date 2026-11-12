import csv
from pathlib import Path

import pytest

from brasileirao import simulacao
from brasileirao.ingestion.bronze import ARQUIVOS

FIXTURES_RAW_GOLD = Path(__file__).parent.parent / "fixtures" / "raw_gold"
FONTE_REAL = Path(__file__).parent.parent.parent / "data" / "raw"
EVENTOS = """tipo,partida_id,rodada_publicacao,placar_mandante,placar_visitante,descricao
adiado,3,4,,,"rodada 2, jogada depois da 4"
placar_corrigido,1,4,1,0,"publicado 1 x 0 sem o gol dos 45+2, real 2 x 0"
"""


def _linhas(caminho: Path) -> list[dict]:
    return list(csv.DictReader(caminho.open(encoding="utf-8")))


@pytest.mark.parametrize(
    "destino", ["data", "./data", "data/sub", ".", "app/dados", "s3a://bucket/lake"]
)
def test_recusa_destino_do_lake_normal(destino):
    with pytest.raises(simulacao.SimulacaoError):
        simulacao.validar_destino(destino)


def test_aceita_destino_isolado(tmp_path):
    assert simulacao.validar_destino(str(tmp_path / "data_sim")) == tmp_path / "data_sim"
    assert simulacao.validar_destino("data_sim") == Path("data_sim").resolve()


def test_eventos_do_pacote_sao_validos():
    eventos = simulacao.carregar_eventos()

    assert {e.tipo for e in eventos} == set(simulacao.TIPOS_EVENTO)
    assert len({e.partida_id for e in eventos}) == len(eventos)


def test_eventos_do_pacote_batem_com_a_fonte():
    """Todo evento é de partida de 2025 e publica depois da rodada dela. Precisa do
    `data/raw` (`make download`); no CI, sem ele, o teste é pulado."""
    caminho = FONTE_REAL / ARQUIVOS["partidas"]
    if not caminho.exists():
        pytest.skip("data/raw ausente")
    fonte = {r["ID"]: r for r in _linhas(caminho)}

    for evento in simulacao.carregar_eventos():
        partida = fonte[evento.partida_id]
        assert simulacao.temporada_da_data(partida["data"]) == 2025
        assert evento.rodada_publicacao > int(partida["rodata"])


def test_evento_de_tipo_desconhecido():
    with pytest.raises(simulacao.SimulacaoError, match="desconhecido"):
        simulacao.carregar_eventos(
            "tipo,partida_id,rodada_publicacao,placar_mandante,"
            "placar_visitante,descricao\nsumiu,1,2,,,x\n"
        )


def test_campos_crus_preserva_aspas_e_virgula():
    linha = '"1","3","12/04/2025","","Nabizão, Bragança",'

    assert simulacao.campos_crus(linha) == [
        '"1"',
        '"3"',
        '"12/04/2025"',
        '""',
        '"Nabizão, Bragança"',
        "",
    ]


def test_temporada_da_data():
    assert simulacao.temporada_da_data("15/02/2021") == 2020
    assert simulacao.temporada_da_data("15/05/2021") == 2021


@pytest.mark.parametrize(
    ("rodada", "ids", "pendentes", "provisorios"),
    [
        (0, [], [], []),
        (1, ["1", "2"], [], ["1"]),
        (3, ["1", "2", "4", "5", "6"], ["3"], ["1"]),
        (4, ["1", "2", "3", "4", "5", "6", "7", "8"], [], []),
    ],
)
def test_retrato_da_rodada(tmp_path, rodada, ids, pendentes, provisorios):
    eventos = simulacao.carregar_eventos(EVENTOS)

    retrato = simulacao.gerar_retrato(FIXTURES_RAW_GOLD, tmp_path, 2019, rodada, eventos)

    partidas = _linhas(tmp_path / ARQUIVOS["partidas"])
    de_2019 = [p["ID"] for p in partidas if p["data"].endswith("2019")]
    assert de_2019 == ids
    assert [p["ID"] for p in partidas if p["data"].endswith("2020")] == ["13"]
    assert retrato.partidas == len(ids)
    assert retrato.adiadas_pendentes == pendentes
    assert retrato.placares_provisorios == provisorios
    gols = {g["partida_id"] for g in _linhas(tmp_path / ARQUIVOS["gols"])}
    assert gols <= set(ids) | {"13"}


def test_placar_provisorio_e_correcao(tmp_path):
    eventos = simulacao.carregar_eventos(EVENTOS)
    original = (FIXTURES_RAW_GOLD / ARQUIVOS["partidas"]).read_text(encoding="utf-8")

    simulacao.gerar_retrato(FIXTURES_RAW_GOLD, tmp_path / "r3", 2019, 3, eventos)
    simulacao.gerar_retrato(FIXTURES_RAW_GOLD, tmp_path / "r6", 2019, 6, eventos)

    provisoria = _linhas(tmp_path / "r3" / ARQUIVOS["partidas"])[0]
    assert (provisoria["mandante_Placar"], provisoria["visitante_Placar"]) == ("1", "0")
    assert provisoria["vencedor"] == "Flamengo"
    gols = [
        g["minuto"] for g in _linhas(tmp_path / "r3" / ARQUIVOS["gols"]) if g["partida_id"] == "1"
    ]
    assert gols == ["10"]
    # Na rodada 6 tudo já foi publicado: o retrato é a fonte, byte a byte.
    assert (tmp_path / "r6" / ARQUIVOS["partidas"]).read_text(encoding="utf-8") == original
    for entidade in ("gols", "cartoes", "estatisticas"):
        assert (tmp_path / "r6" / ARQUIVOS[entidade]).read_bytes() == (
            FIXTURES_RAW_GOLD / ARQUIVOS[entidade]
        ).read_bytes()


def test_placar_provisorio_nao_inventa_gol(tmp_path):
    eventos = simulacao.carregar_eventos(EVENTOS.replace("4,1,0,", "4,1,1,"))

    with pytest.raises(simulacao.SimulacaoError, match="mais gols que a fonte nas partidas 1"):
        simulacao.gerar_retrato(FIXTURES_RAW_GOLD, tmp_path, 2019, 3, eventos)


def test_vencedor_do_placar_provisorio():
    campos = simulacao.campos_crus(
        '"1","1","01/05/2019","16:00","Flamengo","Palmeiras","","","","","Flamengo","X","2","0"'
    )
    evento = simulacao.Evento("placar_corrigido", "1", 2, "0", "1", "")

    assert simulacao.campos_crus(simulacao._com_placar(campos, evento))[10:14] == [
        '"Palmeiras"',
        '"X"',
        '"0"',
        '"1"',
    ]
