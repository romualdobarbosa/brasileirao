from brasileirao.aggregate.disciplina import calcular_disciplina
from brasileirao.aggregate.qualidade import avisos_disciplina

from .conftest import CARTOES

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("jogos", "int"),
    ("amarelos", "int"),
    ("vermelhos", "int"),
    ("amarelos_por_jogo", "double"),
    ("vermelhos_por_jogo", "double"),
    ("fonte", "string"),
]


def _por_clube(df, temporada):
    return {r["clube"]: tuple(r)[2:] for r in df.where(f"temporada = {temporada}").collect()}


def test_temporada_com_cartoes_usa_cartoes(silver_partidas, silver_cartoes, silver_estatisticas):
    df = calcular_disciplina(silver_partidas(), silver_cartoes(), silver_estatisticas())

    assert df.dtypes == COLUNAS
    # 2019 também tem estatísticas, mas os cartões têm prioridade. O Delta não levou cartão.
    assert _por_clube(df, 2019) == {
        "Atlas": (6, 1, 0, 0.17, 0.0, "cartoes"),
        "Bravo": (6, 1, 1, 0.17, 0.17, "cartoes"),
        "Cometa": (6, 2, 1, 0.33, 0.17, "cartoes"),
        "Delta": (6, 0, 0, 0.0, 0.0, "cartoes"),
    }
    assert avisos_disciplina(df) == []


def test_temporada_sem_cartoes_usa_estatisticas(
    silver_partidas, silver_cartoes, silver_estatisticas
):
    partidas = silver_partidas().unionByName(silver_partidas(temporada=2020))
    estatisticas = silver_estatisticas().unionByName(silver_estatisticas(temporada=2020))

    df = calcular_disciplina(partidas, silver_cartoes(), estatisticas)

    assert _por_clube(df, 2020) == {
        "Atlas": (6, 2, 0, 0.33, 0.0, "estatisticas"),
        "Bravo": (6, 3, 1, 0.5, 0.17, "estatisticas"),
        "Cometa": (6, 1, 0, 0.17, 0.0, "estatisticas"),
        "Delta": (6, 0, 0, 0.0, 0.0, "estatisticas"),
    }


def test_temporada_sem_cartoes_nem_estatisticas_nao_aparece(
    silver_partidas, silver_cartoes, silver_estatisticas
):
    # A silver de estatísticas não tem linha antes de 2014: a fonte vem zerada e é descartada.
    partidas = silver_partidas().unionByName(silver_partidas(temporada=2013))

    df = calcular_disciplina(partidas, silver_cartoes(), silver_estatisticas())

    assert [r["temporada"] for r in df.select("temporada").distinct().collect()] == [2019]


def test_aviso_de_clube_sem_partida(silver_partidas, silver_cartoes, silver_estatisticas):
    cartoes = silver_cartoes([*CARTOES, (1, "Zeta", "Amarelo", "Zé")])

    df = calcular_disciplina(silver_partidas(), cartoes, silver_estatisticas())

    zeta = df.where("clube = 'Zeta'").first()
    assert (zeta["jogos"], zeta["amarelos"], zeta["amarelos_por_jogo"]) == (0, 1, None)
    assert avisos_disciplina(df) == ["disciplina: clube sem partida na temporada: 2019 Zeta"]
