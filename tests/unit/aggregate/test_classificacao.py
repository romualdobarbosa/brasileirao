from brasileirao.aggregate.classificacao import calcular_classificacao

from .conftest import MINICAMPEONATO

COLUNAS = [
    ("temporada", "int"),
    ("rodada", "int"),
    ("clube", "string"),
    ("posicao", "int"),
    ("pontos", "int"),
    ("ajuste_pontos", "int"),
    ("jogos", "int"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("saldo", "int"),
    ("aproveitamento", "double"),
    ("ultimos_5", "string"),
]
CAMPOS = [c for c, _ in COLUNAS[3:]]


def _por_rodada(df):
    """{rodada: [(clube, posicao, pontos, ...), ...] em ordem de posição}."""
    tabelas = {}
    for r in df.orderBy("rodada", "posicao").collect():
        tabelas.setdefault(r["rodada"], []).append((r["clube"], *(r[c] for c in CAMPOS)))
    return tabelas


def test_schema_e_grade_completa(silver_partidas, ajustes):
    df = calcular_classificacao(silver_partidas(), ajustes)

    assert df.dtypes == COLUNAS
    assert df.count() == 24
    for posicoes in df.groupBy("rodada").agg({"posicao": "collect_set"}).collect():
        assert sorted(posicoes[1]) == [1, 2, 3, 4]


def test_tabela_final_com_ajuste(silver_partidas, ajustes):
    final = _por_rodada(calcular_classificacao(silver_partidas(), ajustes))[6]

    # Bravo e Cometa empatam em pontos: vitórias decidem, mesmo com saldo pior.
    # O ajuste do Atlas tira o time da ponta, mas não entra no aproveitamento.
    assert final == [
        ("Bravo", 1, 7, 0, 6, 2, 1, 3, 2, 6, -4, 0.3889, "EVDVD"),
        ("Cometa", 2, 7, 0, 6, 1, 4, 1, 6, 5, 1, 0.3889, "EDEEV"),
        ("Atlas", 3, 6, -8, 6, 4, 2, 0, 11, 4, 7, 0.7778, "EVVEV"),
        ("Delta", 4, 3, 0, 6, 0, 3, 3, 2, 6, -4, 0.1667, "EDEDD"),
    ]


def test_ajuste_so_na_ultima_rodada(silver_partidas, ajustes):
    df = calcular_classificacao(silver_partidas(), ajustes)

    ajustados = {(r["rodada"], r["clube"]) for r in df.where("ajuste_pontos != 0").collect()}
    assert ajustados == {(6, "Atlas")}
    rodada_5 = _por_rodada(df)[5]
    assert rodada_5[0][:3] == ("Atlas", 1, 11)


def test_partida_adiada_entra_na_rodada_dela(silver_partidas, sem_ajustes):
    # Atlas × Cometa da rodada 2 foi jogada depois da rodada 4.
    tabelas = _por_rodada(calcular_classificacao(silver_partidas(), sem_ajustes))

    atlas_r2 = next(t for t in tabelas[2] if t[0] == "Atlas")
    assert atlas_r2 == ("Atlas", 1, 4, 0, 2, 1, 1, 0, 3, 1, 2, 0.6667, "VE")


def test_desempate_por_saldo(silver_partidas, sem_ajustes):
    tabelas = _por_rodada(calcular_classificacao(silver_partidas(), sem_ajustes))

    # Cometa e Delta com 2 pontos e nenhuma vitória: saldo −1 contra −2.
    assert [t[:3] + (t[5], t[10]) for t in tabelas[3]] == [
        ("Atlas", 1, 7, 2, 4),
        ("Bravo", 2, 4, 1, -1),
        ("Cometa", 3, 2, 0, -1),
        ("Delta", 4, 2, 0, -2),
    ]


def test_ultimos_5_na_primeira_rodada(silver_partidas, sem_ajustes):
    primeira = _por_rodada(calcular_classificacao(silver_partidas(), sem_ajustes))[1]

    assert {t[0]: t[-1] for t in primeira} == {
        "Atlas": "V",
        "Bravo": "D",
        "Cometa": "E",
        "Delta": "E",
    }


def test_rodada_sem_jogo_repete_o_acumulado(silver_partidas, sem_ajustes):
    # Sem Atlas × Bravo da rodada 1 (partida não jogada), os dois ficam sem jogo na rodada 1.
    partidas = silver_partidas([p for p in MINICAMPEONATO if p[0] != 1])

    df = calcular_classificacao(partidas, sem_ajustes)

    assert df.count() == 24
    linhas = {(r["rodada"], r["clube"]): r for r in df.collect()}
    for clube in ("Atlas", "Bravo"):
        r1 = linhas[(1, clube)]
        assert (r1["jogos"], r1["pontos"], r1["aproveitamento"], r1["ultimos_5"]) == (
            0,
            0,
            None,
            None,
        )
    assert linhas[(2, "Atlas")]["ultimos_5"] == "E"
    assert linhas[(6, "Atlas")]["jogos"] == 5
    assert linhas[(6, "Bravo")]["ultimos_5"] == "EVDVD"
