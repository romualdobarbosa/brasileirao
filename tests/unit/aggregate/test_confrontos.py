from datetime import date

from brasileirao.aggregate.confrontos import calcular_confrontos, checar_confrontos

COLUNAS = [
    ("clube", "string"),
    ("adversario", "string"),
    ("jogos", "int"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("aproveitamento", "double"),
    ("maior_vitoria", "string"),
    ("maior_derrota", "string"),
    ("data_ultimo_jogo", "date"),
    ("ultimo_jogo", "string"),
]


def _d(ano, mes=5, dia=1):
    return date(ano, mes, dia)


def _linhas(df):
    return {(r["clube"], r["adversario"]): r.asDict() for r in df.collect()}


def _fabricar(silver_partidas, partidas):
    """Cada partida na própria temporada: o ano do texto sai da coluna `temporada`."""
    dfs = [silver_partidas([p], temporada=p[2].year) for p in partidas]
    df = dfs[0]
    for outro in dfs[1:]:
        df = df.unionByName(outro)
    return df


def test_schema_e_duas_linhas_por_par(silver_partidas):
    df = calcular_confrontos(silver_partidas())
    assert df.dtypes == COLUNAS
    # 4 times: 6 pares, 12 linhas; cada par joga 2 vezes.
    assert df.count() == 12
    assert {r["jogos"] for r in df.collect()} == {2}


def test_metricas_na_visao_de_cada_clube(silver_partidas):
    linhas = _linhas(calcular_confrontos(silver_partidas()))
    atlas_bravo = linhas[("Atlas", "Bravo")]
    assert atlas_bravo["vitorias"] == 2
    assert atlas_bravo["empates"] == atlas_bravo["derrotas"] == 0
    assert (atlas_bravo["gols_pro"], atlas_bravo["gols_contra"]) == (4, 0)
    assert atlas_bravo["aproveitamento"] == 1.0
    bravo_atlas = linhas[("Bravo", "Atlas")]
    assert bravo_atlas["derrotas"] == 2
    assert bravo_atlas["aproveitamento"] == 0.0
    # Atlas × Cometa: 1 × 1 e 2 × 2 → 2 pontos em 6.
    assert linhas[("Atlas", "Cometa")]["aproveitamento"] == 0.3333


def test_maior_e_nula_sem_resultado(silver_partidas):
    linhas = _linhas(calcular_confrontos(silver_partidas()))
    assert linhas[("Atlas", "Bravo")]["maior_vitoria"] == "2 × 0 (2019)"
    assert linhas[("Atlas", "Bravo")]["maior_derrota"] is None
    assert linhas[("Bravo", "Atlas")]["maior_vitoria"] is None
    assert linhas[("Bravo", "Atlas")]["maior_derrota"] == "0 × 2 (2019)"
    assert linhas[("Atlas", "Cometa")]["maior_vitoria"] is None


def test_maior_desempata_por_gols_e_depois_pelo_mais_recente(silver_partidas):
    partidas = [
        (1, 1, _d(2010), "Atlas", "Bravo", 3, 0),
        (2, 1, _d(2012), "Bravo", "Atlas", 1, 4),  # mesmo saldo, mais gols: ganha do 3 × 0
        (3, 1, _d(2014), "Atlas", "Bravo", 4, 1),  # mesmo placar, mais recente: é a maior
        (4, 1, _d(2016), "Atlas", "Bravo", 2, 0),  # mais recente, saldo menor: não conta
    ]
    linhas = _linhas(calcular_confrontos(_fabricar(silver_partidas, partidas)))
    assert linhas[("Atlas", "Bravo")]["maior_vitoria"] == "4 × 1 (2014)"
    assert linhas[("Bravo", "Atlas")]["maior_derrota"] == "1 × 4 (2014)"


def test_ultimo_jogo_vem_com_o_mandante_primeiro(silver_partidas):
    partidas = [
        (1, 1, _d(2010), "Atlas", "Bravo", 3, 0),
        (2, 1, _d(2012), "Bravo", "Atlas", 1, 2),
    ]
    linhas = _linhas(calcular_confrontos(_fabricar(silver_partidas, partidas)))
    for par in [("Atlas", "Bravo"), ("Bravo", "Atlas")]:
        assert linhas[par]["ultimo_jogo"] == "Bravo 1 × 2 Atlas"
        assert linhas[par]["data_ultimo_jogo"] == _d(2012)


def test_ultimo_jogo_segue_a_data_e_nao_a_rodada(silver_partidas):
    # Atlas × Cometa da rodada 2 é em 25/05; Cometa × Atlas da rodada 5 é em 01/06.
    linhas = _linhas(calcular_confrontos(silver_partidas()))
    assert linhas[("Atlas", "Cometa")]["ultimo_jogo"] == "Cometa 2 × 2 Atlas"
    # Atlas × Delta: a rodada 6 é a mais recente, com o Delta mandante.
    assert linhas[("Atlas", "Delta")]["data_ultimo_jogo"] == date(2019, 6, 8)


def test_checar_passa_com_a_tabela_certa(silver_partidas):
    partidas = silver_partidas()
    assert checar_confrontos(calcular_confrontos(partidas), partidas) == []


def test_checar_acusa_soma_de_jogos(silver_partidas):
    partidas = silver_partidas()
    df = calcular_confrontos(partidas).where("clube != 'Atlas' or adversario != 'Bravo'")
    problemas = checar_confrontos(df, partidas)
    assert any("2 × partidas" in p for p in problemas)
    assert any("Atlas × Bravo" in p for p in problemas)
