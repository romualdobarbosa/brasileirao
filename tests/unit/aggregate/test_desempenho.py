from brasileirao.aggregate.classificacao import calcular_classificacao
from brasileirao.aggregate.desempenho import calcular_desempenho
from brasileirao.aggregate.qualidade import ultima_rodada

COLUNAS = [
    ("temporada", "int"),
    ("clube", "string"),
    ("mando", "string"),
    ("jogos", "int"),
    ("vitorias", "int"),
    ("empates", "int"),
    ("derrotas", "int"),
    ("gols_pro", "int"),
    ("gols_contra", "int"),
    ("pontos", "int"),
    ("aproveitamento", "double"),
]
SOMADAS = ["jogos", "vitorias", "empates", "derrotas", "gols_pro", "gols_contra", "pontos"]


def test_schema_e_duas_linhas_por_clube(silver_partidas):
    df = calcular_desempenho(silver_partidas())

    assert df.dtypes == COLUNAS
    linhas = sorted((r["clube"], r["mando"]) for r in df.collect())
    assert linhas == [
        (clube, mando)
        for clube in ("Atlas", "Bravo", "Cometa", "Delta")
        for mando in ("mandante", "visitante")
    ]


def test_valores_do_atlas(silver_partidas):
    df = calcular_desempenho(silver_partidas())

    atlas = {r["mando"]: tuple(r)[3:] for r in df.where("clube = 'Atlas'").collect()}
    # Em casa: 2×0 Bravo, 1×1 Cometa, 3×1 Delta. Fora: 2×0 Bravo, 2×2 Cometa, 1×0 Delta.
    assert atlas == {
        "mandante": (3, 2, 1, 0, 6, 2, 7, 0.7778),
        "visitante": (3, 2, 1, 0, 5, 2, 7, 0.7778),
    }


def test_soma_bate_com_a_classificacao_sem_ajuste(silver_partidas, ajustes):
    partidas = silver_partidas()
    desempenho = calcular_desempenho(partidas).groupBy("clube").sum(*SOMADAS)
    final = ultima_rodada(calcular_classificacao(partidas, ajustes))

    soma = {r["clube"]: tuple(r[f"sum({c})"] for c in SOMADAS) for r in desempenho.collect()}
    esperado = {
        r["clube"]: (*(r[c] for c in SOMADAS[:-1]), r["pontos"] - r["ajuste_pontos"])
        for r in final.collect()
    }
    assert soma == esperado
    assert soma["Atlas"][-1] == 14


def test_aproveitamento_com_4_casas(silver_partidas):
    df = calcular_desempenho(silver_partidas())

    # Bravo em casa: 1 vitória, 1 empate e 1 derrota = 4 de 9.
    bravo = df.where("clube = 'Bravo' and mando = 'mandante'").first()
    assert bravo["aproveitamento"] == 0.4444
