"""Checagens do gold. Rodam antes da escrita: uma falha não grava nenhuma tabela gold."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LIMITE_EXEMPLOS = 20
# Chapecoense × Atlético-MG de 2016 não foi jogada: as duas terminam com 1 jogo a menos.
JOGOS_A_MENOS = {(2016, "Chapecoense"): 1, (2016, "Atlético-MG"): 1}


class QualidadeGoldError(Exception):
    """Falha de qualidade no gold. `problemas` lista cada checagem que falhou."""

    def __init__(self, problemas: list[str]) -> None:
        self.problemas = problemas
        detalhe = "\n".join(f"- {p}" for p in problemas)
        super().__init__(f"gold reprovado, nenhuma tabela gravada:\n{detalhe}")


def ultima_rodada(classificacao: DataFrame) -> DataFrame:
    """Linhas da última rodada de cada temporada."""
    ultima = F.max("rodada").over(Window.partitionBy("temporada"))
    return (
        classificacao.withColumn("_ultima", ultima)
        .where(F.col("rodada") == F.col("_ultima"))
        .drop("_ultima")
    )


def posicoes_invalidas(classificacao: DataFrame) -> list[str]:
    """(`temporada`, `rodada`) em que `posicao` repete ou sai de 1..N."""
    linhas = (
        classificacao.groupBy("temporada", "rodada")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.count_distinct("posicao").alias("distintas"),
            F.min("posicao").alias("menor"),
            F.max("posicao").alias("maior"),
        )
        .where(
            (F.col("distintas") != F.col("n"))
            | (F.col("menor") != 1)
            | (F.col("maior") != F.col("n"))
        )
        .orderBy("temporada", "rodada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    return [f"({r['temporada']}, {r['rodada']})" for r in linhas]


def jogos_finais_invalidos(classificacao: DataFrame) -> list[str]:
    """Clubes que não terminam a temporada com 2 × (times − 1) jogos, fora a exceção de 2016."""
    final = ultima_rodada(classificacao).withColumn(
        "_times", F.count(F.lit(1)).over(Window.partitionBy("temporada"))
    )
    a_menos = F.lit(0)
    for (temporada, clube), jogos in JOGOS_A_MENOS.items():
        a_menos = F.when(
            (F.col("temporada") == temporada) & (F.col("clube") == clube), jogos
        ).otherwise(a_menos)
    linhas = (
        final.withColumn("_esperado", (F.col("_times") - 1) * 2 - a_menos)
        .where(F.col("jogos") != F.col("_esperado"))
        .orderBy("temporada", "clube")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    return [
        f"{r['temporada']} {r['clube']}: {r['jogos']} jogos, esperado {r['_esperado']}"
        for r in linhas
    ]


def vitorias_derrotas_desiguais(classificacao: DataFrame) -> list[str]:
    """Temporadas em que a soma de vitórias da última rodada difere da de derrotas."""
    linhas = (
        ultima_rodada(classificacao)
        .groupBy("temporada")
        .agg(F.sum("vitorias").alias("v"), F.sum("derrotas").alias("d"))
        .where(F.col("v") != F.col("d"))
        .orderBy("temporada")
        .collect()
    )
    return [f"{r['temporada']}: {r['v']} vitórias, {r['d']} derrotas" for r in linhas]


def ajustes_invalidos(ajustes: DataFrame, mapa: DataFrame, classificacao: DataFrame) -> list[str]:
    """Clube de ajuste fora do mapa ou sem jogo na temporada.

    Só olha as temporadas que estão na classificação: com a silver parcial, o ajuste de uma
    temporada que não foi carregada não tem onde entrar e não é erro.
    """
    canonicos = mapa.select(F.col("nome_canonico").alias("clube")).distinct()
    fora_do_mapa = ajustes.join(F.broadcast(canonicos), on="clube", how="left_anti")
    temporadas = classificacao.select("temporada").distinct()
    com_jogo = ultima_rodada(classificacao).where(F.col("jogos") > 0).select("temporada", "clube")
    sem_jogo = (
        ajustes.join(canonicos, on="clube")
        .join(temporadas, on="temporada")
        .join(com_jogo, on=["temporada", "clube"], how="left_anti")
    )
    problemas = [
        (r["temporada"], r["clube"], "clube fora de reference/times.csv")
        for r in fora_do_mapa.select("temporada", "clube").distinct().collect()
    ] + [
        (r["temporada"], r["clube"], "ajuste de clube sem jogo na temporada")
        for r in sem_jogo.select("temporada", "clube").distinct().collect()
    ]
    return [f"{t} {c}: {motivo}" for t, c, motivo in sorted(problemas)]


def checar_classificacao(
    classificacao: DataFrame, ajustes: DataFrame, mapa: DataFrame
) -> list[str]:
    """Todas as checagens de falha de `classificacao_por_rodada`, já com o nome da tabela."""
    checagens = [
        (
            "posição repetida ou fora de 1..N em (temporada, rodada)",
            posicoes_invalidas(classificacao),
        ),
        ("jogos na última rodada", jogos_finais_invalidos(classificacao)),
        ("vitórias ≠ derrotas na última rodada", vitorias_derrotas_desiguais(classificacao)),
        ("ajustes_pontos.csv", ajustes_invalidos(ajustes, mapa, classificacao)),
    ]
    return [
        f"classificacao_por_rodada: {titulo}: {', '.join(exemplos)}"
        for titulo, exemplos in checagens
        if exemplos
    ]


def avisos_disciplina(disciplina: DataFrame) -> list[str]:
    """Clube com cartão e sem partida na temporada. É aviso, não reprova o gold."""
    linhas = (
        disciplina.where(F.col("jogos") == 0)
        .orderBy("temporada", "clube")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if not linhas:
        return []
    exemplos = ", ".join(f"{r['temporada']} {r['clube']}" for r in linhas)
    return [f"disciplina: clube sem partida na temporada: {exemplos}"]


def checar_gols_por_minuto(gols_por_minuto: DataFrame, gols: DataFrame) -> list[str]:
    """Temporadas em que o total de `gols_por_minuto` difere do número de linhas de
    `silver/gols`."""
    na_tabela = gols_por_minuto.groupBy("temporada").agg(F.sum("gols").alias("tabela"))
    na_silver = gols.groupBy("temporada").agg(F.count(F.lit(1)).alias("silver"))
    linhas = (
        na_tabela.join(na_silver, on="temporada", how="full")
        .where(~F.col("tabela").eqNullSafe(F.col("silver")))
        .orderBy("temporada")
        .limit(LIMITE_EXEMPLOS)
        .collect()
    )
    if not linhas:
        return []
    exemplos = ", ".join(
        f"{r['temporada']}: {r['tabela'] or 0} na tabela, {r['silver'] or 0} na silver"
        for r in linhas
    )
    return [f"gols_por_minuto: total de gols ≠ silver/gols: {exemplos}"]
