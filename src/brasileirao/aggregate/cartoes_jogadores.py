"""`cartoes_jogadores`: cartões de cada atleta por temporada e clube."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from brasileirao.aggregate.qualidade import LIMITE_EXEMPLOS


def _com_atleta(cartoes: DataFrame) -> DataFrame:
    """A linha sem `atleta` não tem a quem ser atribuída."""
    return cartoes.where(F.col("atleta").isNotNull())


def calcular_cartoes_jogadores(cartoes: DataFrame) -> DataFrame:
    """Silver de cartões → uma linha por (`temporada`, `atleta`, `clube`).

    Quem troca de clube na temporada fica com uma linha em cada um.
    """
    return (
        _com_atleta(cartoes)
        .groupBy("temporada", "atleta", "clube")
        .agg(
            F.count_if(F.col("cartao") == "Amarelo").cast("int").alias("amarelos"),
            F.count_if(F.col("cartao") == "Vermelho").cast("int").alias("vermelhos"),
        )
    )


def checar_cartoes_jogadores(cartoes_jogadores: DataFrame, cartoes: DataFrame) -> list[str]:
    """Temporadas em que o total da tabela difere da silver, tirando as linhas sem `atleta`.

    Um `cartao` que não seja amarelo nem vermelho some da tabela e aparece aqui.
    """
    na_tabela = cartoes_jogadores.groupBy("temporada").agg(
        F.sum(F.col("amarelos") + F.col("vermelhos")).alias("tabela")
    )
    na_silver = _com_atleta(cartoes).groupBy("temporada").agg(F.count(F.lit(1)).alias("silver"))
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
    return [f"cartoes_jogadores: total de cartões ≠ silver/cartoes: {exemplos}"]
