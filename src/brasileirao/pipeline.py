"""CLI: `python -m brasileirao.pipeline lakehouse --temporada N | --todas`, `... gold` e
`... explain [--destino DIR]`."""

import argparse
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from brasileirao.aggregate import explain, gold
from brasileirao.aggregate import qualidade as qualidade_gold
from brasileirao.ingestion import bronze
from brasileirao.transform import qualidade, silver
from brasileirao.utils.spark import get_spark
from brasileirao.utils.temporada import partidas_esperadas

log = logging.getLogger(__name__)

# `partidas` vai primeiro: as filhas tiram a temporada de `bronze/partidas`.
ENTIDADES_BRONZE = ("partidas", *bronze.ENTIDADES_FILHAS)


class TemporadaInvalidaError(ValueError):
    pass


def temporada_em_andamento(contagens: dict[int, int]) -> int | None:
    """A maior temporada da fonte, quando ela tem menos partidas que o esperado."""
    if not contagens:
        return None
    maior = max(contagens)
    return maior if contagens[maior] < partidas_esperadas(maior) else None


def temporadas_encerradas(contagens: dict[int, int]) -> list[int]:
    em_andamento = temporada_em_andamento(contagens)
    return sorted(t for t in contagens if t != em_andamento)


def validar_temporada(temporada: int, contagens: dict[int, int]) -> None:
    if temporada not in contagens:
        disponiveis = f"{min(contagens)}–{max(contagens)}" if contagens else "nenhuma"
        raise TemporadaInvalidaError(
            f"Temporada {temporada} não existe na fonte (disponíveis: {disponiveis})"
        )
    if temporada == temporada_em_andamento(contagens):
        raise TemporadaInvalidaError(
            f"Temporada {temporada} está em andamento ({contagens[temporada]} de "
            f"{partidas_esperadas(temporada)} partidas) e fica fora de bronze e silver"
        )


def avisar_orfas(spark: SparkSession) -> None:
    """Filha com `partida_id` fora de `partidas` não tem temporada e não chega na bronze.

    Por isso a checagem roda uma vez por execução, na fonte inteira, e não por temporada.
    """
    partidas = bronze.ler_raw(spark, "partidas").select(F.col("ID").alias("partida_id"))
    for entidade in bronze.ENTIDADES_FILHAS:
        total, ids = qualidade.partida_ids_orfaos(bronze.ler_raw(spark, entidade), partidas)
        if total:
            log.warning(
                "raw/%s: %d linha(s) com partida_id inexistente em partidas, fora de bronze "
                "e silver: %s",
                entidade,
                total,
                ", ".join(ids),
            )


def processar_temporada(spark: SparkSession, temporada: int) -> None:
    """Bronze → transformações → checagens → silver de uma temporada."""
    inicio = time.monotonic()
    for entidade in ENTIDADES_BRONZE:
        bronze.escrever_bronze(spark, entidade, temporada)
    silver.processar_silver(spark, temporada)
    log.info("temporada %d processada em %.1fs", temporada, time.monotonic() - inicio)


def lakehouse(spark: SparkSession, temporada: int | None) -> list[int]:
    """Processa uma temporada ou, com `temporada=None`, todas as encerradas em ordem."""
    contagens = bronze.contar_partidas_por_temporada(spark)
    if temporada is None:
        temporadas = temporadas_encerradas(contagens)
        em_andamento = temporada_em_andamento(contagens)
        if em_andamento is not None:
            log.info("temporada %d em andamento, fica de fora", em_andamento)
    else:
        validar_temporada(temporada, contagens)
        temporadas = [temporada]
    avisar_orfas(spark)
    for t in temporadas:
        processar_temporada(spark, t)
    return temporadas


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m brasileirao.pipeline")
    comandos = parser.add_subparsers(dest="comando", required=True)
    cmd = comandos.add_parser("lakehouse", help="raw → bronze → silver")
    alvo = cmd.add_mutually_exclusive_group(required=True)
    alvo.add_argument("--temporada", type=int, help="processa só essa temporada")
    alvo.add_argument("--todas", action="store_true", help="todas as temporadas encerradas")
    comandos.add_parser("gold", help="silver → gold, todas as tabelas com overwrite")
    cmd = comandos.add_parser("explain", help="planos físicos de algumas tabelas gold em texto")
    cmd.add_argument(
        "--destino",
        default=explain.DESTINO_PADRAO,
        help=f"diretório dos .txt (padrão: {explain.DESTINO_PADRAO})",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    try:
        if args.comando == "gold":
            gold.processar_gold(get_spark())
        elif args.comando == "explain":
            explain.gerar_planos(get_spark(), args.destino)
        else:
            lakehouse(get_spark(), None if args.todas else args.temporada)
    except (
        TemporadaInvalidaError,
        qualidade.QualidadeError,
        qualidade_gold.QualidadeGoldError,
        gold.SilverAusenteError,
    ) as erro:
        raise SystemExit(f"Erro: {erro}") from erro


if __name__ == "__main__":
    main()
