"""CLI: `python -m brasileirao.pipeline lakehouse --temporada N | --todas [--em-andamento]`,
`... gold [--em-andamento]`,
`... explain [--destino DIR]`, `... carga`, `... exportar [--destino DIR]`, `... manutencao` e
`... historico camada/tabela [--comparar A B]`."""

import argparse
import logging
import sys
import time

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from brasileirao import execucoes
from brasileirao.aggregate import explain, gold
from brasileirao.aggregate import qualidade as qualidade_gold
from brasileirao.config import VariavelAusenteError
from brasileirao.ingestion import bronze
from brasileirao.lake import (
    TabelaInvalidaError,
    caminho_de,
    cdf,
    constraints,
    historico,
    manutencao,
)
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


def validar_temporada(
    temporada: int, contagens: dict[int, int], aceitar_em_andamento: bool = False
) -> None:
    """Recusa temporada fora da fonte e, sem `aceitar_em_andamento`, a que está em andamento."""
    if temporada not in contagens:
        disponiveis = f"{min(contagens)}–{max(contagens)}" if contagens else "nenhuma"
        raise TemporadaInvalidaError(
            f"Temporada {temporada} não existe na fonte (disponíveis: {disponiveis})"
        )
    if not aceitar_em_andamento and temporada == temporada_em_andamento(contagens):
        raise TemporadaInvalidaError(
            f"Temporada {temporada} está em andamento ({contagens[temporada]} de "
            f"{partidas_esperadas(temporada)} partidas) e fica fora de bronze e silver; "
            "para processar mesmo assim, use --em-andamento"
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
        execucoes.contar(f"bronze/{entidade}", bronze.escrever_bronze(spark, entidade, temporada))
    silver.processar_silver(spark, temporada)
    log.info("temporada %d processada em %.1fs", temporada, time.monotonic() - inicio)


def lakehouse(
    spark: SparkSession, temporada: int | None, aceitar_em_andamento: bool = False
) -> list[int]:
    """Processa uma temporada ou, com `temporada=None`, todas as encerradas em ordem.

    Com `aceitar_em_andamento`, a temporada em andamento também entra (a simulação usa).
    """
    contagens = bronze.contar_partidas_por_temporada(spark)
    if temporada is None:
        temporadas = sorted(contagens) if aceitar_em_andamento else temporadas_encerradas(contagens)
        em_andamento = temporada_em_andamento(contagens)
        if em_andamento is not None and not aceitar_em_andamento:
            log.info("temporada %d em andamento, fica de fora", em_andamento)
    else:
        validar_temporada(temporada, contagens, aceitar_em_andamento)
        temporadas = [temporada]
    avisar_orfas(spark)
    for t in temporadas:
        processar_temporada(spark, t)
    constraints.aplicar_camada(spark, "silver")
    for tabela in cdf.TABELAS_COM_CDF:
        if DeltaTable.isDeltaTable(spark, caminho_de(tabela)):
            cdf.ligar(spark, tabela)
    return temporadas


def temporada_em_andamento_na_silver(spark: SparkSession) -> int | None:
    """A temporada em andamento segundo a `silver/partidas`, pela mesma regra da fonte."""
    caminho = caminho_de("silver/partidas")
    if not DeltaTable.isDeltaTable(spark, caminho):
        return None
    linhas = spark.read.format("delta").load(caminho).groupBy("temporada").count().collect()
    return temporada_em_andamento({r["temporada"]: r["count"] for r in linhas})


def executar_gold(spark: SparkSession, aceitar_em_andamento: bool = False) -> dict[str, int]:
    """Silver → gold e as constraints do gold.

    Sem `aceitar_em_andamento`, temporada incompleta na silver reprova o gold, como sempre. Com
    ele, a temporada em andamento passa pelas checagens de temporada parcial e as demais pelas
    de sempre.

    As constraints entram depois da gravação porque na primeira execução a tabela ainda não
    existe. Nas seguintes elas já estão na tabela e valem durante a própria gravação.
    """
    em_andamento = temporada_em_andamento_na_silver(spark) if aceitar_em_andamento else None
    if em_andamento is not None:
        log.info("gold: temporada %d em andamento, com as checagens parciais", em_andamento)
    linhas = gold.processar_gold(spark, em_andamento)
    constraints.aplicar_camada(spark, "gold")
    for tabela, total in (linhas or {}).items():
        execucoes.contar(f"gold/{tabela}", total)
    return linhas


def consultar_historico(
    spark: SparkSession, tabela: str, comparar: list[int] | None = None
) -> None:
    historico.imprimir_historico(spark, tabela)
    if comparar:
        print(historico.comparar_versoes(spark, tabela, *comparar).resumo())


def _carga() -> None:
    # `serving` puxa o psycopg, e o `exportar`, o py4j da JVM local: importados só no próprio
    # subcomando, para o pipeline rodar onde eles não existem (Databricks).
    from brasileirao.serving import carga, postgres
    from brasileirao.serving.tabelas import DRIVER_JDBC

    try:
        linhas = carga.carregar(get_spark(pacotes=[DRIVER_JDBC]))
    except (carga.GoldAusenteError, carga.CargaError, postgres.PostgresIndisponivelError) as erro:
        raise SystemExit(f"Erro: {erro}") from erro
    for tabela, total in (linhas or {}).items():
        execucoes.contar(f"postgres/{tabela}", total)


def _exportar(destino: str | None) -> None:
    from brasileirao.aggregate import exportar

    try:
        linhas = exportar.exportar(get_spark(), destino or exportar.DESTINO_PADRAO)
    except exportar.GoldIncompletoError as erro:
        raise SystemExit(f"Erro: {erro}") from erro
    for tabela, total in (linhas or {}).items():
        execucoes.contar(f"exportar/{tabela}", total)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m brasileirao.pipeline")
    comandos = parser.add_subparsers(dest="comando", required=True)
    cmd = comandos.add_parser("lakehouse", help="raw → bronze → silver")
    alvo = cmd.add_mutually_exclusive_group(required=True)
    alvo.add_argument("--temporada", type=int, help="processa só essa temporada")
    alvo.add_argument("--todas", action="store_true", help="todas as temporadas encerradas")
    cmd.add_argument(
        "--em-andamento",
        action="store_true",
        help="aceita a temporada em andamento (a execução normal recusa)",
    )
    cmd = comandos.add_parser("gold", help="silver → gold, todas as tabelas com overwrite")
    cmd.add_argument(
        "--em-andamento",
        action="store_true",
        help="aceita temporada parcial na silver, com as checagens de temporada parcial",
    )
    cmd = comandos.add_parser("explain", help="planos físicos de algumas tabelas gold em texto")
    cmd.add_argument(
        "--destino",
        default=explain.DESTINO_PADRAO,
        help=f"diretório dos .txt (padrão: {explain.DESTINO_PADRAO})",
    )
    comandos.add_parser("carga", help="gold → Postgres do .env (gold_carga e troca)")
    cmd = comandos.add_parser("exportar", help="gold → um Parquet por tabela, para o app")
    cmd.add_argument("--destino", help="diretório dos .parquet (padrão: o que o app lê, app/dados)")
    comandos.add_parser("manutencao", help="OPTIMIZE, Z-Order e VACUUM com retenção padrão")
    cmd = comandos.add_parser("historico", help="versões de uma tabela Delta")
    cmd.add_argument("tabela", help="camada/tabela, como silver/partidas")
    cmd.add_argument(
        "--comparar", nargs=2, type=int, metavar=("A", "B"), help="compara duas versões"
    )
    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    # Toda execução vira uma linha em `ops/execucoes`, inclusive a que falha (ver execucoes).
    with execucoes.registrar(execucoes.comando_de("pipeline", argv)):
        _executar(args)


def _executar(args: argparse.Namespace) -> None:
    try:
        if args.comando == "gold":
            executar_gold(get_spark(), args.em_andamento)
        elif args.comando == "carga":
            _carga()
        elif args.comando == "exportar":
            _exportar(args.destino)
        elif args.comando == "explain":
            explain.gerar_planos(get_spark(), args.destino)
        elif args.comando == "manutencao":
            manutencao.manutencao(get_spark())
        elif args.comando == "historico":
            consultar_historico(get_spark(), args.tabela, args.comparar)
        else:
            lakehouse(get_spark(), None if args.todas else args.temporada, args.em_andamento)
    except (
        TemporadaInvalidaError,
        qualidade.QualidadeError,
        qualidade_gold.QualidadeGoldError,
        gold.SilverAusenteError,
        VariavelAusenteError,
        constraints.ConstraintRecusadaError,
        TabelaInvalidaError,
        historico.VersaoIndisponivelError,
    ) as erro:
        raise SystemExit(f"Erro: {erro}") from erro


if __name__ == "__main__":
    main()
