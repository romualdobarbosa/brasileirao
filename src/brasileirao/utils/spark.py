from pyspark.sql import DataFrame, SparkSession

from brasileirao import config

AMBIENTES_FUTUROS = ("emr",)
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
# No serverless do Databricks quase nenhuma propriedade da sessão pode ser mudada; esta pode.
CONF_DATABRICKS = frozenset({"spark.sql.shuffle.partitions"})
MATERIALIZACOES_DATABRICKS = ("local_checkpoint", "cache")


def get_spark(
    app_name: str = "brasileirao",
    conf: dict[str, str] | None = None,
    pacotes: list[str] | None = None,
) -> SparkSession:
    """Sessão Spark com Delta, escolhida por `BRASILEIRAO_SPARK_ENV`.

    `pacotes` são coordenadas Maven extras (ex.: o driver JDBC), baixadas pelo Ivy junto com
    os jars do Delta. Só valem na criação da sessão: uma sessão já aberta não ganha pacote.
    """
    ambiente = config.spark_env()
    if ambiente == "local":
        return _spark_local(app_name, conf or {}, pacotes)
    if ambiente == "databricks":
        return _spark_databricks(conf or {}, pacotes)
    if ambiente in AMBIENTES_FUTUROS:
        raise NotImplementedError(f"Ambiente Spark {ambiente!r} ainda não foi implementado")
    raise ValueError(
        f"BRASILEIRAO_SPARK_ENV desconhecido: {ambiente!r}. "
        f"Use um de: local, databricks, {', '.join(AMBIENTES_FUTUROS)}"
    )


def _spark_local(app_name: str, conf: dict[str, str], pacotes: list[str] | None) -> SparkSession:
    # Importado aqui: o `configure_spark_with_delta_pip` é do delta-spark do PyPI, que o
    # Databricks não tem.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        # Os defaults (200 partições de shuffle, 50 no snapshot do Delta) são para cluster;
        # com tabelas de milhares de linhas, viram centenas de tarefas vazias por temporada.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        # O `VACUUM` lista a tabela com um job deste tamanho: o default de 10.000 tarefas custa
        # uns 9 s por tabela, mesmo numa pasta com meia dúzia de arquivos.
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "8")
    )
    for chave, valor in conf.items():
        builder = builder.config(chave, valor)
    return configure_spark_with_delta_pip(builder, extra_packages=pacotes).getOrCreate()


def _spark_databricks(conf: dict[str, str], pacotes: list[str] | None) -> SparkSession:
    """A sessão que o Databricks já abriu, com Delta pronto.

    Nada de extensão, catálogo, Maven ou nome de app: no serverless isso é do Databricks.
    """
    if pacotes:
        raise ValueError(f"Ambiente databricks não baixa pacote Maven: {', '.join(pacotes)}")
    recusadas = sorted(set(conf) - CONF_DATABRICKS)
    if recusadas:
        raise ValueError(
            f"Ambiente databricks só aceita {', '.join(sorted(CONF_DATABRICKS))}; "
            f"recusadas: {', '.join(recusadas)}"
        )
    sessao = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    for chave, valor in conf.items():
        sessao.conf.set(chave, valor)
    return sessao


def materializar(df: DataFrame) -> DataFrame:
    """Calcula o DataFrame agora e devolve uma versão que não recalcula a partir da fonte.

    No local é o `localCheckpoint()` de sempre. No Databricks, `localCheckpoint` não está
    documentado no serverless; `BRASILEIRAO_MATERIALIZAR=cache` troca por `cache()` + `count()`
    se a sondagem mostrar que ele falha. O `cache` não corta a linhagem: se o cache cair, o
    Spark recalcula a partir da fonte, inclusive de uma tabela que já foi regravada.
    """
    if config.spark_env() != "databricks":
        return df.localCheckpoint()
    modo = config.materializacao_databricks()
    if modo == "local_checkpoint":
        return df.localCheckpoint()
    if modo == "cache":
        df = df.cache()
        df.count()
        return df
    raise ValueError(
        f"BRASILEIRAO_MATERIALIZAR desconhecido: {modo!r}. "
        f"Use um de: {', '.join(MATERIALIZACOES_DATABRICKS)}"
    )
