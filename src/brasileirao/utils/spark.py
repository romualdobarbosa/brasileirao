from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from brasileirao import config

AMBIENTES_FUTUROS = ("emr", "databricks")
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def get_spark(app_name: str = "brasileirao", conf: dict[str, str] | None = None) -> SparkSession:
    """Sessão Spark com Delta, escolhida por `BRASILEIRAO_SPARK_ENV`."""
    ambiente = config.spark_env()
    if ambiente == "local":
        return _spark_local(app_name, conf or {})
    if ambiente in AMBIENTES_FUTUROS:
        raise NotImplementedError(f"Ambiente Spark {ambiente!r} ainda não foi implementado")
    raise ValueError(
        f"BRASILEIRAO_SPARK_ENV desconhecido: {ambiente!r}. "
        f"Use um de: local, {', '.join(AMBIENTES_FUTUROS)}"
    )


def _spark_local(app_name: str, conf: dict[str, str]) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
    )
    for chave, valor in conf.items():
        builder = builder.config(chave, valor)
    return configure_spark_with_delta_pip(builder).getOrCreate()
