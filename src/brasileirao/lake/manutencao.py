"""`make manutencao`: `OPTIMIZE` (com Z-Order onde há motivo) e `VACUUM`, com a medida antes e
depois de cada tabela.

O `VACUUM` roda só com a retenção padrão do Delta e com a trava de segurança ligada. Este módulo
não recebe retenção como parâmetro e não mexe em configuração de sessão.
"""

import logging
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from brasileirao.lake import caminho_de
from brasileirao.lake.constraints import CONSTRAINTS

log = logging.getLogger(__name__)

# Colunas filtradas de verdade (dashboard em `docs/serving/dashboard.md` e páginas do app), só em
# tabelas com pelo menos mil linhas. A justificativa, e os limites, estão em `docs/delta/`. A
# silver fica de fora: o único filtro do código é `temporada`, que é a coluna de partição.
ZORDER: dict[str, tuple[str, ...]] = {
    "gold/classificacao_por_rodada": ("temporada", "rodada"),
    "gold/cartoes_jogadores": ("temporada",),
    "gold/artilharia": ("temporada",),
    "gold/sequencias": ("temporada",),
    "gold/confrontos": ("clube", "adversario"),
}


@dataclass(frozen=True)
class Medida:
    """`arquivos`/`bytes`: o que a versão atual usa. `*_disco`: Parquet na pasta, inclusive de
    versões antigas. Só o segundo par enxerga o `VACUUM`."""

    arquivos: int
    bytes: int
    arquivos_disco: int
    bytes_disco: int
    versao: int


@dataclass(frozen=True)
class ResultadoManutencao:
    tabela: str
    zorder: tuple[str, ...]
    antes: Medida
    depois: Medida


def _resumo_de_conteudo(spark: SparkSession, caminho: str) -> tuple[int, int]:
    """Arquivos e bytes abaixo de `caminho`, pelo Hadoop: funciona em disco e em `s3://`."""
    jvm_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(caminho)
    sistema = jvm_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    resumo = sistema.getContentSummary(jvm_path)
    return resumo.getFileCount(), resumo.getLength()


def medir(spark: SparkSession, caminho: str) -> Medida:
    alvo = DeltaTable.forPath(spark, caminho)
    detalhe = alvo.detail().first()
    arquivos_pasta, bytes_pasta = _resumo_de_conteudo(spark, caminho)
    arquivos_log, bytes_log = _resumo_de_conteudo(spark, f"{caminho}/_delta_log")
    return Medida(
        arquivos=detalhe["numFiles"],
        bytes=detalhe["sizeInBytes"],
        arquivos_disco=arquivos_pasta - arquivos_log,
        bytes_disco=bytes_pasta - bytes_log,
        versao=alvo.history(1).first()["version"],
    )


def manter_tabela(spark: SparkSession, tabela: str) -> ResultadoManutencao:
    caminho = caminho_de(tabela)
    alvo = DeltaTable.forPath(spark, caminho)
    antes = medir(spark, caminho)
    zorder = ZORDER.get(tabela, ())
    if zorder:
        alvo.optimize().executeZOrderBy(*zorder)
    else:
        alvo.optimize().executeCompaction()
    alvo.vacuum()
    return ResultadoManutencao(tabela, zorder, antes, medir(spark, caminho))


def _mb(valor: int) -> str:
    return f"{valor / 1024 / 1024:.2f}"


def relatorio(resultados: list[ResultadoManutencao]) -> str:
    """Tabela em texto, com o antes → depois da versão atual e da pasta."""
    linhas = [
        f"{'tabela':<36} {'arquivos atuais':>16} {'KB atuais':>17} "
        f"{'arquivos na pasta':>18} {'MB na pasta':>15} {'versão':>10}  z-order"
    ]
    for r in resultados:
        a, d = r.antes, r.depois
        linhas.append(
            f"{r.tabela:<36} {f'{a.arquivos} → {d.arquivos}':>16} "
            f"{f'{a.bytes / 1024:.1f} → {d.bytes / 1024:.1f}':>17} "
            f"{f'{a.arquivos_disco} → {d.arquivos_disco}':>18} "
            f"{f'{_mb(a.bytes_disco)} → {_mb(d.bytes_disco)}':>15} "
            f"{f'{a.versao} → {d.versao}':>10}  {', '.join(r.zorder) or '-'}"
        )
    return "\n".join(linhas)


def manutencao(
    spark: SparkSession, tabelas: tuple[str, ...] | None = None
) -> list[ResultadoManutencao]:
    """Roda em `tabelas` (padrão: toda silver e gold do registro) e pula as que não existem."""
    resultados = []
    for tabela in tabelas if tabelas is not None else tuple(CONSTRAINTS):
        if not DeltaTable.isDeltaTable(spark, caminho_de(tabela)):
            log.warning("%s não existe: pulada", tabela)
            continue
        resultado = manter_tabela(spark, tabela)
        log.info(
            "%s: %d → %d arquivos atuais, %d → %d na pasta",
            tabela,
            resultado.antes.arquivos,
            resultado.depois.arquivos,
            resultado.antes.arquivos_disco,
            resultado.depois.arquivos_disco,
        )
        resultados.append(resultado)
    print(relatorio(resultados))
    return resultados
