"""Silver → gold: lê a silver inteira, calcula todas as tabelas, checa e só então grava."""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from brasileirao import config
from brasileirao.aggregate import qualidade
from brasileirao.aggregate.classificacao import calcular_classificacao
from brasileirao.aggregate.desempenho import calcular_desempenho
from brasileirao.aggregate.ranking import calcular_ranking
from brasileirao.aggregate.referencia import carregar_ajustes
from brasileirao.aggregate.sequencias import calcular_sequencias
from brasileirao.transform.silver import carregar_mapa_times

log = logging.getLogger(__name__)


class SilverAusenteError(FileNotFoundError):
    pass


@dataclass(frozen=True)
class TabelaGold:
    """Uma tabela do registro.

    `entradas` e `entradas_checagem` nomeiam o que `calcular` e `checar` recebem, em ordem:
    `silver/<tabela>`, uma referência (`ajustes`, `mapa_times`) ou uma tabela gold calculada
    antes no registro. `checar` recebe o DataFrame calculado e devolve os problemas.
    """

    calcular: Callable[..., DataFrame]
    entradas: tuple[str, ...]
    checar: Callable[..., list[str]] | None = None
    entradas_checagem: tuple[str, ...] = ()


# Em ordem de dependência: tabela que usa outra vem depois dela.
TABELAS: dict[str, TabelaGold] = {
    "classificacao_por_rodada": TabelaGold(
        calcular_classificacao,
        ("silver/partidas", "ajustes"),
        qualidade.checar_classificacao,
        ("ajustes", "mapa_times"),
    ),
    "desempenho_mandante_visitante": TabelaGold(calcular_desempenho, ("silver/partidas",)),
    "sequencias": TabelaGold(calcular_sequencias, ("silver/partidas",)),
    "ranking_historico": TabelaGold(calcular_ranking, ("classificacao_por_rodada",)),
}


class Fontes:
    """Entradas das funções do registro, lidas uma vez e só quando alguém pede."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.carregadas: dict[str, DataFrame] = {}
        self.referencias = {"ajustes": carregar_ajustes, "mapa_times": carregar_mapa_times}

    def __getitem__(self, nome: str) -> DataFrame:
        if nome not in self.carregadas:
            self.carregadas[nome] = self._carregar(nome)
        return self.carregadas[nome]

    def __setitem__(self, nome: str, df: DataFrame) -> None:
        self.carregadas[nome] = df

    def _carregar(self, nome: str) -> DataFrame:
        if nome in self.referencias:
            return self.referencias[nome](self.spark)
        camada, _, tabela = nome.partition("/")
        if camada != "silver":
            raise KeyError(f"Entrada desconhecida: {nome!r}")
        caminho = config.caminho_tabela("silver", tabela)
        if not DeltaTable.isDeltaTable(self.spark, caminho):
            raise SilverAusenteError(
                f"silver/{tabela} não existe em {caminho}: rode `make lakehouse` antes"
            )
        return self.spark.read.format("delta").load(caminho)


def gravar(df: DataFrame, tabela: str) -> None:
    """Overwrite sem particionar. Com `overwriteSchema` desligado, mudar o schema falha."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
        .save(config.caminho_tabela("gold", tabela))
    )


def processar_gold(spark: SparkSession) -> dict[str, int]:
    """Calcula e checa todas as tabelas do registro antes de gravar qualquer uma.

    Devolve as linhas gravadas por tabela.
    """
    inicio = time.monotonic()
    fontes = Fontes(spark)
    calculadas = {}
    for nome, tabela in TABELAS.items():
        # Materializada: as checagens, a gravação e as tabelas seguintes leem de novo.
        df = tabela.calcular(*[fontes[e] for e in tabela.entradas]).localCheckpoint()
        calculadas[nome] = fontes[nome] = df
    problemas = [
        problema
        for nome, tabela in TABELAS.items()
        if tabela.checar is not None
        for problema in tabela.checar(
            calculadas[nome], *[fontes[e] for e in tabela.entradas_checagem]
        )
    ]
    if problemas:
        raise qualidade.QualidadeGoldError(problemas)
    linhas = {}
    for nome, df in calculadas.items():
        gravar(df, nome)
        linhas[nome] = df.count()
        log.info("gold/%s: %d linhas", nome, linhas[nome])
    log.info("gold: %d tabela(s) em %.1fs", len(linhas), time.monotonic() - inicio)
    return linhas
