import pytest
from pyspark.sql import functions as F

from brasileirao.aggregate import qualidade
from brasileirao.aggregate.classificacao import calcular_classificacao

from .conftest import MINICAMPEONATO


@pytest.fixture
def mapa(spark):
    clubes = ["Atlas", "Bravo", "Cometa", "Delta"]
    return spark.createDataFrame(
        [(c, c, "SP") for c in clubes], "nome_fonte string, nome_canonico string, uf string"
    )


@pytest.fixture
def classificacao(silver_partidas, ajustes):
    return calcular_classificacao(silver_partidas(), ajustes).localCheckpoint()


def test_classificacao_valida_passa(classificacao, ajustes, mapa):
    assert qualidade.checar_classificacao(classificacao, ajustes, mapa) == []


def test_posicao_repetida(classificacao):
    repetida = classificacao.withColumn(
        "posicao",
        F.when((F.col("rodada") == 3) & (F.col("posicao") == 2), 1).otherwise(F.col("posicao")),
    )

    assert qualidade.posicoes_invalidas(repetida) == ["(2019, 3)"]


def test_posicao_fora_de_1_a_n(classificacao):
    fora = classificacao.withColumn(
        "posicao",
        F.when((F.col("rodada") == 5) & (F.col("posicao") == 4), 5).otherwise(F.col("posicao")),
    )

    assert qualidade.posicoes_invalidas(fora) == ["(2019, 5)"]


def test_jogos_na_ultima_rodada(silver_partidas, sem_ajustes):
    # Sem Bravo × Atlas da rodada 4, os dois terminam com 5 jogos em vez de 6.
    partidas = silver_partidas([p for p in MINICAMPEONATO if p[0] != 7])
    classificacao = calcular_classificacao(partidas, sem_ajustes)

    assert qualidade.jogos_finais_invalidos(classificacao) == [
        "2019 Atlas: 5 jogos, esperado 6",
        "2019 Bravo: 5 jogos, esperado 6",
    ]


def test_excecao_de_2016(silver_partidas, sem_ajustes):
    # O minicampeonato renomeado para 2016, sem Bravo × Atlas da rodada 4: o par
    # Chapecoense × Atlético-MG pode ter 1 jogo a menos, outro clube não.
    nomes = {"Atlas": "Chapecoense", "Bravo": "Atlético-MG"}
    sem_jogo = [
        (*p[:3], nomes.get(p[3], p[3]), nomes.get(p[4], p[4]), *p[5:])
        for p in MINICAMPEONATO
        if p[0] != 7
    ]
    classificacao = calcular_classificacao(silver_partidas(sem_jogo, 2016), sem_ajustes)

    assert qualidade.jogos_finais_invalidos(classificacao) == []
    assert qualidade.jogos_finais_invalidos(
        classificacao.withColumn(
            "jogos",
            F.when(F.col("clube") == "Cometa", F.col("jogos") - 1).otherwise(F.col("jogos")),
        )
    ) == ["2016 Cometa: 5 jogos, esperado 6"]


def test_vitorias_e_derrotas_desiguais(classificacao):
    desigual = classificacao.withColumn(
        "vitorias",
        F.when(F.col("clube") == "Delta", F.col("vitorias") + 1).otherwise(F.col("vitorias")),
    )

    assert qualidade.vitorias_derrotas_desiguais(desigual) == ["2019: 8 vitórias, 7 derrotas"]


def test_ajuste_de_clube_fora_do_mapa_ou_sem_jogo(spark, classificacao, mapa):
    ajustes = spark.createDataFrame(
        [
            (2019, "Atlas", -1, "ok", "u"),
            (2019, "Zebra", -3, "fora do mapa", "u"),
            (2019, "Fantasma", -2, "no mapa, sem jogo", "u"),
            (2003, "Atlas", -4, "temporada fora da classificação", "u"),
        ],
        "temporada int, clube string, pontos int, motivo string, fonte string",
    )
    mapa = mapa.unionByName(spark.createDataFrame([("Fantasma", "Fantasma", "SP")], mapa.schema))

    assert qualidade.ajustes_invalidos(ajustes, mapa, classificacao) == [
        "2019 Fantasma: ajuste de clube sem jogo na temporada",
        "2019 Zebra: clube fora de reference/times.csv",
    ]


def test_checar_classificacao_junta_os_problemas(classificacao, ajustes, mapa):
    ruim = classificacao.withColumn("posicao", F.lit(1))

    problemas = qualidade.checar_classificacao(ruim, ajustes, mapa)

    assert len(problemas) == 1
    assert problemas[0].startswith("classificacao_por_rodada: posição repetida ou fora de 1..N")


def test_erro_lista_os_problemas():
    erro = qualidade.QualidadeGoldError(["a", "b"])

    assert erro.problemas == ["a", "b"]
    assert str(erro) == "gold reprovado, nenhuma tabela gravada:\n- a\n- b"


def _ate_a_rodada(rodada):
    return [p for p in MINICAMPEONATO if p[1] <= rodada]


def test_temporada_parcial_reprova_sem_em_andamento(silver_partidas, ajustes, mapa):
    # Só até a rodada 3: cada clube tem 2 ou 3 jogos, e a execução normal recusa.
    classificacao = calcular_classificacao(silver_partidas(_ate_a_rodada(3)), ajustes)

    problemas = qualidade.checar_classificacao(classificacao, ajustes, mapa)

    assert [p.split(": ")[1] for p in problemas] == ["jogos na última rodada"]


def test_temporada_parcial_passa_com_em_andamento(silver_partidas, ajustes, mapa):
    classificacao = calcular_classificacao(silver_partidas(_ate_a_rodada(3)), ajustes)

    assert qualidade.checar_classificacao(classificacao, ajustes, mapa, em_andamento=2019) == []


def test_em_andamento_nao_afrouxa_outra_temporada(silver_partidas, sem_ajustes):
    # 2019 incompleta e declarada em andamento 2020: 2019 continua sendo cobrada.
    partidas = silver_partidas([p for p in MINICAMPEONATO if p[0] != 7])
    classificacao = calcular_classificacao(partidas, sem_ajustes)

    assert qualidade.jogos_finais_invalidos(classificacao, em_andamento=2020) == [
        "2019 Atlas: 5 jogos, esperado 6",
        "2019 Bravo: 5 jogos, esperado 6",
    ]
    assert qualidade.jogos_finais_invalidos(classificacao, em_andamento=2019) == []


def test_jogos_acima_das_rodadas(classificacao):
    assert qualidade.jogos_parciais_invalidos(classificacao) == []
    a_mais = classificacao.withColumn(
        "jogos",
        F.when((F.col("clube") == "Delta") & (F.col("rodada") == 2), 3).otherwise(F.col("jogos")),
    )

    assert qualidade.jogos_parciais_invalidos(a_mais) == ["2019 rodada 2 Delta: 3 jogos"]


def test_pontos_inconsistentes(classificacao):
    assert qualidade.pontos_invalidos(classificacao) == []
    errado = classificacao.withColumn(
        "pontos",
        F.when((F.col("clube") == "Bravo") & (F.col("rodada") == 3), 99).otherwise(F.col("pontos")),
    )
    caindo = classificacao.withColumn(
        "vitorias",
        F.when((F.col("clube") == "Atlas") & (F.col("rodada") == 4), 0).otherwise(
            F.col("vitorias")
        ),
    ).withColumn("pontos", F.col("vitorias") * 3 + F.col("empates") + F.col("ajuste_pontos"))

    assert qualidade.pontos_invalidos(errado) == ["2019 rodada 3 Bravo"]
    assert qualidade.pontos_invalidos(caindo) == ["2019 rodada 4 Atlas"]
