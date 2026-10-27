from datetime import date, datetime
from decimal import Decimal

from brasileirao.transform import limpeza
from brasileirao.transform.silver import carregar_mapa_times

SCHEMA_SILVER = [
    ("partida_id", "int"),
    ("temporada", "int"),
    ("rodada", "int"),
    ("data", "date"),
    ("hora", "string"),
    ("mandante", "string"),
    ("visitante", "string"),
    ("placar_mandante", "int"),
    ("placar_visitante", "int"),
    ("vencedor", "string"),
    ("arena", "string"),
    ("uf_mandante", "string"),
    ("uf_visitante", "string"),
    ("tecnico_mandante", "string"),
    ("tecnico_visitante", "string"),
    ("formacao_mandante", "string"),
    ("formacao_visitante", "string"),
    ("arrecadacao", "decimal(14,2)"),
]


def test_limpar_renomeia_e_tipa(bronze_partidas):
    linha = limpeza.limpar_partidas(bronze_partidas({"ID": "7", "rodata": "12"})).first()

    assert linha["partida_id"] == 7
    assert linha["rodada"] == 12
    assert linha["data"] == date(2019, 4, 27)
    assert (linha["placar_mandante"], linha["placar_visitante"]) == (2, 0)
    assert (linha["uf_mandante"], linha["uf_visitante"]) == ("SP", "RS")


def test_arena_perde_espaco_no_inicio(bronze_partidas):
    df = bronze_partidas({"ID": "1", "arena": " Morumbi"}, {"ID": "2", "arena": " Pacaembu "})

    arenas = {r["partida_id"]: r["arena"] for r in limpeza.limpar_partidas(df).collect()}

    assert arenas == {1: "Morumbi", 2: "Pacaembu"}


def test_empate_vira_vencedor_nulo(bronze_partidas):
    df = bronze_partidas({"vencedor": "-", "mandante_Placar": "1", "visitante_Placar": "1"})

    assert limpeza.limpar_partidas(df).first()["vencedor"] is None


def test_string_vazia_vira_nulo(bronze_partidas):
    df = bronze_partidas(
        {"tecnico_mandante": "", "formacao_visitante": "  ", "hora": "", "arena": " "}
    )

    linha = limpeza.limpar_partidas(df).first()

    assert linha["tecnico_mandante"] is None
    assert linha["formacao_visitante"] is None
    assert linha["hora"] is None
    assert linha["arena"] is None


def test_placar_nao_numerico_vira_nulo(bronze_partidas):
    linha = limpeza.limpar_partidas(bronze_partidas({"mandante_Placar": "x"})).first()

    assert linha["placar_mandante"] is None


def test_arrecadacao_com_centavos(bronze_partidas):
    df = bronze_partidas(
        {"ID": "1", "arrecadacao": "2802427.7"},
        {"ID": "2", "arrecadacao": "220000"},
        {"ID": "3", "arrecadacao": None},
    )

    valores = {r["partida_id"]: r["arrecadacao"] for r in limpeza.limpar_partidas(df).collect()}

    assert valores == {1: Decimal("2802427.70"), 2: Decimal("220000.00"), 3: None}


def test_dedup_fica_o_ingested_at_mais_recente(bronze_partidas):
    df = bronze_partidas(
        {"ID": "1", "mandante_Placar": "1", "_ingested_at": datetime(2026, 1, 1)},
        {"ID": "1", "mandante_Placar": "3", "_ingested_at": datetime(2026, 2, 1)},
        {"ID": "2"},
    )

    resultado = limpeza.deduplicar_partidas(limpeza.limpar_partidas(df))

    placares = {r["partida_id"]: r["placar_mandante"] for r in resultado.collect()}
    assert placares == {1: 3, 2: 2}


def test_dedup_com_mesmo_ingested_at_e_deterministico(bronze_partidas):
    linhas = [{"ID": "1", "mandante_Placar": "1"}, {"ID": "1", "mandante_Placar": "3"}]

    ida = limpeza.deduplicar_partidas(limpeza.limpar_partidas(bronze_partidas(*linhas)))
    volta = limpeza.deduplicar_partidas(limpeza.limpar_partidas(bronze_partidas(*linhas[::-1])))

    assert ida.collect() == volta.collect()
    assert ida.count() == 1


def test_dedup_mantem_ids_nulos_para_a_qualidade(bronze_partidas):
    df = bronze_partidas({"ID": ""}, {"ID": "abc"}, {"ID": "1"})

    assert limpeza.deduplicar_partidas(limpeza.limpar_partidas(df)).count() == 3


def test_normalizar_times_resolve_alias(bronze_partidas, mapa_times):
    df = limpeza.limpar_partidas(
        bronze_partidas({"mandante": "Gremio FBPA", "visitante": "Santos", "vencedor": "Santos"})
    )

    linha = limpeza.normalizar_times(df, mapa_times, limpeza.COLUNAS_TIME_PARTIDAS).first()

    assert (linha["mandante"], linha["visitante"], linha["vencedor"]) == (
        "Grêmio",
        "Santos",
        "Santos",
    )


def test_normalizar_times_sem_mapa_vira_nulo(bronze_partidas, mapa_times):
    df = limpeza.limpar_partidas(bronze_partidas({"visitante": "Time Novo", "vencedor": "-"}))

    linha = limpeza.normalizar_times(df, mapa_times, limpeza.COLUNAS_TIME_PARTIDAS).first()

    assert linha["mandante"] == "São Paulo"
    assert linha["visitante"] is None
    assert linha["vencedor"] is None


def test_para_silver_segue_o_contrato(bronze_partidas, mapa_times):
    limpo = limpeza.deduplicar_partidas(limpeza.limpar_partidas(bronze_partidas({})))

    silver = limpeza.para_silver_partidas(limpo, mapa_times)

    assert silver.dtypes == SCHEMA_SILVER


def test_mapa_de_times_do_pacote(spark):
    mapa = carregar_mapa_times(spark).collect()

    fontes = [r["nome_fonte"] for r in mapa]
    assert len(fontes) == 46
    assert len(set(fontes)) == 46
    assert all(r["nome_canonico"] and len(r["uf"]) == 2 for r in mapa)
    canonicos = {r["nome_fonte"]: r["nome_canonico"] for r in mapa}
    assert canonicos["Gremio"] == "Grêmio"
    assert canonicos["Sao Paulo"] == "São Paulo"
    assert canonicos["Athletico-PR"] == "Athletico-PR"
