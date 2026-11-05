"""Cada página do app abre com os dados de verdade e reage aos filtros principais."""

import json
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

APP = Path(__file__).parent.parent.parent / "app"
DADOS = APP / "dados"

# Página → como ela diz o período que cobre.
PAGINAS = {
    "temporada": "de 2003 a 2025",
    "historico": "de 2003 a 2025",
    "confronto": "de 2003 a 2025",
    "viradas": "De 2014 a 2025",
    "rebaixamento": "De 2003 a 2025",
    "goleadas": "De 2003 a 2025",
    "tecnicos": "de 2014 a 2024",
    "disciplina": "de 2014 a 2024",
}


def abrir(pagina: str) -> AppTest:
    app = AppTest.from_file(str(APP / "paginas" / f"{pagina}.py"), default_timeout=60).run()
    assert not app.exception
    return app


def textos(app: AppTest) -> str:
    return " ".join(e.value for lista in (app.markdown, app.caption, app.info) for e in lista)


def ler(tabela: str) -> pd.DataFrame:
    return pd.read_parquet(DADOS / f"{tabela}.parquet")


@pytest.mark.parametrize("pagina", PAGINAS)
def test_pagina_abre_com_periodo_e_rodape(pagina):
    app = abrir(pagina)

    assert PAGINAS[pagina] in textos(app)
    rodape = app.caption[-1].value
    assert "pipeline em Spark" in rodape
    assert "https://github.com/romualdobarbosa/brasileirao" in rodape


def test_entrada_abre_temporada_e_navega_pelas_oito_paginas():
    app = AppTest.from_file(str(APP / "streamlit_app.py"), default_timeout=60).run()
    assert not app.exception
    assert app.title[0].value == "Temporada"

    titulos = {
        "historico": "Histórico",
        "confronto": "Confronto direto",
        "viradas": "Viradas e fim de jogo",
        "rebaixamento": "Rebaixamento e campanhas",
        "goleadas": "Goleadas",
        "tecnicos": "Técnicos e esquemas",
        "disciplina": "Disciplina",
    }
    for pagina, titulo in titulos.items():
        app.switch_page(f"paginas/{pagina}.py").run()
        assert not app.exception
        assert app.title[0].value == titulo


def test_tabela_final_de_2025_bate_com_o_parquet():
    tela = abrir("temporada").dataframe[0].value

    classificacao = ler("classificacao_por_rodada")
    final = classificacao[(classificacao["temporada"] == 2025) & (classificacao["rodada"] == 38)]
    final = final.sort_values("posicao").reset_index(drop=True)
    assert len(tela) == 20
    assert tela["Clube"].tolist() == final["clube"].tolist()
    for coluna, origem in {
        "Pos.": "posicao",
        "Pts": "pontos",
        "J": "jogos",
        "V": "vitorias",
        "E": "empates",
        "D": "derrotas",
        "GP": "gols_pro",
        "GC": "gols_contra",
        "SG": "saldo",
        "Aprov.": "aproveitamento",
        "Últimos 5": "ultimos_5",
    }.items():
        assert tela[coluna].tolist() == final[origem].tolist(), coluna
    # A mesma tabela final, pela campanha.
    campanhas = ler("campanhas")
    campanhas = campanhas[campanhas["temporada"] == 2025].sort_values("posicao")
    assert tela["Clube"].tolist() == campanhas["clube"].tolist()
    assert tela["Pts"].tolist() == campanhas["pontos"].tolist()
    assert tela["Clube"].iloc[0] == "Flamengo"


def test_temporada_troca_ano_e_rodada():
    app = abrir("temporada")

    app.selectbox(key="temporada").set_value(2003).run()
    assert not app.exception
    assert len(app.dataframe[0].value) == 24
    assert app.slider(key="rodada_2003").value == 46
    assert app.metric[0].value == "Cruzeiro"

    app.slider(key="rodada_2003").set_value(1).run()
    assert not app.exception
    assert app.dataframe[0].value["J"].max() == 1


def test_temporada_sem_gols_avisa_em_vez_de_quebrar():
    app = abrir("temporada")

    app.selectbox(key="temporada").set_value(2010).run()

    assert not app.exception
    assert [i.value for i in app.info] == ["A fonte só traz os gols a partir de 2014."] * 2


def test_temporada_2016_explica_os_379_jogos():
    app = abrir("temporada")
    assert "379 jogos" not in textos(app)

    app.selectbox(key="temporada").set_value(2016).run()

    assert "379 jogos" in textos(app)


def test_curva_de_posicao_tem_o_lider_no_alto_e_destaca_os_escolhidos():
    app = abrir("temporada")

    def curva():
        return json.loads(app.get("vega_lite_chart")[0].proto.spec)

    dominio = curva()["layer"][0]["encoding"]["y"]["scale"]["domain"]
    assert dominio[0] > dominio[1]
    cores = curva()["layer"][1]["encoding"]["color"]["scale"]["domain"]
    assert cores == ["Flamengo", "Palmeiras"]

    app.selectbox(key="meu_time_2025").set_value("Vasco").run()
    app.selectbox(key="outro_time_2025").set_value("Ninguém").run()
    assert not app.exception
    assert curva()["layer"][1]["encoding"]["color"]["scale"]["domain"] == ["Vasco"]


def test_clube_escolhido_segue_no_outro_ano_quando_ele_jogou():
    app = abrir("temporada")
    app.selectbox(key="meu_time_2025").set_value("Vasco").run()

    app.selectbox(key="temporada").set_value(2020).run()
    assert app.selectbox(key="meu_time_2020").value == "Vasco"

    # O Vasco não estava na Série A de 2009: volta o campeão daquele ano.
    app.selectbox(key="temporada").set_value(2009).run()
    assert not app.exception
    assert app.selectbox(key="meu_time_2009").value == "Flamengo"


def test_confronto_mostra_os_dois_lados():
    app = abrir("confronto")

    assert [m.value for m in app.metric] == ["15", "14", "7"]
    assert "Flamengo 1 × 1 Vasco, em 21/09/2025" in textos(app)

    app.selectbox(key="clube_a").set_value("Vasco").run()
    app.selectbox(key="clube_b").set_value("Flamengo").run()
    assert [m.value for m in app.metric] == ["7", "14", "15"]


def test_fregueses_e_carrascos_respeitam_o_minimo_de_jogos():
    app = abrir("confronto")

    assert len(app.dataframe) == 4
    for tabela in app.dataframe:
        assert (tabela.value["J"] >= 10).all()
    fregueses, carrascos = app.dataframe[0].value, app.dataframe[1].value
    assert fregueses["Aprov."].is_monotonic_decreasing
    assert carrascos["Aprov."].is_monotonic_increasing
    assert fregueses["Aprov."].iloc[0] > carrascos["Aprov."].iloc[0]


def test_confronto_sem_jogo_ou_com_o_mesmo_clube_avisa():
    app = abrir("confronto")

    app.selectbox(key="clube_b").set_value("Flamengo").run()
    assert not app.exception
    assert "Escolha dois clubes diferentes." in textos(app)

    app.selectbox(key="clube_a").set_value("Barueri").run()
    app.selectbox(key="clube_b").set_value("Grêmio Prudente").run()
    assert not app.exception
    assert "não se enfrentaram" in textos(app)
    assert "Barueri não tem nenhum adversário" in textos(app)


def test_clube_sem_vitoria_no_confronto_nao_quebra():
    confrontos = ler("confrontos")
    sem_vitoria = confrontos[confrontos["vitorias"] == 0].iloc[0]
    app = abrir("confronto")

    app.selectbox(key="clube_a").set_value(sem_vitoria["clube"]).run()
    app.selectbox(key="clube_b").set_value(sem_vitoria["adversario"]).run()

    assert not app.exception
    assert f"Maior vitória de {sem_vitoria['clube']}: nenhuma" in textos(app)


def test_viradas_filtra_por_ano_e_soma_os_anos():
    app = abrir("viradas")
    viradas = ler("viradas")

    # A conta do fim de jogo soma os 12 anos; a de 2025 fica só com os 20 clubes dele.
    assert app.dataframe[1].value["Jogos"].sum() == ler("gols_decisivos")["jogos"].sum()
    assert app.dataframe[0].value["Chegou a perder por"].iloc[0] == (
        viradas["maior_desvantagem"].max()
    )

    app.selectbox(key="ano_viradas").set_value(2025).run()
    assert not app.exception
    assert len(app.dataframe[1].value) == 20
    assert (app.dataframe[0].value["Data"].map(lambda d: d.year) == 2025).all()


def test_viradas_ordena_e_trata_acrescimo_como_piso():
    app = abrir("viradas")
    assert "pelo menos" in textos(app)

    app.radio(key="ordem_decisivos").set_value("Mais pontos perdidos").run()

    assert not app.exception
    assert app.dataframe[1].value["Pontos perdidos"].is_monotonic_decreasing


def test_rebaixamento_compara_por_pontos_ou_aproveitamento():
    app = abrir("rebaixamento")
    linha_corte = app.dataframe[0].value
    assert len(linha_corte) == 23
    caiu = app.dataframe[1].value
    assert caiu["Pts"].is_monotonic_decreasing
    assert (caiu["Clube"].iloc[0], caiu["Ano"].iloc[0]) == ("Criciúma", 2004)

    app.radio(key="criterio").set_value("Aproveitamento").run()

    assert not app.exception
    assert app.dataframe[1].value["Aprov."].is_monotonic_decreasing
    assert app.dataframe[4].value["Aprov."].is_monotonic_increasing
    assert "Quem caiu com o melhor aproveitamento?" in textos(app)


def test_goleadas_filtra_por_ano_e_clube():
    app = abrir("goleadas")
    maiores, mais_gols = app.dataframe[0].value, app.dataframe[1].value
    assert maiores["Jogo"].iloc[0] == "Flamengo 8 × 0 Vitória"
    assert mais_gols["Jogo"].iloc[0] == "Bahia 4 × 7 Santos"

    app.selectbox(key="ano_goleadas").set_value(2023).run()
    app.selectbox(key="clube_goleadas").set_value("Bahia").run()
    assert not app.exception
    assert app.dataframe[1].value["Jogo"].iloc[0] == "Goiás 4 × 6 Bahia"
    assert app.dataframe[1].value["Jogo"].str.contains("Bahia").all()

    app.selectbox(key="ano_goleadas").set_value(2003).run()
    app.selectbox(key="clube_goleadas").set_value("Mirassol").run()
    assert not app.exception
    assert "Nenhum jogo desse tipo" in textos(app)


def test_danca_dos_tecnicos_ordena_por_tecnicos_diferentes():
    app = abrir("tecnicos")
    danca = app.dataframe[0].value
    assert danca["Técnicos diferentes"].is_monotonic_decreasing
    assert (danca["Clube"].iloc[0], danca["Ano"].iloc[0]) == ("Coritiba", 2023)
    assert 2025 not in app.selectbox(key="ano_tecnicos").options

    app.selectbox(key="ano_tecnicos").set_value(2022).run()
    assert not app.exception
    danca = app.dataframe[0].value
    palmeiras = danca[danca["Clube"] == "Palmeiras"].iloc[0]
    assert (palmeiras["Técnicos diferentes"], palmeiras["Trocas"]) == (2, 8)


def test_tecnicos_e_esquemas_respeitam_o_minimo_de_jogos():
    app = abrir("tecnicos")
    assert (app.dataframe[2].value["J"] >= 50).all()
    assert (app.dataframe[3].value["J"] >= 30).all()
    assert "3-3-3-1" not in app.dataframe[3].value["Esquema"].tolist()
    assert app.dataframe[2].value["Aprov."].is_monotonic_decreasing

    app.slider(key="minimo_tecnicos_None").set_value(100).run()
    assert (app.dataframe[2].value["J"] >= 100).all()

    app.selectbox(key="ano_esquemas").set_value(2025).run()
    assert not app.exception
    esquemas = ler("esquemas_taticos")
    de_2025 = esquemas[(esquemas["temporada"] == 2025) & (esquemas["jogos"] >= 30)]
    assert app.dataframe[3].value["J"].sum() == de_2025["jogos"].sum()


def test_disciplina_filtra_e_nao_oferece_ano_sem_dado():
    app = abrir("disciplina")
    assert app.dataframe[0].value["Vermelhos"].is_monotonic_decreasing
    assert 2025 not in app.selectbox(key="ano_cartoes").options
    assert 2024 not in app.selectbox(key="ano_faltas").options

    app.selectbox(key="ano_cartoes").set_value(2024).run()
    app.selectbox(key="ano_faltas").set_value(2019).run()

    assert not app.exception
    cartoes = ler("cartoes_jogadores")
    assert app.dataframe[0].value["Vermelhos"].iloc[0] == (
        cartoes[cartoes["temporada"] == 2024].groupby("atleta")["vermelhos"].sum().max()
    )
