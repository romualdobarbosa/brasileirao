import streamlit as st
from comum import barras, desenhar, filtro_ano, ler, nomes_em_ordem, pct, rodape, tabela


def _aproveitamento(df):
    return (3 * df["vitorias"] + df["empates"]) / (3 * df["jogos"])


def _sem_repetir(nomes):
    return ", ".join(dict.fromkeys(nomes))


st.title("Técnicos e esquemas")
st.write(
    "Técnicos vão de 2014 a 2024. Em 2025 a fonte está errada, em 19 dos 20 clubes um nome só "
    "aparece em 36 ou 37 jogos. O Cruzeiro, por exemplo, aparece com o Tite em 36 jogos, e ele só "
    "foi anunciado depois do fim do campeonato. Esquemas vão de 2015 a 2025, porque em 2014 "
    "quase nenhum jogo traz a formação."
)

passagens = ler("passagens_tecnicos")
esquemas = ler("esquemas_taticos")

st.subheader("A dança dos técnicos")
ano = filtro_ano(passagens["temporada"], "ano_tecnicos")
do_recorte = passagens if ano is None else passagens[passagens["temporada"] == ano]
ordenadas = do_recorte.sort_values(["temporada", "clube", "ordem"])
danca = (
    ordenadas.groupby(["temporada", "clube"], as_index=False)
    .agg(
        tecnicos=("tecnico", "nunique"),
        passagens=("ordem", "count"),
        quem=("tecnico", _sem_repetir),
    )
    .assign(trocas=lambda d: d["passagens"] - 1)
    .sort_values(
        ["tecnicos", "trocas", "temporada", "clube"], ascending=[False, False, False, True]
    )
)
tabela(
    danca.head(20)[["temporada", "clube", "tecnicos", "trocas", "quem"]].rename(
        columns={
            "temporada": "Ano",
            "clube": "Clube",
            "tecnicos": "Técnicos diferentes",
            "trocas": "Trocas",
            "quem": "Quem passou",
        }
    ),
    fixar=["Clube"],
)
st.caption(
    "A lista segue o número de técnicos diferentes, porque as trocas sozinhas enganam. Toda vez "
    "que o nome muda conta como troca, até auxiliar num jogo isolado. O Palmeiras de 2022 teve 8 "
    "trocas com 2 técnicos. Alguns técnicos ainda aparecem com o nome de registro, como vêm na "
    "fonte."
)

clubes = nomes_em_ordem(do_recorte["clube"])
clube = st.selectbox("Ver as passagens de um clube", clubes, key="clube_passagens")
do_clube = do_recorte[do_recorte["clube"] == clube].sort_values(
    ["temporada", "ordem"], ascending=[False, True]
)
tabela(
    do_clube[
        [
            "temporada",
            "tecnico",
            "data_inicio",
            "data_fim",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "aproveitamento",
        ]
    ].rename(
        columns={
            "temporada": "Ano",
            "tecnico": "Técnico",
            "data_inicio": "Início",
            "data_fim": "Fim",
            "jogos": "J",
            "vitorias": "V",
            "empates": "E",
            "derrotas": "D",
            "aproveitamento": "Aprov.",
        }
    ),
    pcts=["Aprov."],
    datas=["Início", "Fim"],
    max_linhas=12,
)

st.subheader("Qual técnico tem o melhor aproveitamento?")
maximo = 100 if ano is None else 30
minimo_jogos = st.slider(
    "Mínimo de jogos", 5, maximo, 50 if ano is None else 15, step=5, key=f"minimo_tecnicos_{ano}"
)
por_tecnico = do_recorte.groupby("tecnico", as_index=False).agg(
    jogos=("jogos", "sum"),
    vitorias=("vitorias", "sum"),
    empates=("empates", "sum"),
    derrotas=("derrotas", "sum"),
    clubes=("clube", lambda c: ", ".join(nomes_em_ordem(c))),
)
por_tecnico = por_tecnico[por_tecnico["jogos"] >= minimo_jogos]
por_tecnico["aproveitamento"] = _aproveitamento(por_tecnico)
if por_tecnico.empty:
    st.info("Nenhum técnico chega a esse número de jogos nesse ano.")
else:
    tabela(
        por_tecnico.sort_values(["aproveitamento", "jogos"], ascending=[False, False])
        .head(20)[
            ["tecnico", "aproveitamento", "jogos", "vitorias", "empates", "derrotas", "clubes"]
        ]
        .rename(
            columns={
                "tecnico": "Técnico",
                "aproveitamento": "Aprov.",
                "jogos": "J",
                "vitorias": "V",
                "empates": "E",
                "derrotas": "D",
                "clubes": "Clubes",
            }
        ),
        pcts=["Aprov."],
        fixar=["Técnico"],
    )

st.subheader("Quais esquemas os times usam?")
ano_esquema = filtro_ano(esquemas["temporada"], "ano_esquemas")
recorte = esquemas if ano_esquema is None else esquemas[esquemas["temporada"] == ano_esquema]
por_esquema = recorte.groupby("esquema", as_index=False)[
    ["jogos", "vitorias", "empates", "derrotas"]
].sum()
por_esquema["uso"] = por_esquema["jogos"] / por_esquema["jogos"].sum()
por_esquema["aproveitamento"] = _aproveitamento(por_esquema)
por_esquema["uso_texto"] = por_esquema["uso"].map(pct)
mais_usados = por_esquema.sort_values(["uso", "esquema"], ascending=[False, True]).head(8)
desenhar(barras(mais_usados, "esquema", "uso", rotulo="uso_texto"), "Os esquemas mais usados.")
st.caption("Cada jogo conta duas vezes, uma para cada time.")

st.subheader("Qual esquema rende mais?")
minimo_esquema = st.slider(
    "Mínimo de jogos", 10, 100, 30, step=10, key=f"minimo_esquemas_{ano_esquema}"
)
com_jogos = por_esquema[por_esquema["jogos"] >= minimo_esquema]
if com_jogos.empty:
    st.info("Nenhum esquema chega a esse número de jogos nesse ano.")
else:
    tabela(
        com_jogos.sort_values(["aproveitamento", "jogos"], ascending=[False, False])[
            ["esquema", "aproveitamento", "jogos", "vitorias", "empates", "derrotas"]
        ].rename(
            columns={
                "esquema": "Esquema",
                "aproveitamento": "Aprov.",
                "jogos": "J",
                "vitorias": "V",
                "empates": "E",
                "derrotas": "D",
            }
        ),
        pcts=["Aprov."],
    )
st.caption(
    "Esquema com poucos jogos entra e sai do ranking por acaso, daí o mínimo. Formações estranhas, "
    "como 4-2-4-0, vêm assim da fonte."
)

rodape()
