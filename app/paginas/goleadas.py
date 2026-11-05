import streamlit as st
from comum import filtro_ano, ler, nomes_em_ordem, rodape, tabela

TODOS = "Todos os clubes"
COLUNAS = ["data", "Jogo", "diferenca", "total_gols", "rodada", "arena"]
NOMES = {
    "data": "Data",
    "diferenca": "Diferença",
    "total_gols": "Gols no jogo",
    "rodada": "Rodada",
    "arena": "Estádio",
}

st.title("Goleadas")
st.write(
    "De 2003 a 2025. Entra aqui o jogo com diferença de 4 gols ou mais, ou com 8 gols ou mais "
    "somando os dois times."
)

goleadas = ler("goleadas")
coluna_ano, coluna_clube = st.columns(2)
with coluna_ano:
    ano = filtro_ano(goleadas["temporada"], "ano_goleadas")
with coluna_clube:
    clubes = nomes_em_ordem([*goleadas["mandante"], *goleadas["visitante"]])
    clube = st.selectbox("Clube", [TODOS, *clubes], key="clube_goleadas")

if ano is not None:
    goleadas = goleadas[goleadas["temporada"] == ano]
if clube != TODOS:
    goleadas = goleadas[(goleadas["mandante"] == clube) | (goleadas["visitante"] == clube)]

if goleadas.empty:
    st.info("Nenhum jogo desse tipo com esse filtro.")
else:
    goleadas = goleadas.assign(
        Jogo=goleadas["mandante"]
        + " "
        + goleadas["placar_mandante"].astype(str)
        + " × "
        + goleadas["placar_visitante"].astype(str)
        + " "
        + goleadas["visitante"]
    )
    st.subheader("Quais foram as maiores goleadas?")
    maiores = goleadas.sort_values(
        ["diferenca", "total_gols", "data"], ascending=[False, False, False]
    ).head(15)
    tabela(maiores[COLUNAS].rename(columns=NOMES), datas=["Data"], fixar=["Jogo"])

    st.subheader("Quais jogos tiveram mais gols?")
    mais_gols = goleadas.sort_values(
        ["total_gols", "diferenca", "data"], ascending=[False, False, False]
    ).head(15)
    tabela(mais_gols[COLUNAS].rename(columns=NOMES), datas=["Data"], fixar=["Jogo"])
    st.caption(f"São {len(goleadas)} jogos nesse filtro. A lista mostra os 15 primeiros.")

rodape()
