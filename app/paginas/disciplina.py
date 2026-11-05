import streamlit as st
from comum import barras, decimal, desenhar, filtro_ano, ler, nomes_em_ordem, rodape, tabela

st.title("Disciplina")
st.write(
    "Cartões por jogador vão de 2014 a 2024, porque a fonte não traz esse detalhe em 2025. "
    "Faltas vão de 2015 a 2023 e voltam em 2025. A fonte não tem faltas de 2024, e em 2019 "
    "faltam alguns jogos, então olhe a média por jogo."
)

cartoes = ler("cartoes_jogadores")
faltas = ler("faltas_clubes")

st.subheader("Quem levou mais cartão vermelho?")
ano_cartoes = filtro_ano(cartoes["temporada"], "ano_cartoes")
recorte = cartoes if ano_cartoes is None else cartoes[cartoes["temporada"] == ano_cartoes]
jogadores = recorte.groupby("atleta", as_index=False).agg(
    vermelhos=("vermelhos", "sum"),
    amarelos=("amarelos", "sum"),
    clubes=("clube", lambda c: ", ".join(nomes_em_ordem(c))),
)
jogadores = jogadores.sort_values(
    ["vermelhos", "amarelos", "atleta"], ascending=[False, False, True]
).head(15)
tabela(
    jogadores.rename(
        columns={
            "atleta": "Jogador",
            "vermelhos": "Vermelhos",
            "amarelos": "Amarelos",
            "clubes": "Clubes",
        }
    ),
    fixar=["Jogador"],
)
st.caption("Alguns jogadores ainda aparecem com o nome de registro, como vêm na fonte.")

st.subheader("Quais times fazem mais faltas?")
ano_faltas = filtro_ano(faltas["temporada"], "ano_faltas")
recorte = faltas if ano_faltas is None else faltas[faltas["temporada"] == ano_faltas]
times = recorte.groupby("clube", as_index=False)[["faltas", "jogos_com_dado"]].sum()
times["por_jogo"] = times["faltas"] / times["jogos_com_dado"]
times["texto"] = times["por_jogo"].map(decimal)
mais_faltosos = times.sort_values(["por_jogo", "clube"], ascending=[False, True]).head(10)
desenhar(
    barras(
        mais_faltosos,
        "clube",
        "por_jogo",
        rotulo="texto",
        dicas=["clube:N", "texto:N", "jogos_com_dado:Q"],
    ),
    "Os dez times com mais faltas por jogo.",
)
st.caption("Faltas por jogo, contando só os jogos em que a fonte traz o número.")

rodape()
