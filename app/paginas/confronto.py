import pandas as pd
import streamlit as st
from comum import NOTA_BARUERI, data_br, ler, nomes_em_ordem, pct, rodape, tabela

MINIMO_JOGOS = 10
COLUNAS = {
    "adversario": "Adversário",
    "jogos": "J",
    "vitorias": "V",
    "empates": "E",
    "derrotas": "D",
    "aproveitamento": "Aprov.",
}


def _placar(texto):
    return texto if pd.notna(texto) else "nenhuma"


def _fregueses_e_carrascos(confrontos, clube):
    """Freguês é quem o clube mais bate. Carrasco é quem mais bate nele."""
    jogos = confrontos[(confrontos["clube"] == clube) & (confrontos["jogos"] >= MINIMO_JOGOS)]
    st.subheader(clube)
    if jogos.empty:
        st.info(f"{clube} não tem nenhum adversário com {MINIMO_JOGOS} jogos ou mais.")
        return
    jogos = jogos[list(COLUNAS)].rename(columns=COLUNAS)
    st.markdown("##### Fregueses")
    fregueses = jogos.sort_values(["Aprov.", "J"], ascending=[False, False]).head(5)
    tabela(fregueses, pcts=["Aprov."])
    st.markdown("##### Carrascos")
    carrascos = jogos.sort_values(["Aprov.", "J"], ascending=[True, False]).head(5)
    tabela(carrascos, pcts=["Aprov."])


st.title("Confronto direto")
st.write(
    "Escolha dois clubes e veja como foi cada encontro deles de 2003 a 2025, só na Série A, "
    "que é o que a base cobre."
)

confrontos = ler("confrontos")
clubes = nomes_em_ordem(confrontos["clube"])
coluna_a, coluna_b = st.columns(2)
with coluna_a:
    clube_a = st.selectbox("Clube", clubes, index=clubes.index("Flamengo"), key="clube_a")
with coluna_b:
    clube_b = st.selectbox("Contra", clubes, index=clubes.index("Vasco"), key="clube_b")

encontro = confrontos[(confrontos["clube"] == clube_a) & (confrontos["adversario"] == clube_b)]
if clube_a == clube_b:
    st.info("Escolha dois clubes diferentes.")
elif encontro.empty:
    st.info(f"{clube_a} e {clube_b} não se enfrentaram na Série A entre 2003 e 2025.")
else:
    a = encontro.iloc[0]
    b = confrontos[(confrontos["clube"] == clube_b) & (confrontos["adversario"] == clube_a)].iloc[0]
    st.subheader(f"{clube_a} × {clube_b}, em {a['jogos']} jogos")
    vitorias_a, empates, vitorias_b = st.columns(3)
    vitorias_a.metric(f"{clube_a} venceu", a["vitorias"])
    empates.metric("Empates", a["empates"])
    vitorias_b.metric(f"{clube_b} venceu", a["derrotas"])
    st.markdown(
        f"- Gols: {clube_a} {a['gols_pro']}, {clube_b} {a['gols_contra']}\n"
        f"- Aproveitamento: {clube_a} {pct(a['aproveitamento'])}, "
        f"{clube_b} {pct(b['aproveitamento'])}\n"
        f"- Maior vitória de {clube_a}: {_placar(a['maior_vitoria'])}\n"
        f"- Maior vitória de {clube_b}: {_placar(b['maior_vitoria'])}\n"
        f"- Último jogo: {a['ultimo_jogo']}, em {data_br(a['data_ultimo_jogo'])}"
    )

st.divider()
st.write(
    f"Freguês é o adversário contra quem o clube tem o melhor aproveitamento, e carrasco é "
    f"aquele contra quem ele tem o pior. Só entra quem já jogou {MINIMO_JOGOS} vezes ou mais "
    "contra o clube, para um jogo isolado não virar freguesia."
)
coluna_a, coluna_b = st.columns(2)
with coluna_a:
    _fregueses_e_carrascos(confrontos, clube_a)
with coluna_b:
    if clube_b != clube_a:
        _fregueses_e_carrascos(confrontos, clube_b)

st.caption(NOTA_BARUERI)
rodape()
