"""Vitrine do Brasileirão: só lê os Parquet de `app/dados`, gerados por `make exportar`."""

import streamlit as st

st.set_page_config(page_title="Brasileirão de 2003 a 2025", layout="wide")

PAGINAS = [
    st.Page("paginas/temporada.py", title="Temporada", default=True),
    st.Page("paginas/historico.py", title="Histórico", url_path="historico"),
    st.Page("paginas/confronto.py", title="Confronto direto", url_path="confronto"),
    st.Page("paginas/viradas.py", title="Viradas e fim de jogo", url_path="viradas"),
    st.Page("paginas/rebaixamento.py", title="Rebaixamento e campanhas", url_path="rebaixamento"),
    st.Page("paginas/goleadas.py", title="Goleadas", url_path="goleadas"),
    st.Page("paginas/tecnicos.py", title="Técnicos e esquemas", url_path="tecnicos"),
    st.Page("paginas/disciplina.py", title="Disciplina", url_path="disciplina"),
]

st.navigation(PAGINAS).run()
