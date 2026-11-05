import altair as alt
import pandas as pd
import streamlit as st
from comum import (
    AMARELO,
    AZUL,
    LARANJA,
    NOTA_BARUERI,
    VERMELHO,
    barras,
    desenhar,
    ler,
    pct,
    rodape,
    tabela,
    virgula,
)

CINZA_ESCURO = "#7a7974"


def _linha_por_ano(dados, coluna, titulo, formato, cor=AZUL):
    dados = dados.assign(texto=dados[coluna].map(lambda v: f"{v:.2f}".replace(".", ",")))
    return (
        alt.Chart(dados)
        .mark_line(color=cor, strokeWidth=2, point=alt.OverlayMarkDef(size=28, color=cor))
        .encode(
            x=alt.X("temporada:O", title=None, axis=alt.Axis(labelAngle=-90)),
            y=alt.Y(
                f"{coluna}:Q",
                title=titulo,
                scale=alt.Scale(zero=False),
                axis=alt.Axis(labelExpr=virgula(formato)),
            ),
            tooltip=[
                alt.Tooltip("temporada:O", title="Ano"),
                alt.Tooltip("texto:N", title=titulo),
            ],
        )
        .properties(height=280)
    )


def _mando_por_ano(resumo):
    nomes = {
        "pct_vitoria_mandante": "Vitória em casa",
        "pct_empate": "Empate",
        "pct_vitoria_visitante": "Vitória fora",
    }
    longa = resumo.melt(
        id_vars="temporada", value_vars=list(nomes), var_name="serie", value_name="pct"
    )
    longa["Resultado"] = longa["serie"].map(nomes)
    longa["texto"] = longa["pct"].map(pct)
    x = alt.X("temporada:O", title=None, axis=alt.Axis(labelAngle=-90))
    linhas = (
        alt.Chart(longa)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=28))
        .encode(
            x=x,
            y=alt.Y(
                "pct:Q",
                title="Parte dos jogos",
                # Folga no alto para a marca de 2020 não cair em cima das linhas.
                scale=alt.Scale(domain=[0, 0.7]),
                axis=alt.Axis(format=".0%"),
            ),
            color=alt.Color(
                "Resultado:N",
                scale=alt.Scale(domain=list(nomes.values()), range=[AZUL, CINZA_ESCURO, LARANJA]),
                legend=alt.Legend(orient="top", title=None, labelLimit=240),
            ),
            tooltip=[
                alt.Tooltip("temporada:O", title="Ano"),
                alt.Tooltip("Resultado:N"),
                alt.Tooltip("texto:N", title="Parte dos jogos"),
            ],
        )
    )
    marca = pd.DataFrame({"temporada": [2020], "texto": ["2020, sem torcida"]})
    regra = alt.Chart(marca).mark_rule(strokeDash=[4, 4], color="#52514e").encode(x=x)
    rotulo = (
        alt.Chart(marca)
        .mark_text(align="right", dx=-4, y=8, fontSize=11)
        .encode(x=x, text="texto:N")
    )
    return (linhas + regra + rotulo).properties(height=320)


st.title("Histórico")
st.write(
    "A era dos pontos corridos, de 2003 a 2025. Os cartões só começam em 2014, porque a fonte "
    "não traz cartão antes disso."
)

ranking = ler("ranking_historico")
resumo = ler("resumo_temporada")

st.subheader("Quem mais somou pontos desde 2003?")
tabela(
    ranking.sort_values("posicao")[
        [
            "posicao",
            "clube",
            "temporadas",
            "pontos",
            "titulos",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "saldo",
            "aproveitamento",
        ]
    ].rename(
        columns={
            "posicao": "Pos.",
            "clube": "Clube",
            "temporadas": "Anos",
            "pontos": "Pts",
            "titulos": "Títulos",
            "jogos": "J",
            "vitorias": "V",
            "empates": "E",
            "derrotas": "D",
            "saldo": "SG",
            "aproveitamento": "Aprov.",
        }
    ),
    pcts=["Aprov."],
    milhares=["Pts", "J"],
    fixar=["Pos.", "Clube"],
    max_linhas=15,
)
st.caption(NOTA_BARUERI)

st.subheader("Quem tem mais títulos?")
titulos = ranking[ranking["titulos"] > 0].sort_values(["titulos", "clube"], ascending=[False, True])
desenhar(
    barras(
        titulos, "clube", "titulos", dicas=["clube:N", alt.Tooltip("titulos:Q", title="Títulos")]
    ),
    "Títulos de cada clube na era dos pontos corridos.",
)

st.subheader("O mando de campo perdeu força?")
desenhar(
    _mando_por_ano(resumo),
    "Parte dos jogos com vitória do mandante, empate e vitória do visitante, por ano.",
)

st.subheader("O campeonato ficou com mais ou menos gols?")
desenhar(
    _linha_por_ano(resumo, "media_gols", "Gols por jogo", ".1f"),
    "Média de gols por jogo em cada ano.",
)

st.subheader("O jogo ficou mais violento?")
disciplina = ler("disciplina")
cartoes = disciplina.groupby("temporada", as_index=False)[["amarelos", "vermelhos", "jogos"]].sum()
# Cada jogo aparece duas vezes em `jogos`, uma para cada clube.
cartoes["amarelos_por_jogo"] = cartoes["amarelos"] / (cartoes["jogos"] / 2)
cartoes["vermelhos_por_jogo"] = cartoes["vermelhos"] / (cartoes["jogos"] / 2)
amarelos, vermelhos = st.columns(2)
with amarelos:
    desenhar(
        _linha_por_ano(cartoes, "amarelos_por_jogo", "Amarelos por jogo", ".1f", cor=AMARELO),
        "Cartões amarelos por jogo em cada ano.",
    )
with vermelhos:
    desenhar(
        _linha_por_ano(cartoes, "vermelhos_por_jogo", "Vermelhos por jogo", ".2f", cor=VERMELHO),
        "Cartões vermelhos por jogo em cada ano.",
    )
st.caption(
    "De 2014 a 2025. O número de 2025 vem de outra tabela da fonte, que conta um pouco diferente, "
    "então vale olhar com cuidado a comparação com os anos anteriores."
)

rodape()
