import altair as alt
import streamlit as st
from comum import AZUL, LARANJA, desenhar, ler, pct, rodape, tabela

COLUNAS = {
    "temporada": "Ano",
    "clube": "Clube",
    "posicao": "Pos.",
    "pontos": "Pts",
    "aproveitamento": "Aprov.",
    "jogos": "J",
    "vitorias": "V",
    "empates": "E",
    "derrotas": "D",
    "ajuste_pontos": "Punição ou bônus",
}


def _linha_corte(corte):
    nomes = {
        "aproveitamento_ultimo_salvo": "Último a se salvar",
        "aproveitamento_primeiro_rebaixado": "Primeiro a cair",
    }
    longa = corte.melt(
        id_vars=["temporada", "ultimo_salvo", "primeiro_rebaixado"],
        value_vars=list(nomes),
        var_name="serie",
        value_name="aproveitamento",
    )
    longa["Quem"] = longa["serie"].map(nomes)
    longa["Clube"] = longa["ultimo_salvo"].where(
        longa["serie"] == "aproveitamento_ultimo_salvo", longa["primeiro_rebaixado"]
    )
    longa["texto"] = longa["aproveitamento"].map(pct)
    return (
        alt.Chart(longa)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=28))
        .encode(
            x=alt.X("temporada:O", title=None, axis=alt.Axis(labelAngle=-90)),
            y=alt.Y(
                "aproveitamento:Q",
                title="Aproveitamento",
                scale=alt.Scale(zero=False),
                axis=alt.Axis(format=".0%"),
            ),
            color=alt.Color(
                "Quem:N",
                scale=alt.Scale(domain=list(nomes.values()), range=[AZUL, LARANJA]),
                legend=alt.Legend(orient="top", title=None),
            ),
            tooltip=[
                alt.Tooltip("temporada:O", title="Ano"),
                alt.Tooltip("Quem:N"),
                alt.Tooltip("Clube:N"),
                alt.Tooltip("texto:N", title="Aproveitamento"),
            ],
        )
        .properties(height=320)
    )


def _top(df, coluna, crescente=False, linhas=10):
    return df.sort_values(
        [coluna, "aproveitamento" if coluna == "pontos" else "pontos", "temporada"],
        ascending=[crescente, crescente, False],
    ).head(linhas)[list(COLUNAS)]


st.title("Rebaixamento e campanhas")
st.write(
    "De 2003 a 2025. Os campeonatos não tiveram sempre o mesmo tamanho, foram de 38 a 46 rodadas. "
    "Por isso o aproveitamento, a parte dos pontos possíveis que o time ganhou, é o que permite "
    "comparar um ano com outro."
)

corte = ler("linha_corte")
campanhas = ler("campanhas")

st.subheader("Onde passou a linha de rebaixamento em cada ano?")
desenhar(
    _linha_corte(corte),
    "Aproveitamento do último a se salvar e do primeiro a cair em cada ano.",
)
tabela(
    corte.sort_values("temporada", ascending=False)[
        [
            "temporada",
            "ultimo_salvo",
            "pontos_ultimo_salvo",
            "aproveitamento_ultimo_salvo",
            "primeiro_rebaixado",
            "pontos_primeiro_rebaixado",
            "aproveitamento_primeiro_rebaixado",
            "pontos_campeao",
            "pontos_vice",
        ]
    ].rename(
        columns={
            "temporada": "Ano",
            "ultimo_salvo": "Último a se salvar",
            "pontos_ultimo_salvo": "Pts do salvo",
            "aproveitamento_ultimo_salvo": "Aprov. do salvo",
            "primeiro_rebaixado": "Primeiro a cair",
            "pontos_primeiro_rebaixado": "Pts de quem caiu",
            "aproveitamento_primeiro_rebaixado": "Aprov. de quem caiu",
            "pontos_campeao": "Pts do campeão",
            "pontos_vice": "Pts do vice",
        }
    ),
    pcts=["Aprov. do salvo", "Aprov. de quem caiu"],
    max_linhas=10,
)

st.subheader("Recordes de campanha")
criterio = st.radio("Comparar por", ["Pontos", "Aproveitamento"], horizontal=True, key="criterio")
coluna = "pontos" if criterio == "Pontos" else "aproveitamento"
if criterio == "Pontos":
    st.caption(
        "Em 2003 e 2004 foram 46 rodadas, e em 2005 foram 42. Esses anos saem na frente quando "
        "a conta é por pontos."
    )
rebaixados = campanhas[campanhas["situacao"] == "rebaixado"]
vices = campanhas[campanhas["situacao"] == "vice"]
medida = "mais pontos" if criterio == "Pontos" else "o melhor aproveitamento"
blocos = [
    (f"Quem caiu com {medida}?", _top(rebaixados, coluna)),
    (f"Quem foi vice com {medida}?", _top(vices, coluna)),
    ("Quais foram as melhores campanhas?", _top(campanhas, coluna)),
    ("Quais foram as piores campanhas?", _top(campanhas, coluna, crescente=True)),
]
for titulo, dados in blocos:
    st.markdown(f"##### {titulo}")
    tabela(
        dados.rename(columns=COLUNAS),
        pcts=["Aprov."],
        zero_vazio=["Punição ou bônus"],
        fixar=["Clube"],
    )
st.caption(
    "A última coluna mostra os pontos que o STJD tirou ou deu naquele ano, e os pontos já vêm com "
    "esse acerto. A Portuguesa de 2013, por exemplo, caiu com 4 pontos a menos."
)

rodape()
