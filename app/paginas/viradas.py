import streamlit as st
from comum import barras, desenhar, filtro_ano, ler, rodape, tabela

ORDENS = {
    "Mais pontos ganhos": "Pontos ganhos",
    "Mais pontos perdidos": "Pontos perdidos",
    "Mais gols": "Gols depois dos 85'",
}

st.title("Viradas e fim de jogo")
st.write(
    "De 2014 a 2025, porque antes disso a fonte não diz em que minuto saiu cada gol. "
    "Virada aqui é o time ter ficado atrás no placar em algum momento e ter vencido o jogo."
)

viradas = ler("viradas")
decisivos = ler("gols_decisivos")
ano = filtro_ano(viradas["temporada"], "ano_viradas")
if ano is not None:
    viradas = viradas[viradas["temporada"] == ano]
    decisivos = decisivos[decisivos["temporada"] == ano]

vira, toma = st.columns(2)
with vira:
    st.subheader("Quem mais vira?")
    contagem = viradas.groupby("clube", as_index=False).size().rename(columns={"size": "viradas"})
    contagem = contagem.sort_values(["viradas", "clube"], ascending=[False, True]).head(10)
    desenhar(barras(contagem, "clube", "viradas"), "Os dez clubes com mais viradas.")
with toma:
    st.subheader("Quem mais toma virada?")
    contagem = (
        viradas.groupby("adversario", as_index=False).size().rename(columns={"size": "viradas"})
    )
    contagem = contagem.sort_values(["viradas", "adversario"], ascending=[False, True]).head(10)
    desenhar(barras(contagem, "adversario", "viradas"), "Os dez clubes que mais tomaram virada.")
if ano is None:
    st.caption("A conta soma os 12 anos, então quem disputou mais edições sai na frente.")

st.subheader("Quais foram as maiores viradas?")
maiores = viradas.sort_values(
    ["maior_desvantagem", "gols_pro", "data"], ascending=[False, False, False]
).head(10)
maiores = maiores.assign(
    Placar=maiores["gols_pro"].astype(str) + " × " + maiores["gols_contra"].astype(str),
    Onde=maiores["mando"].map({"mandante": "Em casa", "visitante": "Fora"}),
)
tabela(
    maiores[
        ["data", "clube", "adversario", "Placar", "maior_desvantagem", "Onde", "rodada"]
    ].rename(
        columns={
            "data": "Data",
            "clube": "Quem virou",
            "adversario": "Contra",
            "maior_desvantagem": "Chegou a perder por",
            "rodada": "Rodada",
        }
    ),
    datas=["Data"],
)
st.caption("O placar está na visão de quem virou, com os gols dele primeiro.")

st.subheader("O que acontece depois dos 85 minutos?")
st.write(
    "A conta compara o placar final com o placar de antes dos 85. Se o time empatava e fez o gol "
    "da vitória aos 88, ganhou 2 pontos. Se ganhava e tomou o empate, perdeu 2."
)
criterio = st.radio("Ordenar por", list(ORDENS), horizontal=True, key="ordem_decisivos")
somas = decisivos.groupby("clube", as_index=False)[
    [
        "jogos",
        "gols_85",
        "pontos_ganhos_85",
        "pontos_perdidos_85",
        "gols_acrescimo",
        "pontos_ganhos_acrescimo",
        "pontos_perdidos_acrescimo",
    ]
].sum()
somas = somas.rename(
    columns={
        "clube": "Clube",
        "jogos": "Jogos",
        "gols_85": "Gols depois dos 85'",
        "pontos_ganhos_85": "Pontos ganhos",
        "pontos_perdidos_85": "Pontos perdidos",
        "gols_acrescimo": "Gols nos acréscimos (pelo menos)",
        "pontos_ganhos_acrescimo": "Pontos ganhos nos acréscimos",
        "pontos_perdidos_acrescimo": "Pontos perdidos nos acréscimos",
    }
)
tabela(
    somas.sort_values([ORDENS[criterio], "Clube"], ascending=[False, True]),
    fixar=["Clube"],
    max_linhas=15,
)
st.caption(
    "Acréscimos são pelo menos esse tanto. Cerca de 15% dos gols no minuto 90 não dizem se saíram "
    "nos acréscimos, então o número real pode ser um pouco maior."
)

rodape()
