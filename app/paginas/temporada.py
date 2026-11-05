import altair as alt
import pandas as pd
import streamlit as st
from comum import (
    AZUL,
    CINZA,
    LARANJA,
    barras,
    decimal,
    desenhar,
    filtro_ano,
    ler,
    nomes_em_ordem,
    pct,
    rodape,
    tabela,
)

NINGUEM = "Ninguém"
SEQUENCIAS = {
    "invencibilidade": "Sem perder",
    "vitorias": "Vitórias seguidas",
    "jejum": "Sem vencer",
    "derrotas": "Derrotas seguidas",
}


def _escolher_clube(rotulo, opcoes, padrao, memoria, temporada):
    """A chave muda com o ano, porque a lista de clubes muda. O clube escolhido segue valendo
    no ano seguinte quando ele jogou aquele campeonato."""
    anterior = st.session_state.get(memoria)
    indice = opcoes.index(anterior if anterior in opcoes else padrao)
    escolha = st.selectbox(rotulo, opcoes, index=indice, key=f"{memoria}_{temporada}")
    st.session_state[memoria] = escolha
    return escolha


def _curva(dados, selecionados, rodada, times, rebaixados):
    ultima = int(dados["rodada"].max())
    ticks = sorted({1, 5, 10, 15, 20, times})
    x = alt.X(
        "rodada:Q",
        title="Rodada",
        scale=alt.Scale(domain=[1, ultima], nice=False),
        axis=alt.Axis(grid=False, values=list(range(5, ultima + 1, 5))),
    )
    # Domínio de trás pra frente: é o que põe o líder no alto.
    y = alt.Y(
        "posicao:Q",
        title="Posição",
        scale=alt.Scale(domain=[times + 0.5, 0.5], nice=False),
        axis=alt.Axis(values=ticks, gridOpacity=0.4),
    )
    dica = [
        alt.Tooltip("clube:N", title="Clube"),
        alt.Tooltip("rodada:Q", title="Rodada"),
        alt.Tooltip("posicao:Q", title="Posição"),
        alt.Tooltip("pontos:Q", title="Pontos"),
    ]
    resto = dados[~dados["clube"].isin(selecionados)]
    destaque = dados[dados["clube"].isin(selecionados)]
    cor = alt.Color(
        "clube:N",
        scale=alt.Scale(domain=selecionados, range=[AZUL, LARANJA][: len(selecionados)]),
        legend=alt.Legend(orient="top", title=None),
    )
    camadas = [
        alt.Chart(resto)
        .mark_line(color=CINZA, strokeWidth=1, opacity=0.45)
        .encode(x=x, y=y, detail="clube:N", tooltip=dica),
        alt.Chart(destaque)
        .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=28))
        .encode(x=x, y=y, color=cor, tooltip=dica),
    ]
    zona = pd.DataFrame({"posicao": [times - rebaixados + 0.5], "rotulo": ["Zona de rebaixamento"]})
    camadas.append(alt.Chart(zona).mark_rule(strokeDash=[4, 4], color=LARANJA).encode(y=y))
    camadas.append(
        alt.Chart(zona)
        .mark_text(align="left", baseline="top", dx=4, dy=3, fontSize=11)
        .encode(y=y, x=alt.value(0), text="rotulo:N")
    )
    if rodada < ultima:
        marca = pd.DataFrame({"rodada": [rodada]})
        camadas.append(alt.Chart(marca).mark_rule(strokeDash=[2, 2], color="#52514e").encode(x=x))
    return alt.layer(*camadas).properties(height=460)


def _mando(dados):
    largura = dados.pivot(index="clube", columns="mando", values="aproveitamento").reset_index()
    largura = largura.sort_values(["mandante", "clube"], ascending=[False, True])
    ordem = largura["clube"].tolist()
    y = alt.Y("clube:N", sort=ordem, title=None, axis=alt.Axis(labelLimit=170, ticks=False))
    eixo_x = alt.X(
        "aproveitamento:Q",
        title="Aproveitamento",
        scale=alt.Scale(domain=[0, 1]),
        axis=alt.Axis(format=".0%", orient="top"),
    )
    ligacao = (
        alt.Chart(largura)
        .mark_rule(color=CINZA, strokeWidth=2)
        .encode(y=y, x=alt.X("mandante:Q", scale=alt.Scale(domain=[0, 1])), x2="visitante:Q")
    )
    pontos = dados.assign(
        Onde=dados["mando"].map({"mandante": "Em casa", "visitante": "Fora"}),
        texto=dados["aproveitamento"].map(pct),
    )
    bolas = (
        alt.Chart(pontos)
        .mark_circle(size=90, opacity=1)
        .encode(
            y=y,
            x=eixo_x,
            color=alt.Color(
                "Onde:N",
                scale=alt.Scale(domain=["Em casa", "Fora"], range=[AZUL, LARANJA]),
                legend=alt.Legend(orient="top", title=None),
            ),
            tooltip=[
                alt.Tooltip("clube:N", title="Clube"),
                alt.Tooltip("Onde:N"),
                alt.Tooltip("texto:N", title="Aproveitamento"),
                alt.Tooltip("jogos:Q", title="Jogos"),
            ],
        )
    )
    return (ligacao + bolas).properties(height=26 * len(largura) + 40)


def _minutos(dados):
    dados = dados.assign(Lado=dados["lado"].map({"mandante": "Mandante", "visitante": "Visitante"}))
    return (
        alt.Chart(dados)
        .mark_bar(size=18, cornerRadiusEnd=4)
        .encode(
            x=alt.X("faixa:N", title="Minuto do jogo", axis=alt.Axis(labelAngle=0)),
            xOffset="Lado:N",
            y=alt.Y("gols:Q", title="Gols"),
            color=alt.Color(
                "Lado:N",
                scale=alt.Scale(domain=["Mandante", "Visitante"], range=[AZUL, LARANJA]),
                legend=alt.Legend(orient="top", title=None),
            ),
            tooltip=[
                alt.Tooltip("faixa:N", title="Minuto"),
                alt.Tooltip("Lado:N"),
                alt.Tooltip("gols:Q", title="Gols"),
            ],
        )
        .properties(height=320)
    )


st.title("Temporada")
st.write(
    "Escolha o ano e a rodada para ver como estava a tabela. A página cobre de 2003 a 2025. "
    "Artilharia e minuto dos gols só começam em 2014, porque antes disso a fonte não traz os gols."
)
st.caption("No celular, as outras análises ficam no menu da setinha, lá em cima à esquerda.")

classificacao = ler("classificacao_por_rodada")
resumo = ler("resumo_temporada")

coluna_ano, coluna_rodada = st.columns([1, 2])
with coluna_ano:
    temporada = filtro_ano(classificacao["temporada"], "temporada", com_todos=False, padrao=2025)
do_ano = classificacao[classificacao["temporada"] == temporada]
ultima_rodada = int(do_ano["rodada"].max())
with coluna_rodada:
    rodada = st.slider("Rodada", 1, ultima_rodada, ultima_rodada, key=f"rodada_{temporada}")

info = resumo[resumo["temporada"] == temporada].iloc[0]
nomes_rebaixados = info["rebaixados"]
campeao, vice, gols = st.columns(3)
campeao.metric("Campeão", info["campeao"])
vice.metric("Vice", info["vice"])
gols.metric("Gols por jogo", decimal(info["media_gols"]))
st.write(f"Caíram {' e '.join(nomes_rebaixados.rsplit(', ', 1))}.")
if temporada == 2016:
    st.caption(
        "Chapecoense × Atlético-MG, da última rodada, não foi disputado, depois do acidente da "
        "Chapecoense. Por isso 2016 tem 379 jogos."
    )

st.subheader("Como estava a tabela nessa rodada?")
na_rodada = do_ano[do_ano["rodada"] == rodada].sort_values("posicao")
tabela(
    na_rodada[
        [
            "posicao",
            "clube",
            "pontos",
            "jogos",
            "vitorias",
            "empates",
            "derrotas",
            "gols_pro",
            "gols_contra",
            "saldo",
            "aproveitamento",
            "ultimos_5",
        ]
    ].rename(
        columns={
            "posicao": "Pos.",
            "clube": "Clube",
            "pontos": "Pts",
            "jogos": "J",
            "vitorias": "V",
            "empates": "E",
            "derrotas": "D",
            "gols_pro": "GP",
            "gols_contra": "GC",
            "saldo": "SG",
            "aproveitamento": "Aprov.",
            "ultimos_5": "Últimos 5",
        }
    ),
    pcts=["Aprov."],
    fixar=["Pos.", "Clube"],
)
st.caption(
    "Nos últimos 5, V é vitória, E é empate e D é derrota, com o jogo mais recente à direita. "
    "Punição do STJD só entra nos pontos da última rodada. Antes dela, vale o resultado de campo."
)

st.subheader("Como a posição mudou ao longo do campeonato?")
clubes = nomes_em_ordem(do_ano["clube"])
coluna_time, coluna_outro = st.columns(2)
with coluna_time:
    meu_time = _escolher_clube("Seu time", clubes, info["campeao"], "meu_time", temporada)
with coluna_outro:
    outro = _escolher_clube(
        "Comparar com", [NINGUEM, *clubes], info["vice"], "outro_time", temporada
    )
selecionados = [meu_time] if outro in (NINGUEM, meu_time) else [meu_time, outro]
desenhar(
    _curva(
        do_ano,
        selecionados,
        rodada,
        int(info["times"]),
        len(nomes_rebaixados.split(", ")),
    ),
    f"Posição de {' e '.join(selecionados)} rodada a rodada em {temporada}, com o líder no alto.",
)

st.subheader("Quem fez mais gols?")
artilharia = ler("artilharia")
artilharia = artilharia[(artilharia["temporada"] == temporada) & (artilharia["posicao"] <= 10)]
if artilharia.empty:
    st.info("A fonte só traz os gols a partir de 2014.")
else:
    artilharia = artilharia.sort_values(["posicao", "atleta"])
    desenhar(
        barras(
            artilharia,
            "atleta",
            "gols",
            dicas=[
                alt.Tooltip("atleta:N", title="Jogador"),
                alt.Tooltip("clubes:N", title="Clube"),
                alt.Tooltip("gols:Q", title="Gols"),
                alt.Tooltip("penaltis:Q", title="De pênalti"),
            ],
        ),
        f"Os maiores artilheiros de {temporada}.",
    )

st.subheader("Quem rende mais em casa e fora?")
st.caption(
    "Aproveitamento é a parte dos pontos possíveis que o time ganhou. "
    "Aqui vale o resultado de campo, sem punições."
)
mando = ler("desempenho_mandante_visitante")
desenhar(
    _mando(mando[mando["temporada"] == temporada]),
    f"Aproveitamento em casa e fora de cada clube em {temporada}.",
)

st.subheader("Em que minuto saem os gols?")
minutos = ler("gols_por_minuto")
minutos = minutos[minutos["temporada"] == temporada]
if minutos.empty:
    st.info("A fonte só traz os gols a partir de 2014.")
else:
    desenhar(
        _minutos(minutos), f"Gols de mandantes e visitantes por faixa de minuto em {temporada}."
    )

st.subheader("Quais foram as maiores sequências?")
sequencias = ler("sequencias")
sequencias = sequencias[sequencias["temporada"] == temporada]
maiores = sequencias[sequencias["jogos"] == sequencias.groupby("tipo")["jogos"].transform("max")]
maiores = maiores.assign(
    Tipo=pd.Categorical(
        maiores["tipo"].map(SEQUENCIAS), categories=list(SEQUENCIAS.values()), ordered=True
    ),
    ate_o_fim=maiores["em_aberto"].map({1: "Sim", 0: "Não"}),
).sort_values(["Tipo", "clube"])
maiores["Tipo"] = maiores["Tipo"].astype(str)
tabela(
    maiores[["Tipo", "clube", "jogos", "data_inicio", "data_fim", "ate_o_fim"]].rename(
        columns={
            "clube": "Clube",
            "jogos": "Jogos",
            "data_inicio": "Começou",
            "data_fim": "Terminou",
            "ate_o_fim": "Foi até o fim",
        }
    ),
    datas=["Começou", "Terminou"],
)
st.caption('"Foi até o fim" quer dizer que a sequência chegou ao último jogo do time naquele ano.')

rodape()
