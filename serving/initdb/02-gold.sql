-- Schemas gold (o que o dashboard lê) e gold_carga (área de pouso da carga), as 8 tabelas
-- do contrato do gold (SPEC-gold.md) com PK pelo grão, e os grants do leitor.
-- Este DDL é o contrato do lado do Postgres: mudou o gold, a carga quebra aqui.

CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION carga;
CREATE SCHEMA IF NOT EXISTS gold_carga AUTHORIZATION carga;

-- As tabelas ficam com dono carga, que faz o TRUNCATE na troca.
SET ROLE carga;

CREATE TABLE IF NOT EXISTS gold.classificacao_por_rodada (
    temporada integer,
    rodada integer,
    clube text,
    posicao integer,
    pontos integer,
    ajuste_pontos integer,
    jogos integer,
    vitorias integer,
    empates integer,
    derrotas integer,
    gols_pro integer,
    gols_contra integer,
    saldo integer,
    aproveitamento double precision,
    ultimos_5 text,
    PRIMARY KEY (temporada, rodada, clube)
);

CREATE TABLE IF NOT EXISTS gold.desempenho_mandante_visitante (
    temporada integer,
    clube text,
    mando text,
    jogos integer,
    vitorias integer,
    empates integer,
    derrotas integer,
    gols_pro integer,
    gols_contra integer,
    pontos integer,
    aproveitamento double precision,
    PRIMARY KEY (temporada, clube, mando)
);

CREATE TABLE IF NOT EXISTS gold.sequencias (
    temporada integer,
    clube text,
    tipo text,
    jogos integer,
    data_inicio date,
    data_fim date,
    em_aberto integer,
    PRIMARY KEY (temporada, clube, tipo)
);

CREATE TABLE IF NOT EXISTS gold.ranking_historico (
    posicao integer,
    clube text,
    temporadas integer,
    primeira_temporada integer,
    ultima_temporada integer,
    jogos integer,
    vitorias integer,
    empates integer,
    derrotas integer,
    gols_pro integer,
    gols_contra integer,
    saldo integer,
    pontos integer,
    aproveitamento double precision,
    titulos integer,
    PRIMARY KEY (clube)
);

CREATE TABLE IF NOT EXISTS gold.disciplina (
    temporada integer,
    clube text,
    jogos integer,
    amarelos integer,
    vermelhos integer,
    amarelos_por_jogo double precision,
    vermelhos_por_jogo double precision,
    fonte text,
    PRIMARY KEY (temporada, clube)
);

CREATE TABLE IF NOT EXISTS gold.artilharia (
    temporada integer,
    posicao integer,
    atleta text,
    clubes text,
    gols integer,
    penaltis integer,
    PRIMARY KEY (temporada, atleta)
);

CREATE TABLE IF NOT EXISTS gold.gols_por_minuto (
    temporada integer,
    faixa text,
    lado text,
    gols integer,
    pct_gols double precision,
    PRIMARY KEY (temporada, faixa, lado)
);

CREATE TABLE IF NOT EXISTS gold.resumo_temporada (
    temporada integer,
    times integer,
    partidas integer,
    gols integer,
    media_gols double precision,
    pct_vitoria_mandante double precision,
    pct_empate double precision,
    pct_vitoria_visitante double precision,
    campeao text,
    vice text,
    rebaixados text,
    artilheiro text,
    gols_artilheiro integer,
    PRIMARY KEY (temporada)
);

-- leitor: só SELECT em gold, inclusive em tabela criada depois. Nada em gold_carga.
GRANT USAGE ON SCHEMA gold TO leitor;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO leitor;
ALTER DEFAULT PRIVILEGES FOR ROLE carga IN SCHEMA gold GRANT SELECT ON TABLES TO leitor;

RESET ROLE;
