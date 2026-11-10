# Sondagem do enriquecimento (EN1)

Feita em 2026-11-10, com o projeto `brasileirao-numeros` (Sandbox) e a credencial do `pydata-google-auth`. Critério em [SPEC-enrichment.md](../../SPEC-enrichment.md).

## Resultado

**Não passou, por causa da licença, e eu cancelei o módulo em 2026-11-10.** A parte técnica atende. A tabela existe, é por partida e tem valor de elenco com menos de 20% de nulos em 16 temporadas encerradas (2008 a 2023). A licença é que não dá para confirmar. O catálogo da Base dos Dados registra a licença da tabela como "desconhecida", e o dado vem de raspagem do Transfermarkt. Sem poder publicar o resultado no app, achei melhor parar aqui do que usar o dado mesmo assim.

## O que existe

Dataset `basedosdados.mundo_transfermarkt_competicoes`, com duas tabelas:

| Tabela | Linhas (metadados) | Tamanho | Última alteração |
|---|---|---|---|
| `brasileirao_serie_a` | 8.530 | 1,9 MB | 2024-12-09 |
| `copa_brasil` | 652 | 0,15 MB | 2025-03-05 |

`brasileirao_serie_a` tem uma linha por partida, com 35 colunas: `ano_campeonato`, `data`, `rodada`, `estadio`, `arbitro`, `publico`, `publico_max`, `time_mandante`, `time_visitante`, `tecnico_mandante`, `tecnico_visitante`, `colocacao_mandante`, `colocacao_visitante`, `valor_equipe_titular_mandante`, `valor_equipe_titular_visitante`, `idade_media_titular_mandante`, `idade_media_titular_visitante`, gols (total e 1º tempo), escanteios, faltas, chutes de bola parada, defesas, impedimentos, chutes e chutes para fora, cada um com versão mandante e visitante.

O valor de elenco é o **valor da equipe titular naquela partida**, não o do elenco na temporada.

A descrição da tabela diz que de 2003 a 2006 só existem data, rodada, estádio, times e gols. Diz também que técnico vazio deixa vazios o valor e a idade média do time titular, e que Chapecoense x Atlético-MG de 2016-12-11 foi W.O. duplo.

### Linhas escondidas

Os metadados dão 8.530 linhas, mas a consulta enxerga 8.076. A tabela tem segurança por linha (`rowLevelSecurityApplied: true`) e o catálogo marca o trecho de 2024-10-27 a 2024-12-08 como `closed`, do plano pago da Base dos Dados. Pela conta, faltam as 94 partidas finais de 2024. As outras 360 diferenças não têm explicação visível daqui.

## Temporadas e nulos

Visível sem pagar:

| Temporada | Partidas | Nulos em `publico` | Nulos em valor titular (mandante = visitante) | Observação |
|---|---|---|---|---|
| 2003 | 456 | 100% | 100% | |
| 2004 | 456 | 100% | 100% | |
| 2005 | 418 | 100% | 100% | |
| 2006 | 380 | 100% | 100% | |
| 2007 | 380 | 0% | 24,2% | |
| 2008 | 380 | 0% | 1,1% | |
| 2009 | 380 | 0% | 10,5% | |
| 2010 | 380 | 0% | 11,8% | |
| 2011 | 380 | 0% | 18,7% | |
| 2012 | 380 | 0% | 0,3% | |
| 2013 | 380 | 0% | 7,6% | |
| 2014 | 380 | 0% | 0% | |
| 2015 | 380 | 0% | 5,0% | |
| 2016 | 380 | 0,3% | 13,2% | |
| 2017 | 380 | 0% | 3,7% | |
| 2018 | 380 | 0% | 2,6% | |
| 2019 | 380 | 0% | 0% | |
| 2020 | 380 | 0% | 3,4% | público = 0 nas 380 (sem torcida) |
| 2021 | 380 | 0% | 0% | público = 0 em 218 |
| 2022 | 380 | 0% | 0% | |
| 2023 | 380 | 0% | 0% | |
| 2024 | 286 | 0% | 0% | parcial: até a rodada 31 (2024-10-27) |

O silver cobre 2003 a 2025. As temporadas encerradas que se sobrepõem e passam no corte de 20% são estas:
- **valor titular:** 2008 a 2023 (16 temporadas);
- **público:** 2007 a 2023 (17 temporadas), mas o zero de 2020 e 2021 é real (portões fechados) e precisa ser tratado à parte.

2024 está incompleta sem o plano pago, e 2025 não existe na tabela.

## Licença e atribuição

- **Tabela `brasileirao_serie_a`** no catálogo da Base dos Dados (`backend.basedosdados.org/graphql`, campo `license`): `"desconhecida"`, com URL `http://todo.com` (placeholder).
- **Fontes brutas do dataset:** são o site do Transfermarkt e um tutorial de raspagem (fcpython.com). As que têm licença aparecem como "MIT", também com URL placeholder. MIT é licença de código e não diz nada sobre o dado do Transfermarkt.
- **Transfermarkt:** não consegui ler os termos de uso direto (o site bloqueia a busca automática). Fontes secundárias dizem que os termos proíbem raspagem e redistribuição sem consentimento por escrito, e um relato de 2018 atribuído ao Transfermarkt diz que o dado é protegido por direito autoral alemão e não tem licença. Nada disso foi confirmado no texto original.

Não achei licença confirmada que permita usar e republicar esse dado, no app público por exemplo. Pela spec, sem licença confirmada o módulo não segue, e foi o que decidi.

## Consultas usadas e bytes

Todas rodaram no projeto `brasileirao-numeros`. O BigQuery cobra no mínimo 10 MiB por tabela consultada. Com segurança por linha, o job não informa bytes processados (`total_bytes_processed` volta vazio, inclusive no `dry_run`). Para essas consultas, o teto é o tamanho da tabela (1,9 MB), e o valor cobrado fica no mínimo de 10 MiB.

| # | Consulta | Bytes |
|---|---|---|
| 1 | `SELECT table_name, table_type FROM basedosdados.mundo_transfermarkt_competicoes.INFORMATION_SCHEMA.TABLES` | 10 MiB |
| 2 | `SELECT column_name, data_type FROM ….INFORMATION_SCHEMA.COLUMNS WHERE table_name='brasileirao_serie_a'` | 10 MiB |
| 3 | `SELECT option_name, option_value FROM ….INFORMATION_SCHEMA.TABLE_OPTIONS WHERE table_name='brasileirao_serie_a'` | 10 MiB |
| 4 | contagem por `ano_campeonato` com `COUNTIF(publico IS NULL)`, `COUNTIF(valor_equipe_titular_* IS NULL)`, `MIN/MAX(data)`, `MAX(rodada)` | não informado (segurança por linha); tabela de 1,9 MB |
| 5 | `COUNTIF(publico = 0)`, `COUNTIF(publico IS NULL)`, `AVG(publico)` por temporada a partir de 2019 | não informado (segurança por linha); tabela de 1,9 MB |
| 6 | `SELECT table_id, row_count, size_bytes, last_modified_time FROM ….__TABLES__` | 0 |

Total estimado: cerca de 50 MiB, uns 0,005% do 1 TB mensal grátis.
