# Spec: enrichment

> Status: **cancelada em 2026-11-10, na sondagem do EN1.** A tabela da Base dos Dados existe e atende na parte técnica (valor do time titular com menos de 20% de nulos em 2008–2023), mas a licença está registrada como "desconhecida" e o dado vem de raspagem do Transfermarkt. Sem licença confirmada para publicar, o Romualdo decidiu cancelar o módulo e seguir para o Databricks. Detalhes em [docs/enriquecimento/sondagem.md](docs/enriquecimento/sondagem.md).

## Objetivo

Cruzar o silver com uma segunda fonte para responder o que o Kaggle sozinho não responde, como "quem fez mais pontos do que o valor do elenco previa?".

**Fonte candidata:** Base dos Dados, via BigQuery. A tabela `mundo_transfermarkt_competicoes.brasileirao_serie_a` vem de memória e **não foi confirmada**: a busca do site só carrega com JavaScript. Confirmados na documentação deles: precisa de conta Google e de um projeto próprio, sem cartão (modo Sandbox), com 1 TB grátis de consulta por mês.

Fora do escopo: raspar o Transfermarkt, juntar dados pagos e manter a extração agendada.

## Decisões

- **Extração congelada.** BigQuery só é usado em `make download-enriquecimento`, que grava um arquivo em `raw/` e registra data, consulta e linhas. O pipeline lê esse arquivo, e por isso CI, Databricks e EMR não precisam de credencial do Google.
- **Dependências do Google** num grupo próprio do `uv`, fora do que o pipeline importa.
- **Nomes de clubes** passam pelo mapa de times que já existe; clube sem mapeamento falha a carga, como hoje.
- **Fixture pequena** no repositório, para os testes rodarem sem BigQuery.

## Sondagem do EN1 (vai ou não vai)

Só segue se a tabela existir, cobrir pelo menos 5 temporadas encerradas e tiver público ou valor de elenco com menos de 20% de nulos nessas temporadas. Licença e atribuição precisam permitir o uso e ficam no README.

**Se não passar:** o ticket para e leva o caso ao Romualdo com o que foi encontrado. Padrão: cancelar o módulo, registrar o motivo na spec e seguir para o Databricks. Não entra fonte improvisada.

## Aceite

1. Join com o silver casa pelo menos 95% das partidas ou clube-temporadas do período coberto, e o que não casa é listado em aviso.
2. Tabelas gold novas no registro `TABELAS`, com checagens, testes e período coberto explícito.
3. Linhas e somas das 18 tabelas existentes não mudam.
4. Página nova no app, lendo só o Parquet exportado. O texto passa pela `voz-romualdo`.
5. README cita a fonte e o que ela cobre.
