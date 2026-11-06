# Dashboard do Metabase

Registro dos cards do dashboard "Brasileirão 2003–2025", pra dar pra remontar tudo do zero. A lista de perguntas vem da seção "Dashboard" da [spec do serving](../../SPEC-serving.md).

Cada card tem o SQL equivalente, testado como `leitor` no Postgres local com o gold real. No Metabase, dá pra montar pelo query builder quando ele chegar no mesmo resultado; quando não chegar, cola o SQL numa pergunta nativa.

## Filtros

A aba "Temporada" tem três filtros de dashboard, ligados às variáveis das perguntas nativas:

| Filtro | Variável | Tipo | Padrão | Cards ligados |
|---|---|---|---|---|
| Temporada | `{{temporada}}` | Número, obrigatório | `2025` | todos da aba |
| Rodada | `{{rodada}}` | Número, opcional | vazio = última rodada da temporada | 1 |
| Clube | `{{clube}}` | Texto, opcional | vazio = todos | 2 |

A "última rodada" não é um valor fixo (muda com a temporada), então o padrão fica no SQL: com a rodada vazia, o trecho `[[{{rodada}},]]` some e o `coalesce` cai no `max(rodada)` da temporada escolhida.

A aba "Histórico" não tem filtro.

## Aba "Temporada"

### 1. Como estava a tabela nessa rodada?

- **Tabela:** `gold.classificacao_por_rodada`
- **Colunas:** `posicao`, `clube`, `pontos`, `jogos`, `vitorias`, `empates`, `derrotas`, `gols_pro`, `gols_contra`, `saldo`, `aproveitamento`, `ultimos_5`
- **Gráfico:** tabela, com `aproveitamento` formatado como porcentagem
- **Filtros:** temporada, rodada

```sql
SELECT posicao, clube, pontos, jogos, vitorias, empates, derrotas,
       gols_pro, gols_contra, saldo, aproveitamento, ultimos_5
FROM gold.classificacao_por_rodada
WHERE temporada = {{temporada}}
  AND rodada = coalesce([[{{rodada}},]] (
        SELECT max(rodada) FROM gold.classificacao_por_rodada WHERE temporada = {{temporada}}
      ))
ORDER BY posicao
```

Na última rodada, `pontos` já inclui as punições do STJD (`ajuste_pontos`); nas anteriores, é o resultado de campo.

### 2. Como a posição de cada time mudou ao longo do campeonato?

- **Tabela:** `gold.classificacao_por_rodada`
- **Colunas:** `rodada`, `clube`, `posicao`
- **Gráfico:** linha, `rodada` no eixo X, `posicao` no Y com o eixo invertido (1º no alto), uma série por `clube`
- **Filtros:** temporada, clube (com o clube vazio, saem as 20 linhas)

```sql
SELECT rodada, clube, posicao
FROM gold.classificacao_por_rodada
WHERE temporada = {{temporada}}
  [[AND clube = {{clube}}]]
ORDER BY rodada, posicao
```

### 3. Quem foi campeão, vice e quem caiu?

- **Tabela:** `gold.resumo_temporada`
- **Colunas:** `campeao`, `vice`, `rebaixados`, `media_gols`
- **Gráfico:** quatro cartões de número/texto, um por coluna (a mesma pergunta duplicada, mudando o campo exibido)
- **Filtros:** temporada

```sql
SELECT campeao, vice, rebaixados, media_gols
FROM gold.resumo_temporada
WHERE temporada = {{temporada}}
```

É sempre o resultado final da temporada, qualquer que seja a rodada escolhida.

### 4. Quem fez mais gols?

- **Tabela:** `gold.artilharia`
- **Colunas:** `posicao`, `atleta`, `clubes`, `gols`, `penaltis`
- **Gráfico:** barras horizontais de `gols` por `atleta` (ou tabela)
- **Filtros:** temporada

```sql
SELECT posicao, atleta, clubes, gols, penaltis
FROM gold.artilharia
WHERE temporada = {{temporada}}
  AND posicao <= 10
ORDER BY posicao, atleta
```

`posicao` é `rank()`: com empate na 10ª posição, o top 10 traz mais de 10 nomes. Só existe de 2014 em diante; antes disso o card fica vazio (ver o cartão de notas).

### 5. Quem rende mais em casa e fora?

- **Tabela:** `gold.desempenho_mandante_visitante`
- **Colunas:** `clube`, `mando`, `aproveitamento`
- **Gráfico:** barras agrupadas por `clube`, uma barra por mando, aproveitamento em porcentagem
- **Filtros:** temporada

```sql
SELECT clube,
       max(aproveitamento) FILTER (WHERE mando = 'mandante') AS aproveitamento_casa,
       max(aproveitamento) FILTER (WHERE mando = 'visitante') AS aproveitamento_fora
FROM gold.desempenho_mandante_visitante
WHERE temporada = {{temporada}}
GROUP BY clube
ORDER BY aproveitamento_casa DESC, clube
```

É resultado de campo, sem as punições do STJD.

### 6. Em que minuto saem os gols?

- **Tabela:** `gold.gols_por_minuto`
- **Colunas:** `faixa`, `lado`, `gols`, `pct_gols`
- **Gráfico:** barras agrupadas, `faixa` no eixo X, uma série por `lado` (mandante × visitante)
- **Filtros:** temporada

```sql
SELECT faixa, lado, gols, pct_gols
FROM gold.gols_por_minuto
WHERE temporada = {{temporada}}
ORDER BY faixa, lado
```

A ordem alfabética das faixas (`0-15`, `16-30`, `31-45+`, `46-60`, `61-75`, `76-90+`) já é a ordem do jogo. Só de 2014 em diante.

### 7. Quais foram as maiores sequências?

- **Tabela:** `gold.sequencias`
- **Colunas:** `tipo`, `clube`, `jogos`, `data_inicio`, `data_fim`, `em_aberto`
- **Gráfico:** tabela, a maior sequência de cada tipo (com empate, todos os clubes empatados)
- **Filtros:** temporada

```sql
SELECT tipo, clube, jogos, data_inicio, data_fim, em_aberto = 1 AS em_aberto
FROM (
    SELECT *, rank() OVER (PARTITION BY tipo ORDER BY jogos DESC) AS ordem
    FROM gold.sequencias
    WHERE temporada = {{temporada}}
) AS s
WHERE ordem = 1
ORDER BY CASE tipo
             WHEN 'invencibilidade' THEN 1
             WHEN 'vitorias' THEN 2
             WHEN 'jejum' THEN 3
             ELSE 4
         END,
         clube
```

`em_aberto` verdadeiro quer dizer que a sequência chegou até o último jogo do time na temporada.

## Aba "Histórico"

### 8. Quem mais somou pontos desde 2003?

- **Tabela:** `gold.ranking_historico`
- **Colunas:** `posicao`, `clube`, `temporadas`, `pontos`, `titulos`, `jogos`, `vitorias`, `empates`, `derrotas`, `saldo`, `aproveitamento`
- **Gráfico:** tabela
- **Filtros:** nenhum

```sql
SELECT posicao, clube, temporadas, pontos, titulos, jogos, vitorias, empates, derrotas,
       saldo, aproveitamento
FROM gold.ranking_historico
ORDER BY posicao
```

### 9. Quem tem mais títulos na era dos pontos corridos?

- **Tabela:** `gold.ranking_historico`
- **Colunas:** `clube`, `titulos`
- **Gráfico:** barras horizontais
- **Filtros:** nenhum

```sql
SELECT clube, titulos
FROM gold.ranking_historico
WHERE titulos > 0
ORDER BY titulos DESC, clube
```

### 10. O mando de campo perdeu força?

- **Tabela:** `gold.resumo_temporada`
- **Colunas:** `temporada`, `pct_vitoria_mandante`, `pct_empate`, `pct_vitoria_visitante`
- **Gráfico:** linha por temporada, em porcentagem, com destaque (anotação ou descrição do card) em 2020, a temporada sem torcida
- **Filtros:** nenhum

```sql
SELECT temporada, pct_vitoria_mandante, pct_empate, pct_vitoria_visitante
FROM gold.resumo_temporada
ORDER BY temporada
```

### 11. O campeonato ficou com mais ou menos gols?

- **Tabela:** `gold.resumo_temporada`
- **Colunas:** `temporada`, `media_gols`
- **Gráfico:** linha por temporada
- **Filtros:** nenhum

```sql
SELECT temporada, media_gols
FROM gold.resumo_temporada
ORDER BY temporada
```

### 12. O jogo ficou mais violento?

- **Tabela:** `gold.disciplina`
- **Colunas:** `temporada`, `amarelos`, `vermelhos`, `jogos`, `fonte`
- **Gráfico:** linha de cartões por partida, por temporada (amarelos e vermelhos como séries)
- **Filtros:** nenhum

```sql
SELECT temporada,
       round(sum(amarelos)::numeric / (sum(jogos) / 2.0), 2) AS amarelos_por_partida,
       round(sum(vermelhos)::numeric / (sum(jogos) / 2.0), 2) AS vermelhos_por_partida,
       string_agg(DISTINCT fonte, ', ') AS fonte
FROM gold.disciplina
GROUP BY temporada
ORDER BY temporada
```

Cada partida conta duas vezes em `jogos` (um por clube), daí a divisão por 2. Só de 2014 em diante; 2025 vem de outra fonte (`estatisticas`), que conta um pouco diferente.

## Cartão de notas

Cartão de texto, nas duas abas:

> Os dados vêm de uma base pública que tem buracos. Artilharia, gols por minuto e cartões só existem a partir de 2014. Em 2016, a partida Chapecoense × Atlético-MG da última rodada não foi disputada (W.O. depois do acidente da Chapecoense), então a temporada tem 379 jogos. Barueri (2009) e Grêmio Prudente (2010) são o mesmo clube, mas aparecem separados, como na fonte.

## Como montar

1. Entrar no Metabase como admin.
2. **Admin → Databases:** apagar o "Sample Database" que o Metabase cria sozinho, pra ele não aparecer pra ninguém (nem no link público).
3. **Admin → Databases → Add database:** PostgreSQL, host `postgres`, porta `5432`, banco `brasileirao`, usuário `leitor`. Em "Schemas", escolher "Only these…" com `gold`.
4. **Admin → Table Metadata:** nomes amigáveis em português nas tabelas e colunas (ex.: `gols_pro` → "Gols pró"), e `aproveitamento` e os `pct_*` como porcentagem.
5. Criar as 12 perguntas acima (**New → SQL query**, banco `brasileirao`). Nas variáveis: `temporada` como Number, obrigatória, padrão `2025`; `rodada` como Number, opcional; `clube` como Text, opcional.
6. Criar o dashboard "Brasileirão 2003–2025" com as abas "Temporada" (cards 1 a 7) e "Histórico" (cards 8 a 12), mais o cartão de notas.
7. Na aba "Temporada", adicionar os filtros Temporada (padrão `2025`), Rodada e Clube e ligar cada um às variáveis dos cards, conforme a tabela de filtros.
Os passos 8 a 10 eram para a publicação na nuvem, que não aconteceu (ver o [README](README.md#até-onde-foi)). Ficam aqui como registro.

8. **Admin → Settings → Public Sharing:** ligar. No dashboard, **Sharing → Create a public link** e copiar o link. Não compartilhar nenhuma outra pergunta ou dashboard.
9. Abrir o link numa janela anônima e trocar a temporada e a rodada pra conferir.
10. Rodar `make backup-metabase`.
