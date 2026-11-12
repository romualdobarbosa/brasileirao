# Simulação de temporada em andamento

Eu queria ver o pipeline lidando com dado que muda, rodada a rodada, mas a fonte do Kaggle (versão 18) termina em 07/12/2025 e não existe temporada real em andamento para acompanhar. Então simulei. O módulo `brasileirao.simulacao` pega os CSVs completos de 2025 e monta, para cada rodada N, um retrato da fonte só com o que já teria sido publicado até ali. O pipeline roda nesse retrato rodada a rodada, num lake separado.

## O que entra no retrato da rodada N

- As partidas de 2025 com `rodata` até N, e as outras temporadas inteiras.
- Menos os jogos adiados que ainda não aconteceram. A lista está em `src/brasileirao/reference/simulacao_eventos.csv` e saiu dos próprios dados de 2025. Um jogo conta como adiado quando foi disputado mais de 4 dias depois da data mediana da rodada dele, e aparece na primeira rodada cuja data mediana já passou da data do jogo. São 17 jogos, muitos por causa do Mundial de Clubes.
- Com dois placares errados de propósito, inventados por mim para ver a correção passar pelo pipeline. Juventude x Ceará (rodada 3) sai 1 x 1 sem o gol dos 68 minutos e é corrigido na rodada 4. Fluminense x Fortaleza (rodada 20) sai 1 x 1 sem o gol dos 64 minutos e é corrigido na rodada 22. O gol some também de `gols`, porque o gold confere o placar com os gols e reprovaria um placar sem gol correspondente.
- Gols, cartões e estatísticas seguem as partidas do retrato.

Jogo antecipado, como os da rodada 37 jogados em novembro, entra na própria rodada. Na rodada 38 o retrato é a fonte original, byte a byte.

## Isolamento

Tudo é gravado em `data_sim/`, que fica fora do git. A simulação recusa `./data`, qualquer pasta dentro dele ou acima dele, `app/dados` e caminho remoto, antes de abrir o Spark. Ela nunca chama o `exportar`, e um teste confere que `app/dados` não muda depois de uma simulação.

## O que mudou no pipeline

- `lakehouse --em-andamento` aceita a temporada incompleta. Sem a opção, a execução continua recusando como antes.
- `gold --em-andamento` descobre na `silver/partidas` qual temporada está incompleta e troca, só para ela, a checagem de jogos na última rodada pelas de temporada parcial. As outras temporadas continuam com as checagens de sempre.
- Duas checagens novas valem para toda temporada, completa ou não. Nenhum clube pode ter mais jogos que rodadas disputadas nem mais que 2 x (times − 1). E os pontos precisam fechar com 3 x vitórias + empates + ajuste, sem os pontos de campo caírem de uma rodada para a seguinte.
- O Change Data Feed fica ligado em `silver/partidas`, a única tabela atualizada por `MERGE`. A tabela já nasce com ele, e o `lakehouse` liga numa tabela antiga só se ainda estiver desligado, para não criar versão à toa. As outras tabelas silver trocam a partição inteira, e o feed delas seria a temporada toda apagada e gravada de novo.

No lake normal, o `make lakehouse` de 11/11/2026 ligou o feed com uma versão só, a 30 (`SET TBLPROPERTIES`), sem reescrever dado. A foto de `./data` continuou igual à de `docs/aws/foto_local.json` nas 22 tabelas.

## Como rodar

```
make simulacao-preparar HISTORICO=1   # apaga data_sim/ e carrega 2003 a 2024
make simulacao RODADA=1 ATE=38        # retrato, bronze/silver, gold e relatório de cada rodada
make relatorio-rodada RODADA=4        # só o relatório, lido do lake
```

As etapas de uma rodada também rodam uma a uma (`python -m brasileirao.simulacao retrato|lakehouse|gold|checar N`). É assim que a DAG do Airflow chama a simulação, uma tarefa por etapa, como está no [README da orquestração](README.md).

Cada commit da rodada leva o rótulo `simulacao 2025 rodada N` no histórico do Delta. É assim que o relatório acha as versões de `silver/partidas` daquela rodada.

## Relatório entre rodadas

O relatório lê o Change Data Feed das versões da rodada e confere o resultado com a comparação de versões do OT2 (`lake.historico.comparar_versoes`). Saída real da rodada 4, com a correção do placar.

```
rodada 4: silver/partidas versões 30 a 30 (Change Data Feed)
entraram 10 partida(s)
  #8816 rodada 4 Ceará 2 x 1 Vasco
  #8817 rodada 4 Botafogo-RJ 2 x 2 São Paulo
  #8818 rodada 4 Sport 0 x 1 Bragantino
  #8819 rodada 4 Mirassol 4 x 1 Grêmio
  #8820 rodada 4 Internacional 0 x 1 Palmeiras
  #8821 rodada 4 Corinthians 0 x 2 Fluminense
  #8822 rodada 4 Flamengo 6 x 0 Juventude
  #8823 rodada 4 Vitória 2 x 1 Fortaleza
  #8824 rodada 4 Santos 2 x 0 Atlético-MG
  #8825 rodada 4 Cruzeiro 3 x 0 Bahia
mudaram 1 partida(s)
  #8806 rodada 3 Juventude 2 x 1 Ceará: placar_mandante 1 → 2, vencedor nulo → Juventude
saíram 0 partida(s)
conferência com a comparação de versões (29 → 30): entraram 10, mudaram 1, saíram 0: bate com o feed
```

Rodada 12, com um jogo adiado da rodada 10 chegando.

```
rodada 12: silver/partidas versões 38 a 38 (Change Data Feed)
entraram 7 partida(s)
  #8885 rodada 10 Botafogo-RJ 3 x 2 Ceará (de rodada anterior, chegou agora)
  #8896 rodada 12 Bragantino 0 x 3 Bahia
  #8897 rodada 12 Vitória 0 x 0 Cruzeiro
  #8898 rodada 12 Fortaleza 2 x 3 Santos
  #8899 rodada 12 Grêmio 1 x 1 Corinthians
  #8900 rodada 12 Atlético-MG 2 x 0 Internacional
  #8901 rodada 12 São Paulo 1 x 3 Vasco
mudaram 0 partida(s)
saíram 0 partida(s)
conferência com a comparação de versões (37 → 38): entraram 7, mudaram 0, saíram 0: bate com o feed
```

O `MERGE` mexe só no que mudou. Na rodada 4 o feed tem uma linha `update_preimage` e uma `update_postimage`, as duas da partida corrigida, e nenhuma das outras partidas que já estavam na tabela.

## Prova das rodadas 1 a 38

Rodei `make simulacao-preparar HISTORICO=1` e as rodadas 1 a 38 em sequência, em 11/11/2026, na mesma máquina do [tuning](../tuning/README.md).

| Etapa | Tempo |
|---|---:|
| Preparar, com as temporadas 2003 a 2024 | 185 s |
| Rodadas 1 a 38, em um processo só | 1.219 s |
| Lakehouse por rodada, mediana | 8,0 s |
| Gold por rodada, mediana | 20,5 s |

A rodada 1 é mais lenta (lakehouse 21,2 s e gold 49,6 s) porque cria as tabelas e as constraints. O relatório de cada rodada bateu com a comparação de versões nas 38.

Depois da rodada 38, a foto de `data_sim` (`databricks/foto.py`, linhas e sha256 de cada tabela) ficou igual à de `./data` nas 22 tabelas, as 4 silver e as 18 gold, sem precisar restringir a 2025. As duas correções de placar injetadas não deixam diferença no fim porque chegam antes da rodada 38, e o retrato da 38 é a fonte original.

Rodar a rodada 38 de novo não criou versão em `silver/partidas`. A última versão continuou sendo a 64.

## Limites

- O gold de uma rodada no meio da temporada calcula campeão, rebaixados e linha de corte de 2025 com a tabela daquele momento. Na simulação isso fica só em `data_sim/`.
- O retrato segue a `rodata` da fonte, e os adiamentos vêm de uma regra de datas que eu escolhi. Não é o calendário oficial da CBF, mas para testar o pipeline acho que basta.
