# Decisões de arquitetura

## Incremental por competência

A fonte publica snapshots mensais, não eventos individuais. A unidade de trabalho é o mês cujo conteúdo mudou. O SHA-256 impede duplicação por reexecução; `--refresh` permite conferir revisões na origem. Não há CDC nem promessa de atualização em tempo real.

## Bronze imutável e Silver transacional

O arquivo original fica em `data/bronze/<mes>/<hash>.csv`. O índice aponta para os snapshots selecionados. PySpark valida e normaliza somente competências alteradas; a Silver usa Delta Lake particionado por `source_month`.

O `overwrite` com predicado substitui apenas a competência afetada. Um merge por chave sem tratamento de ausência manteria voos removidos pela republicação. Delta-rs executa o commit transacional dos Parquets produzidos pelo Spark, sem exigir pacotes Maven durante a execução. A conversão PyArrow lê um mês normalizado em memória: é um limite concreto desta implementação local.

O estado Silver avança depois do commit Delta. Se o processo cair entre o commit e o estado, a competência pode ser reprocessada na retentativa, mas não será acrescentada em duplicidade. Um lock local serializa os writers. Não há coordenação entre máquinas diferentes.

## Gold publicada por ponteiro

Uma execução cria `data/gold/<snapshot>/warehouse.duckdb`. Quando existe uma publicação anterior, seu banco é copiado como ponto de partida. Os dados Silver são materializados em tabelas `raw`; dbt aplica as transformações e nove testes. O manifesto registra a versão Delta, os hashes de origem e as contagens de qualidade dessa publicação.

Somente então `published.json` é substituído atomicamente. Leitores devem abrir o banco indicado por esse ponteiro, nunca escolher a pasta com a maior data ou o maior nome. Bancos de execuções que falharam ficam fora da publicação e podem ser investigados pelo log dbt.

A Silver pode estar à frente da Gold após uma falha. Isso é esperado: a próxima tentativa compara os hashes publicados com o estado Silver e reconstrói os meses pendentes, mesmo sem novos downloads. O ponteiro, não a última versão Silver, define o produto disponível aos consumidores.

## Incremental dbt sem registros órfãos

O modelo diário usa `append` com um pre-hook que remove todos os meses alterados dentro da transação da materialização. Depois, recalcula seus grupos. Um simples `delete+insert` baseado apenas nas chaves presentes na nova consulta não removeria um trecho que desapareceu completamente. O teste de revisão reproduz esse caso.

Os outros modelos são reconstruídos ou avaliados como views. A Gold importa a Silver inteira para DuckDB; isso limita a escalabilidade e está separado do processamento incremental por mês. Em uma plataforma gerenciada, os modelos poderiam consultar tabelas Delta diretamente por um catálogo.

## Por que não acrescentar mais serviços

Kafka não resolve uma fonte que entrega um arquivo mensal. Airflow cuida do calendário e das retentativas; o processamento mantém a consistência independentemente do orquestrador. DuckDB fornece um destino SQL sem custo cloud obrigatório. A exportação S3 é um destino opcional e não transforma a execução local em um deployment AWS.

## Evolução para ambiente gerenciado

O mapeamento natural seria Bronze/Silver em S3 ou ADLS, Spark/Delta em Databricks ou EMR, catálogo governado e dbt no engine de consulta escolhido. Seriam necessários identidade, permissões, segredos, coordenação de writers, políticas de retenção, monitoramento e testes naquele ambiente. Não há provisionamento ou validação dessa arquitetura cloud neste repositório.

Referências: [Delta — escrita e substituição por predicado](https://delta-io.github.io/delta-rs/latest/usage/writing/), [dbt — estratégias incrementais](https://docs.getdbt.com/docs/build/incremental-strategy), [dbt-duckdb](https://github.com/duckdb/dbt-duckdb), [Airflow standalone](https://airflow.apache.org/docs/apache-airflow/2.10.5/start.html).
