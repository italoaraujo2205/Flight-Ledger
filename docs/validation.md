# Validação

## Execução real

Arquivos ANAC de janeiro, fevereiro e março de 2025, obtidos diretamente do SIROS. Total de 250.594 registros na origem; 250.568 aceitos e 26 em quarentena por conflitos de chave. Não houve injeção de erros nos arquivos reais. O cadastro OurAirports contém 86.218 códigos depois da resolução de repetidos.

A carga executou PySpark 3.5.5, commits Delta Lake com delta-rs 1.2.1, materialização em DuckDB 1.4.3 e `dbt build` com Core 1.10.15 e adapter 1.9.6. Os nove testes de dados passaram. Ambiente: Python 3.12.14, Java 17, Linux.

Os hashes estão em `reports/validation.json`. Uma repetição usando os mesmos arquivos retornou `NOOP` e manteve o mesmo snapshot público, sem iniciar outro Spark. O tempo observado dessa repetição foi aproximadamente 0,20 segundo dentro da função de carga, com arquivos já em cache; isso não inclui a inicialização do interpretador ou download, e não é um benchmark de escala.

Um replay dos três meses reais foi executado sem download e suas 119.184 agregações diárias coincidiram com as da publicação anterior. A nova publicação preservou a mesma contagem de linhas aceitas e de quarentena.

## Testes automatizados

Doze testes Python passaram. Cobrem snapshots e checksum, download incompleto, resposta HTML, parâmetros, idempotência, UTC, denominadores, atualização e remoção de partições, preservação da publicação após falha, recuperação, replay, lock e manifesto de exportação S3.

O teste de revisão remove um trecho inteiro de um snapshot e confirma que sua agregação antiga desaparece. O teste de recuperação interrompe a publicação depois da Silver e verifica que a execução seguinte publica os meses pendentes. Os testes usam fixtures pequenas e independentes da base real.

## O que não foi validado

Docker não está disponível no ambiente utilizado. As imagens, o Airflow standalone e a DAG executando no Airflow não foram homologados localmente; o workflow possui um job manual para construir as imagens. O arquivo da DAG foi verificado sintaticamente. A exportação S3 foi exercitada com um cliente de teste, incluindo falha de upload; não houve envio a um bucket real. Não houve execução em Databricks ou outro serviço gerenciado.

O período de 2025 é histórico. A dimensão OurAirports é atual e não resolve mudanças retrospectivas de códigos ou geografia. Uma nova obtenção dos CSVs pode trazer revisões legítimas e resultados diferentes, identificáveis pelos checksums.
