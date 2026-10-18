# Requisitos considerados

Consulta em 18 de outubro de 2026. Amostra dirigida de seis anúncios de engenharia de dados pleno; não é um levantamento estatístico do mercado nem garantia de que as vagas continuem abertas.

| Empresa e anúncio | Requisitos relevantes | Aplicação no projeto |
|---|---|---|
| [ClickBus](https://clickbus.gupy.io/jobs/12670894) | Python, SQL, Spark, Airflow, dbt, AWS, idempotência e troubleshooting | Partições incrementais, dbt, recuperação e exportação S3 |
| [Squid](https://squidit.gupy.io/jobs/12506845) | Python, PySpark, Airflow, SQL, Git, APIs; dbt e Docker como diferenciais | Processamento, DAG e testes em CI |
| [Stefanini](https://vagasstefanini.gupy.io/jobs/12661821) | Python, SQL, PySpark, Delta Lake, camadas, falhas e monitoramento | Bronze/Silver/Gold, snapshots, quarentena e publicação segura |
| [Leega](https://leega.gupy.io/jobs/12643772) | Python, SQL, Spark, GCP, modelagem e orquestração | Transformações e modelos; execução GCP não implementada |
| [Itix](https://sejaitix.gupy.io/jobs/12664133) | Python, SQL, Delta Lake, PySpark, qualidade e CI/CD | Contratos, Delta e workflow de validação |
| [Just a Little Data](https://justalittledata.gupy.io/jobs/11674062) | Python, SQL, orquestração, modelagem, CI/CD; dbt/Spark/Delta como diferenciais | Pacote executável, modelos testáveis e runbook |

A prioridade foi demonstrar consistência operacional com uma fonte real, em vez de acrescentar serviços sem função no fluxo. O projeto não substitui experiência de produção em Databricks, AWS, GCP ou Azure. O processamento local e os resultados são reproduzíveis; a transferência para cloud exigiria validação própria.
