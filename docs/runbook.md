# Operação e recuperação

## Caminho de uma execução

1. Obter o lock do diretório de dados.
2. Baixar ou reutilizar os arquivos e registrar snapshots Bronze por hash.
3. Comparar os snapshots com o estado Silver.
4. Validar e substituir cada competência alterada no Delta.
5. Construir a Gold separada e executar `dbt build`.
6. Publicar o ponteiro somente após sucesso e registrar `last_run.json`.

Logs incluem competência, linhas aceitas, quarentena, snapshot e tempo da execução. `data/quality/<mes>.json` descreve a última transformação; o manifesto da Gold preserva a qualidade da publicação que está sendo consumida. `data/last_run.json` distingue `PUBLISHED`, `NOOP` e `FAILED`.

## A origem republicou um mês

Execute `flightledger sync --months 2025-03 --refresh`. Compare o novo SHA-256 e os campos `inserted`, `updated`, `removed` na qualidade. Esses campos contam diferenças entre snapshots; não são eventos de um sistema transacional.

## Uma etapa foi removida da origem

A substituição da partição remove a linha antiga. O pre-hook dbt também remove agregados do mês antes do novo cálculo. Não é necessário truncar o histórico de outras competências.

## Download incompleto ou resposta HTML

O cache existente não deve ser substituído. A execução falha e pode ser repetida. Arquivos `.part` são removidos. HTTP 429 e erros 5xx transitórios têm até três tentativas; 404 não é convertido em mês vazio.

## Quarentena acima do limite

Consulte `data/quarantine/<mes>/<hash>/` e a exceção. Verifique se há alteração legítima do contrato antes de mudar a regra. O limite não deve ser aumentado apenas para fazer a carga passar. Corrija o código, mantenha o arquivo Bronze original e execute `flightledger replay`.

## Falha depois da Silver e antes da publicação

`published.json` continua apontando para o banco anterior. O log completo de dbt fica na pasta Gold que falhou. Execute novamente o mesmo comando: o estado Silver não força outro download e os meses ainda não publicados permanecem pendentes.

## Reprocessar sem acesso à internet

Use `flightledger replay`. O comando verifica os hashes Bronze, normaliza novamente todas as competências registradas e faz `dbt build --full-refresh`. Os testes comparam esse resultado com a carga incremental.

## Verificar a publicação

Use `flightledger audit`. O checksum do banco deve coincidir com o manifesto. Para consultar via Python:

```python
import json
from pathlib import Path
import duckdb

root = Path.cwd()
published = json.loads((root / "data/published.json").read_text())
with duckdb.connect(str(root / published["warehouse"]), read_only=True) as conn:
    print(conn.execute("select * from analytics.mart_route_reliability limit 10").fetchall())
```

## Cuidados com armazenamento

Não remova Parquets de uma tabela Delta manualmente. Não execute vacuum sem definir a retenção necessária para replay e time travel. As pastas Gold não publicadas podem ser removidas depois da investigação; preserve a pasta indicada pelo ponteiro. Não há limpeza automática neste projeto.

A escrita é local e serializada. Não compartilhe o diretório entre workers de máquinas diferentes esperando coordenação distribuída. A DAG limita concorrência, mas o lock do pipeline também protege execuções disparadas fora do Airflow.
