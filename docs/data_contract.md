# Contrato de dados

## Fontes

1. [ANAC VRA — metadados](https://www.anac.gov.br/acesso-a-informacao/dados-abertos/areas-de-atuacao/voos-e-operacoes-aereas/voo-regular-ativo-vra/62-voo-regular-ativo-vra): situação e horários previstos e realizados de etapas de voos, com publicação mensal.
2. [Arquivos oficiais de 2025](https://siros.anac.gov.br/siros/registros/diversos/vra/2025/): CSV UTF-8, separador ponto e vírgula.
3. [OurAirports](https://github.com/davidmegginson/ourairports-data): cadastro de aeroportos em CSV, domínio público segundo seus termos. A dimensão usa o snapshot registrado no manifesto.

Os arquivos originais não são redistribuídos no repositório. O código os baixa diretamente das fontes e registra URL, horário de obtenção, bytes, cabeçalho e SHA-256. A ANAC pode revisar os arquivos históricos; um download posterior pode produzir outro hash.

## Campos principais

| Origem | Silver | Regra |
|---|---|---|
| Sigla ICAO Empresa Aérea | `airline` | Três caracteres alfanuméricos |
| Número Voo | `flight_number` | Texto alfanumérico de até oito caracteres; mantém zeros |
| Código DI e Código Tipo Linha | `di`, `line_type` | Componentes da identidade |
| Aeroportos origem/destino | `origin`, `destination` | Quatro caracteres alfanuméricos |
| Partida/Chegada Prevista/Real | campos `_utc` | `dd/MM/yyyy HH:mm`, Brasília → UTC |
| Referência | `flight_date` | `dd/MM/yyyy HH:mm:ss`; mês deve coincidir com o arquivo |
| Situação Voo | `status` | `REALIZADO`, `CANCELADO`, `NAO_INFORMADO` |
| Justificativa | `justification` | Preservada, sem inferir causalidade |

`source_month` descreve a competência do arquivo; não é uma partição por data de ingestão. A referência identifica o mês publicado segundo a fonte. Alguns voos não possuem previsão: ausência de previsão não é tratada como erro de formato.

## Identidade e revisões

`flight_id` é o SHA-256 da estrutura com empresa, número do voo, DI, tipo de linha, origem, destino, referência e horário de partida previsto. Se a previsão estiver ausente, usa o horário realizado; se ambos estiverem ausentes, usa texto vazio. A chave é uma decisão do projeto, não um identificador oficial fornecido pela ANAC.

`row_hash` considera o conteúdo dos campos selecionados, incluindo horários realizados e justificativa. Registros com a mesma chave e o mesmo conteúdo são deduplicados. Se a chave possui conteúdos diferentes dentro do mesmo arquivo, todos esses registros são isolados. Não há ordenação de revisão confiável dentro do CSV para eleger um vencedor.

Como cada arquivo é um snapshot completo do mês, uma republicação substitui a partição mensal inteira. Isso também remove chaves que deixaram de existir e acomoda alterações na própria chave. Não se presume que o arquivo seja um log de eventos.

## Quarentena e bloqueios

Cabeçalho incompleto, checksum divergente, arquivo vazio, erro de parsing estrutural ou nenhuma linha aceita interrompem a carga. Erros de campo e conflitos de chave são gravados em Parquet com motivos. A política de rejeição de 2% é aplicada por arquivo antes de atualizar a Silver.

Novas colunas da fonte ficam preservadas na Bronze e no cabeçalho do manifesto, mas não são incorporadas automaticamente à Silver. Remover ou renomear um campo contratado interrompe a transformação. Uma mudança de tipo ou de semântica exige revisão do contrato e replay.

Um horário preenchido que não pode ser interpretado é diferente de um horário ausente. O primeiro gera quarentena; o segundo pode ser esperado para cancelamento ou voo não previsto. Ausência de correspondência no cadastro OurAirports permanece como indicador de qualidade do enriquecimento.

## Métricas

- Cancelamentos = cancelados / todas as etapas publicadas.
- Cobertura de partida = realizados com partida real e prevista / realizados.
- OTP15 de partida = partidas elegíveis com atraso ≤ 15 minutos / partidas elegíveis.
- Cobertura e OTP de chegada seguem a mesma lógica com os horários de chegada.
- Atraso negativo é antecipação; não é removido nem limitado a zero.

As razões são calculadas com somas dos numeradores e denominadores, sem média simples de percentuais por trecho. Os indicadores são definições analíticas do projeto, não uma reprodução certificada de indicadores regulatórios. Não há dados de passageiros para medir pessoas afetadas.
