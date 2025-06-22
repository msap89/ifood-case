# iFood Case - Ingestão e Análise de Dados de Táxis NYC

## Visão Geral

Este projeto implementa uma solução de engenharia de dados para ingestão, transformação e análise de dados das corridas de táxis de Nova York, seguindo o desafio proposto. A solução utiliza Apache Spark e Delta lake no ambiente Databricks Community para construir um pipeline de dados em camadas (Raw, Silver, Gold), garantindo qualidade, eficiência e facilidade de consumo.

## Objetivos
- Ingerir dados brutos de corridas de táxis para o DBFS, organizados por tipo, ano e mês
- Realizar transformações para padronizar, tratar e enriquecer os dados na camada Silver
- Consolidar dados na camada Gold com schemas padronizados para facilitar consultas e análises
- Utilizar boas práticas de engenharia de dados, como versionamento Delta, Merges, otimizações e limpeza de dados antios

## Tecnologias Utilizadas
- Apache Spark (Pyspark) para processamento distribuído
- Delta Lake para gerenciamento de dados em camadas com ACID e versionamento
- Databricks Community Edition para desenvolvimento, armazenamento e execução
- Python para funções auxiliares e notebooks
- SQL para consultas analíticas

## Como Executar
Pré-requisitos
- Conta no Databricks
- Cluster configurado com runtime compatível (ex: Databricks Runtime 13.x com suporte Delta)

Passo a Passo
1. Clone o repositório no ambiente ou importe os notebooks no Databricks
2. Configure os widgets de parâmetros em cada notebook para tipos, meses e anos desejados
3. Execute o notebook `execute_all` no diretório src/. Ele irá executa todos os tratamentos necessários até a disponibilização das análises propostas pelo desafio.

### Estrutura do Repositório

```bash
├── src/
│   ├── 001-common/
│   │   └── swiss_knife_taxi                      # Funções utilitárias reaproveitáveis
│   ├── 002-ingestion/
│   │   └── external_data_to_raw                  # Notebook que realiza o download e ingestão dos dados brutos
│   ├── 003-raw-processing/
│   │   └── raw_to_silver                         # Conversão dos dados brutos (raw) para a camada Silver com formatação e controle
│   └── 004-silver-processing/
│       └── silver_to_gold                        # Consolidação e limpeza dos dados para análise (camada Gold)
├── analysis/
│   └── 001-questions/
│       ├── insights_yellow_taxi                 # Análise da média de valor total recebido por mês (yellow)
│       └── may_insights_hourly_passenger_count  # Análise da média de passageiros por hora (mês de maio)

├── README.md                                     # Documentação do projeto

└── .github/
    └── workflows/                                # Workflows de CI/CD
```


Contato

Mário Amaral

mario89sergio@gmail.com

GitHub: msap89
