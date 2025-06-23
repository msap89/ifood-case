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

## Justificativas Técnicas

- **Delta Lake:** escolhido para garantir consistência e escalabilidade no processamento dos dados, além do suporte a operações ACID e `merge` para atualizações incrementais.
- **Arquitetura em camadas (Raw, Silver, Gold):** facilita o controle de qualidade, rastreabilidade e preparação dos dados para análise.
- **Uso de Widgets no Databricks:** para parametrizar execuções e permitir flexibilidade no pipeline sem necessidade de alterar código.
- **Merge Delta na camada Gold:** para evitar duplicações e manter dados atualizados incrementalmente.
- **Particionamento por tipo, ano e mês:** otimiza a leitura e escrita, reduzindo custos computacionais e melhorando performance.
- **Uso do Notebook execute_all:** com a versão Community do Databricks não é possível criar Workflows, então para facilitar a execução como um todo, foi criado esse Notebook.

## Resultados das Análises

### 1. Média do valor total (`total_amount`) recebido por mês nos Yellow Taxis

- Foi calculada a média mensal do valor total das corridas.
- Resultado destacado para os meses de janeiro a maio de 2023, conforme dados ingeridos.
- Exemplo: Maio apresentou um aumento gradual em relação aos meses anteriores, com média de $28,50.
  ![image](https://github.com/user-attachments/assets/ab79d290-0ffa-4c00-9e23-e2c9617e8c64)

  Resultado da Consulta:
  
![image](https://github.com/user-attachments/assets/caf8aa0c-43f3-4483-88be-46dd68b8a6d0)




### 2. Média de passageiros por hora do dia no mês de maio (todos os táxis)

- Análise mostrou que o maior número médio de passageiros ocorre entre as 02h e 03h.
- Esses dados auxiliam na identificação dos horários de pico para planejamento e tomada de decisão.
  ![image](https://github.com/user-attachments/assets/9b8ef278-7509-41b0-9466-7b6089fb0932)

  Resultado da Consulta:

  ![image](https://github.com/user-attachments/assets/899ef17a-e6c9-4ab2-98ef-7eff918cb003)



### 3. Análises complementares
- Foi adicionado 2 análises complementares interessantes para entender ainda mais o comportamento para Yellow Táxis.
- Volume de corrida por mês:
![image](https://github.com/user-attachments/assets/6a771da0-d0b5-4827-87b6-b294aaca441c)

Este gráfico mostra o comportamento mensal da demanda por táxis entre janeiro e maio de 2023, útil para identificar sazonalidade ou picos de atividade.

Resultado da Consulta:

![image](https://github.com/user-attachments/assets/11eb7f67-2263-4ade-a369-74c472fd5350)



- Top 10 corridas com os maiores valores pagos:
![image](https://github.com/user-attachments/assets/fed3d4c4-3a87-452f-877e-df5dfe68d0e0)


Essas corridas apresentam valores de total_amount muito acima da média. Possivelmente envolvem erros de entrada ou tarifas extraordinárias.

Resultado da Consulta:

![image](https://github.com/user-attachments/assets/413dc060-78c6-450e-800f-96d7e1be0995)





## Como Executar
Pré-requisitos
- Conta no Databricks
- Cluster configurado com runtime compatível (ex: Databricks Runtime 13.x com suporte Delta)

Passo a Passo
1. Clone o repositório no ambiente ou importe os notebooks no Databricks
2. Configure os widgets de parâmetros em cada notebook para tipos, meses e anos desejados
3. Execute o notebook `execute_all` no diretório analysis/. Ele irá executa todos os tratamentos necessários até a disponibilização das análises propostas pelo desafio.

### Estrutura do Repositório

```bash
├── .github/
│   └── workflows/                                # Workflows de CI/CD
├── analysis/
│   └── 001-awsers/
│   |   ├── insights_yellow_taxi                 # Análise da média de valor total recebido por mês (yellow)
│   |   └── may_insights_hourly_passenger_count  # Análise da média de passageiros por hora (mês de maio)
|   └── execute_all
├── src/
│   ├── 001-common/
│   │   └── swiss_knife_taxi                      # Funções utilitárias reaproveitáveis
│   ├── 002-ingestion/
│   │   └── external_data_to_raw                  # Notebook que realiza o download e ingestão dos dados brutos
│   ├── 003-raw-processing/
│   │   └── raw_to_silver                         # Conversão dos dados brutos (raw) para a camada Silver com formatação e controle
│   └── 004-silver-processing/
│       └── silver_to_gold                        # Consolidação e limpeza dos dados para análise (camada Gold)
├── README.md                                     # Documentação do projeto
```


Contato

Mário Amaral

mario89sergio@gmail.com

GitHub: msap89
