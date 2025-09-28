# Introdução ao Projeto
Este projeto demonstra Apache Spark com Delta Lake e Iceberg usando o dataset IPS Brasil. Veja exemplos ACID em Delta e Iceberg.

# Trabalho de Pesquisa - Apache Spark com Delta Lake e Apache Iceberg

Este projeto tem como objetivo demonstrar a utilização de **Delta Lake** e **Apache Iceberg** em Apache Spark, utilizando dados públicos do **IPS Brasil**.

## Estrutura do projeto

- **notebooks/**: notebooks interativos para análise de dados
  - `delta_analysis.ipynb`
  - `iceberg_analysis.ipynb`
- **data/**: CSV público com dados de municípios brasileiros
- **main.py**: pipeline completo (ETL + ACID + Time Travel)
- **docs/**: documentação do MKDocs
  - `delta.md`: descrição Delta Lake
  - `iceberg.md`: descrição Iceberg

## Como rodar

1. Criar ambiente Python e instalar dependências (Poetry ou pip)
2. Rodar `main.py` para carregar dados no Delta e Iceberg
3. Abrir JupyterLab ou notebooks em `notebooks/` para análise interativa

