# Trabalho Apache Spark com Delta Lake e Apache Iceberg

## Descrição
Trabalho sobre o uso do Apache Spark para processamento de dados em larga escala, com foco em tabelas de data lake: **Delta Lake** (para transações ACID) e **Apache Iceberg** (para gerenciamento de metadados e particionamento avançado).

O projeto inclui:
- Scripts PySpark para criar e manipular tabelas Delta e Iceberg.
- Notebook Jupyter com análise de dados reais (leitura de Parquet, particionamento, time travel).
- Documentação gerada com MkDocs (exemplos de INSERT, UPDATE, DELETE, MERGE).

## Participantes
- Eunice Correia
- Maria Laura
- Vitoria Viana


## Tecnologias utilizadas  
- Python (PySpark)  
- Apache Spark  
- Delta Lake  
- Apache Iceberg  
- Jupyter Notebook  
- MkDocs  

---

## Requisitos  

Antes de rodar o projeto, verifique se de que:  
- Possui **Python** instalado.  
- **Apache Spark** está configurado com suporte a Delta Lake e Iceberg.  
- As dependências estão instaladas via Poetry ou pip.  

### Instalação das dependências  
```bash
poetry install
# ou
pyenv install -r requirements.txt

---

### Como executar:

git clone https://github.com/EuniceCorreia/trabalho_spark.git
cd trabalho_spark

---

(Opcional) Gere a documentação localmente com MkDocs:
mkdocs serve

---

### Exemplo de operações

O projeto cobre operações comuns em Delta Lake e Iceberg:
INSERT
UPDATE
DELETE
MERGE (fusões condicionais)
Time travel (consulta a versões anteriores)
Particionamento e otimização de consultas
