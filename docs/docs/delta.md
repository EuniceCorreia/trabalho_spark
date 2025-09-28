# Delta Lake no Spark

Delta Lake adiciona transações ACID a Spark.

**Exemplos de Comandos (Tabela ips_municipios):**
- **INSERT:** `df.write.format("delta").mode("append").saveAsTable("delta.ips_municipios")`
- **UPDATE:** `spark.sql("UPDATE delta.ips_municipios SET nota_ips = 90 WHERE municipio = 'CidadeX'")`
- **DELETE:** `spark.sql("DELETE FROM delta.ips_municipios WHERE nota_ips < 50")`

Fonte: [Delta.io](https://delta.io) e vídeos DataWay BR.
# Delta Lake - IPS Brasil

## 1. Cenário da tabela

Tabela `delta.ips_municipios` com indicadores socioeconômicos dos municípios brasileiros em 2022.

### Modelo ER simplificado

┌───────────────┐
│ Municipio │
├───────────────┤
│ id_municipio │ PK
│ nome_municipio│
│ uf │
│ area_km2 │
│ populacao_2022│
│ pib_per_capita│
│ nota_ips │
│ dimensao_basica│
│ dimensao_bem_estar│
│ dimensao_oportunidades│
│ nutricao_cuidados_medicos│
│ agua_saneamento│
└───────────────┘


> Fonte: CSV público `data/raw/ips_brasil.csv`

---

## 2. DDL - Criação da tabela Delta

```python
df.write.format("delta").mode("overwrite").option("path", "/data/warehouse/delta/ips_municipios").saveAsTable("delta.ips_municipios")
