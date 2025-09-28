# Apache Iceberg no Spark

Iceberg é formato aberto para lagos de dados com ACID.

**Exemplos de Comandos (Tabela ips_municipios):**
- **INSERT:** `df.write.format("iceberg").mode("append").saveAsTable("iceberg.ips_municipios")`
- **UPDATE:** `spark.sql("UPDATE iceberg.ips_municipios SET nota_ips = 90 WHERE municipio = 'CidadeX'")`
- **DELETE:** `spark.sql("DELETE FROM iceberg.ips_municipios WHERE nota_ips < 50")`

Fonte: [Iceberg.apache.org](https://iceberg.apache.org) e repositórios GitHub.
