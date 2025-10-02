


# Apache Iceberg com Spark - Time Travel Demo

Este guia demonstra o uso do **Apache Iceberg** com **Spark SQL/PySpark**, incluindo **INSERT, UPDATE, DELETE** e histórico de snapshots (**Time Travel**).

---

## Configuração do SparkSession

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("iceberg-time-travel-demo")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
    .config("spark.sql.catalog.iceberg_catalog.warehouse", "/tmp/iceberg")
    .getOrCreate()
)

#Operações DML - Exemplo de DELETE

table_name = "iceberg_catalog.default.iceberg_ips"

# DELETE de linha fictícia
spark.sql(f"DELETE FROM {table_name} WHERE `Código IBGE` = 9999999")
print("DELETE: Linha fictícia removida! (DELETE ACID executado - nova snapshot).")

# Verificação: busca a linha deletada (deve retornar vazio)
spark.sql(f"""
SELECT `Código IBGE`, `Município`, `UF`, `Índice de Progresso Social`
FROM {table_name}
WHERE `Código IBGE` = 9999999
""").show(truncate=False)

# Top 5 atualizado
spark.sql(f"""
SELECT `Código IBGE`, `Município`, `UF`, `Índice de Progresso Social`
FROM {table_name}
ORDER BY `Índice de Progresso Social` DESC
LIMIT 5
""").show(truncate=False)

# Total de linhas após DELETE
total_rows = spark.sql(f'SELECT COUNT(*) FROM {table_name}').collect()[0][0]
print(f"Total de linhas após DELETE: {total_rows} (original -1 = ~5565)")

#Time Travel - Histórico de Snapshots

print("\n=== Iceberg: Time Travel ===")

# Lista todos os snapshots
snapshots_df = spark.sql(f"""
SELECT snapshot_id, parent_id, committed_at, operation, summary, manifest_list
FROM {table_name}.snapshots
ORDER BY committed_at DESC
""")

print(f"Total de snapshots encontrados: {snapshots_df.count()}")
snapshots_df.show(truncate=False, n=10)

# Coleta snapshots para indexação
snapshots_list = snapshots_df.collect()
current_snapshot_id = snapshots_list[0][0]       # Mais recente (após DELETE)
v1_insert_id = snapshots_list[1][0] if len(snapshots_list) > 1 else None
v2_update_id = snapshots_list[2][0] if len(snapshots_list) > 2 else None
old_snapshot_id = snapshots_list[-1][0]         # Mais antigo (inicial)



#Função auxiliar para consultar snapshots



def query_snapshot(snapshot_id, limit=2):
    return spark.sql(f"""
        SELECT `Código IBGE`, `Município`, `UF`, `Índice de Progresso Social`
        FROM {table_name} VERSION AS OF {snapshot_id}
        WHERE `Código IBGE` IN (9999999, 3550308)
        ORDER BY `Índice de Progresso Social` DESC
    """)


#Histórico Completo - Antes e Depois de INSERT/UPDATE/DELETE
#1. Versão Inicial (inicial, após DDL)

print(f"\n1. Versão INICIAL (snapshot {old_snapshot_id})")
initial_df = query_snapshot(old_snapshot_id)
initial_df.show(truncate=False)
initial_count = spark.sql(f"SELECT COUNT(*) FROM {table_name} VERSION AS OF {old_snapshot_id}").collect()[0][0]
print(f"Linhas totais: {initial_count} (CSV original)")





#2. Após INSERT (linha fictícia adicionada)
if v1_insert_id:
    print(f"\n2. Após INSERT (snapshot {v1_insert_id})")
    insert_df = query_snapshot(v1_insert_id)
    insert_df.show(truncate=False)
    insert_count = spark.sql(f"SELECT COUNT(*) FROM {table_name} VERSION AS OF {v1_insert_id}").collect()[0][0]
    print(f"Linhas totais: {insert_count} (+1 da fictícia 9999999 com IPS 99.9 no topo)")


#3. Após UPDATE, antes do DELETE
if v2_update_id:
    print(f"\n3. Após UPDATE, ANTES DELETE (snapshot {v2_update_id})")
    update_df = query_snapshot(v2_update_id)
    update_df.show(truncate=False)
    update_count = spark.sql(f"SELECT COUNT(*) FROM {table_name} VERSION AS OF {v2_update_id}").collect()[0][0]
    print(f"Linhas totais: {update_count} (IPS de São Paulo atualizado)")



#4. Após DELETE (linha fictícia removida)

print(f"\n4. Após DELETE (snapshot {current_snapshot_id})")
delete_df = query_snapshot(current_snapshot_id)
delete_df.show(truncate=False)
current_count = spark.sql(f'SELECT COUNT(*) FROM {table_name}').collect()[0][0]
print(f"Linhas totais: {current_count} (-1 da fictícia; volta ao inicial)")


## Observações

- O histórico de snapshots permite **auditar todas as operações** (INSERT/UPDATE/DELETE) de forma imutável.
- É possível fazer **rollback** para qualquer snapshot específico, recuperando dados antigos.

#Dicas


- Use format-version = 2 para habilitar UPDATE e DELETE.

- Planeje particionamento para performance.

- Comandos úteis de manutenção:

CALL iceberg_system.expire_snapshots('iceberg_ips', TIMESTAMP '2025-09-01 00:00:00');
CALL iceberg_system.rewrite_data_files('iceberg_ips');
