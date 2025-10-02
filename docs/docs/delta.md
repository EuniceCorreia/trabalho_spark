Delta Lake com Spark


Este guia apresenta como configurar o SparkSession para usar Delta Lake, criar tabelas e executar INSERT, UPDATE, DELETE e Time Travel com exemplos em Spark SQL e PySpark.


#  Configuração do SparkSession


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DeltaLake_ETL_IPS")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Verifica suporte Delta
try:
    spark.sql("CREATE TABLE IF NOT EXISTS spark_catalog.default.test_delta (id INT)")
    spark.sql("DROP TABLE spark_catalog.default.test_delta")
    print("✅ Suporte a Delta Lake confirmado!")
except Exception as e:
    print(f"❌ Erro no Delta: {e}")

print("SparkSession criada com sucesso! Versão:", spark.version)
print("Catálogos disponíveis:", [c.name for c in spark.catalog.listCatalogs()])

#Criação de Tabela Delta


delta_path = "/tmp/delta/ips"

# Limpa tabela se existir
spark.sql("DROP TABLE IF EXISTS delta_ips")

# Grava DataFrame como Delta
df.write.format("delta") \
    .mode("overwrite") \
    .option("delta.columnMapping.mode", "name") \
    .save(delta_path)

# Registra tabela Delta
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta_ips
    USING DELTA
    LOCATION '{delta_path}'
    TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

print(f"Total de linhas na tabela: {spark.sql('SELECT COUNT(*) FROM delta_ips').collect()[0][0]}")




#Operações DML
INSERT

INSERT INTO delta_ips (`Código IBGE`, `Município`, `UF`, `Índice de Progresso Social`)
VALUES ('9999999', 'Exemplo Fictício', 'XX', 99.0);



UPDATE


UPDATE delta_ips
   SET `Índice de Progresso Social` = `Índice de Progresso Social` + 1
 WHERE `Código IBGE` = '1100015';



DELETE


DELETE FROM delta_ips
 WHERE `Código IBGE` = '9999999';



#Leitura e Verificação



Spark SQL


SELECT `Código IBGE`, `Município`, `UF`, `Índice de Progresso Social`
  FROM delta_ips
 ORDER BY `Índice de Progresso Social` DESC
 LIMIT 5;





PySpark


df = spark.table("delta_ips")
df.groupBy("UF").sum("Índice de Progresso Social").show()




Time Travel Delta
-- Lista histórico completo
DESCRIBE HISTORY delta_ips;

-- Consulta versão específica (ex: versão 0)
SELECT `Código IBGE`, `Município`, `UF`, `Índice de Progresso Social`
  FROM delta_ips VERSION AS OF 0
 WHERE `Código IBGE` = '1100015';

-- Comparação com estado atual
SELECT `Código IBGE`, `Município`, `Índice de Progresso Social`
  FROM delta_ips
 WHERE `Código IBGE` = '1100015';



#Dicas


- Use delta.columnMapping.mode = name para lidar com nomes de colunas especiais.

- Projete bem o particionamento e a quantidade de arquivos para performance.

- Histórico de versões permite auditar todas as operações de forma imutável.

- É possível fazer rollback para qualquer versão específica com:


RESTORE TABLE delta_ips TO VERSION AS OF <n>;


