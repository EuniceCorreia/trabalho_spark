from pyspark.sql import SparkSession
import os
import shutil  # Para limpeza de dirs
from pyspark.sql.functions import avg  # Para análise explícita

# Ajusta o caminho base para o root do projeto (útil para caminhos relativos)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Sobe 3 níveis de src/data_engineering/main.py para root

# Temps - Limpa e recria para estado limpo (evita warnings de metadata)
os.makedirs("/tmp/delta", exist_ok=True)
shutil.rmtree("/tmp/iceberg", ignore_errors=True)  # Limpa Iceberg para evitar version-hint warnings
os.makedirs("/tmp/iceberg", exist_ok=True)

# SparkSession simples com Delta e Iceberg
# Aumentado memory para dataset full (~5k linhas)
# Configs para DeltaCatalog (sem databricks prefixo para open-source)
spark = SparkSession.builder \
           .appName("Spark-IPS-Delta-Iceberg") \
           .config("spark.jars.packages", 
                   "io.delta:delta-spark_2.12:3.2.0,"  # Delta
                   "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"  # Iceberg para Spark 3.5
                   "org.apache.hadoop:hadoop-aws:3.3.4") \
           .config("spark.sql.extensions", 
                   "io.delta.sql.DeltaSparkSessionExtension,"
                   "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
           .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
           .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
           .config("spark.sql.catalog.iceberg_catalog.warehouse", "file:///tmp/iceberg") \
           .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
           .config("spark.sql.catalog.iceberg_catalog.default-spec", "v2") \
           .config("spark.driver.memory", "2g") \
           .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # ERROR para menos verbosidade (warnings escondidos; mude para "WARN" se quiser ver)

 print("=== Spark Iniciado com Delta e Iceberg! ===")
 print(f"Versão do Spark: {spark.version}")

# Teste rápido do catálogo Iceberg (opcional, mas útil para debug)
       try:
           spark.sql("SHOW CATALOGS").show(truncate=False)
           print("Catálogo Iceberg carregado com sucesso!")
       except Exception as e:
           print(f"Erro no catálogo: {e}")

# Load CSV de data/raw (IPS Brasil full) - Caminho relativo ao root do projeto
raw_path = os.path.join(BASE_DIR, "data", "raw", "ips_brasil.csv")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)
print(f"Dados IPS full carregados de {raw_path}: {df.count()} linhas, {len(df.columns)} colunas.")
# Mostra só colunas chave (com backticks para nomes especiais)
df.select("`Código IBGE`", "`Município`", "`UF`", "`Índice de Progresso Social`").show(5, truncate=False)

# Confirma schema de Código IBGE (deve ser integer)
df.select("`Código IBGE`").printSchema()

# Chama ETL Delta (import relativo: sobe 2 níveis para root e acessa notebooks)
from ...notebooks.delta import run_delta_etl
run_delta_etl(spark, df)

# Chama ETL Iceberg (mesmo import relativo)
from ...notebooks.iceberg import run_iceberg_etl
run_iceberg_etl(spark, df)

# Análise simples (média nota IPS por UF, top 5 - usa df original com alias para orderBy)
print("\n=== Análise: Média IPS por UF (top 5) ===")
df.groupBy("`UF`").agg(avg("`Índice de Progresso Social`").alias("media_ips")) \
  .orderBy("media_ips", ascending=False).show(5, truncate=False)

spark.stop()
print("=== Fim! ETL Completo com Delta e Iceberg (ACID + Time Travel). ===")

if __name__ == "__main__":
    pass  # O código principal roda diretamente; as chamadas ETL estão acima

def view_snapshots(spark, table_name):
    snapshots_df = spark.sql(f"SELECT * FROM {table_name}.snapshots ORDER BY committed_at DESC")
    snapshots_df.show(truncate=False, n=5)
    return snapshots_df.count()

# Uso: view_snapshots(spark, "iceberg_catalog.default.iceberg_ips")


