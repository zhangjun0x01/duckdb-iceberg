from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
from pyspark import SparkContext

import sys
import os
import glob

DATA_GENERATION_DIR = "data/persistent"
PARQUET_PATH = os.path.join(DATA_GENERATION_DIR, "parquet_files")
TABLE_LOCATION = os.path.join(DATA_GENERATION_DIR, "problematic_v1_table")

SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(
    SCRIPT_DIR,
    "..",
    "data_generators",
    "iceberg-spark-runtime-4.0_2.13-1.10.0.jar",
)

conf = pyspark.SparkConf()
conf.setMaster("local[*]")
conf.set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
conf.set("spark.sql.catalog.iceberg_catalog.type", "hadoop")
conf.set("spark.sql.catalog.iceberg_catalog.warehouse", DATA_GENERATION_DIR)
conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
conf.set("spark.driver.memory", "10g")
conf.set("spark.jars", SPARK_RUNTIME_PATH)
conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)

spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

spark.sql("USE iceberg_catalog")

# ----------------------------------------------------
# Reproduction
# ----------------------------------------------------

spark.sql("CREATE NAMESPACE IF NOT EXISTS duckdb_repro")

table_schema = """
  id bigint,
  name string,
  category string,
  value double,
  count bigint,
  active boolean,
  description string
"""

# ----------------------------------------------------
# Method 1: add_files (problem case)
# ----------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE duckdb_repro.fake_data_test ({table_schema})
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")

spark.sql(f"""
CALL iceberg_catalog.system.add_files(
  table => 'duckdb_repro.fake_data_test',
  source_table => 'parquet.`{PARQUET_PATH}`'
)
""")

# ----------------------------------------------------
# Method 2: control table (works in DuckDB)
# ----------------------------------------------------

spark.sql(f"""
CREATE OR REPLACE TABLE duckdb_repro.fake_data_control ({table_schema})
USING iceberg
TBLPROPERTIES ('format-version'='2')
""")

spark.sql(f"""
INSERT INTO duckdb_repro.fake_data_control
SELECT id, name, category, value, count, active, description
FROM parquet.`{PARQUET_PATH}`
""")

# ----------------------------------------------------
# Debug output
# ----------------------------------------------------

print("Files registered in fake_data_test:")
spark.sql("""
SELECT file_path, record_count
FROM duckdb_repro.fake_data_test.files
""").show(truncate=False)