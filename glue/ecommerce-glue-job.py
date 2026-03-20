import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Inicializar contexto
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Leer argumentos desde Lambda
args = getResolvedOptions(sys.argv, ['input_bucket', 'input_key'])

input_bucket = args['input_bucket']
input_key = args['input_key']
input_path = f"s3://{input_bucket}/{input_key}"

# Definir el esquema esperado
schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Country", StringType(), True)
])

# Leer archivo CSV usando el esquema definido
df = spark.read.option("header", "true").option("delimiter", ";").schema(schema).csv(input_path)

# Agregar columna "total"
df_transformed = df.withColumn("total", col("Quantity") * col("UnitPrice"))

# Imprimir esquema y muestra
df_transformed.printSchema()
df_transformed.show(5)

# Guardar en Parquet
output_path = f"s3://{input_bucket}/gold/ventas_gold"
df_transformed.write.mode("overwrite").parquet(output_path)

# Crear tabla externa en Athena
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS db_online_retail.ventas_gold (
    InvoiceNo string,
    StockCode string,
    Description string,
    Quantity int,
    InvoiceDate string,
    UnitPrice double,
    CustomerID string,
    Country string,
    total double
)
STORED AS PARQUET
LOCATION 's3://{input_bucket}/gold/ventas_gold'
""")
