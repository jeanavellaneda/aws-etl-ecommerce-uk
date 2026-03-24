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
args = getResolvedOptions(sys.argv, ['input_bucket'])
input_bucket = args['input_bucket']
input_path = f"s3://{input_bucket}/silver/ecommerce_retail_silver"

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
df = spark.read.parquet(input_path)

# Agregar columna "total"
df_transformed = df.withColumn("total", col("Quantity") * col("UnitPrice"))

# Imprimir esquema y muestra
df_transformed.printSchema()
df_transformed.show(5)

# Guardar en Parquet
output_path = f"s3://{input_bucket}/gold/ecommerce_retail_gold"
df_transformed.write.mode("overwrite").parquet(output_path)
