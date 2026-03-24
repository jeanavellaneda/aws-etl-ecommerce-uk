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

df_transformed = df.filter((col("CustomerID") != "") | (col("CustomerID").isNotNull())) \
.filter(col("UnitPrice") >= 0.0) \
.filter(col("Quantity") >= 0) \
.filter((col("Description") != "") | (col("Description").isNotNull()))

# Imprimir esquema y muestra
df_transformed.printSchema()
df_transformed.show(5)

# Guardar en Parquet
output_path = f"s3://{input_bucket}/silver/ecommerce_retail_silver"
df_transformed.write.mode("overwrite").parquet(output_path)
