from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Replace String Example") \
    .getOrCreate()

# Load the data from data.txt into a DataFrame
df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv("data.txt")

# Replace "Checking" with "Cash" in the Card_type column
df_with_replaced_string = df.withColumn("Card_type", regexp_replace("Card_type", "Checking", "Cash"))

# Show the result
df_with_replaced_string.show()

df_with_replaced_string.rdd.saveAsTextFile("output_python")
