from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

spark = SparkSession.builder.appName("GroupBySubject").getOrCreate()

# Assuming input5_4.txt is tab-separated as "Name\tSubject\tMarks"
df = spark.read.option("delimiter", "\t").csv("input5_4.txt").toDF("Name", "Subject", "Marks")
df = df.withColumn("Marks", df["Marks"].cast("float"))  # Convert marks to float

# Group by subject and calculate average marks and count
result = df.groupBy("Subject").agg(
    avg("Marks").alias("AverageMarks"),
    count("Marks").alias("Count")
)

result.show()  # Display the result
result.write.csv("Week5_4", header=True)  # Save the result into CSV with headers

spark.stop()

