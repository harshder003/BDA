from pyspark.sql import SparkSession
from pyspark.sql.functions import count,collect_list

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Clickstream Analysis") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("clickstream_data.csv", header=True, inferSchema=True)

df.show(5)
df5= df.take(5)
# Calculate the total number of clicks, views, and purchases for each user
action_counts = df.groupBy("user_id", "action").count()

# Show the result
action_counts.show()

# This is a simplified example and might not work directly for your use case
# It assumes you have a way to order actions by timestamp and user
user_action_sequences = df.groupBy("user_id").agg(collect_list("action").alias("action_sequence"))

# Show the result
user_action_sequences.show()

# df5.saveAsTextFile('Top5')
action_counts.rdd.saveAsTextFile('Count')
user_action_sequences.rdd.saveAsTextFile('Sequences')
