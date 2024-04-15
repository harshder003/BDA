import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Initialize SparkSession
val spark = SparkSession.builder
 .appName("Web Log Analysis")
 .getOrCreate()

// Load the log file into a DataFrame
val df = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/web_logs.txt")

// Convert the timestamp column to a timestamp type
val dfWithTimestamp = df.withColumn("timestamp", to_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss"))

// Sort the DataFrame by user_id and timestamp
val sortedDf = dfWithTimestamp.sort("user_id", "timestamp")

// Calculate the time difference between consecutive actions for each user
val withTimeDifference = sortedDf.withColumn("time_diff", lead($"timestamp",1).over(Window.partitionBy("user_id").orderBy("timestamp")) - $"timestamp")

// Sum the time differences to get the total time spent by each user
val engagedUsers = withTimeDifference
 .filter($"time_diff".isNotNull)
 .groupBy("user_id")
 .agg(sum($"time_diff").alias("total_time_spent"))

// Show the result
engagedUsers.rdd.saveAsTextFile("Week6q2.txt")

