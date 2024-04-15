from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("SumOfElements").getOrCreate()
 
# Load numbers from a text file
numbersRDD = spark.sparkContext.textFile("numbers.txt").map(lambda number: int(number))
 
# Find the sum of all elements
sumOfNumbers = numbersRDD.reduce(lambda x, y: x + y)
 
# Since we cannot directly save a single integer using RDD methods, parallelize it first
sumOfNumbersRDD = spark.sparkContext.parallelize([sumOfNumbers])
 
# Save the output
sumOfNumbersRDD.saveAsTextFile("week5_6")
 
spark.stop()
