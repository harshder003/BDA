from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("First5EmployeeRecords").getOrCreate()
 
# Load employee data from a TSV file
employeeRDD = spark.sparkContext.textFile("input5_2.txt").map(lambda line: line.split("\t"))
 
# Collect the first 5 records
first5Records = employeeRDD.take(5)
 
# Since RDD.take() returns a list, convert it to RDD to save using RDD methods
first5RecordsRDD = spark.sparkContext.parallelize(first5Records)
 
# Save the output
first5RecordsRDD.saveAsTextFile("week5_5")
 
spark.stop()
