from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("FilterBySalaryFromFile").getOrCreate()
 
# Load data from a TSV file
employeeRDD = spark.sparkContext.textFile("input5_2.txt")
filteredRDD = employeeRDD.map(lambda line: line.split("\t")) \
                         .filter(lambda x: int(x[2]) > 50000)
 
filteredRDD.saveAsTextFile("high_salary_employees")
spark.stop()
