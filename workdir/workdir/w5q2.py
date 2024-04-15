from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("TransformEmployeeRecordsFromFile").getOrCreate()
 
# Load data from a TSV file
employeeRDD = spark.sparkContext.textFile("input5_2.txt")
transformedRDD = employeeRDD.map(lambda line: line.split("\t")) \
                            .map(lambda x: (x[0], int(x[1]) * 2, int(x[2])))
 
transformedRDD.saveAsTextFile("transformed_employees")
spark.stop()
