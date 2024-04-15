from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("GroupBySubject").getOrCreate()
# studentData = [("John", "Math", 85), ...] 
studentData = spark.sparkContext.textFile("input5_4.txt").map(lambda line: line.split("\t"))
#studentRDD = spark.sparkContext.parallelize(studentData)
groupedBySubjectRDD = studentData.groupBy(lambda x: x[1])

avgMarksAndCount = groupedBySubjectRDD.map(lambda x:(x[0],sum(float(student[2]) for student in x[1] ) /len(x[1]),len(x[1])))
 
# print([(x, list(y)) for x, y in groupedBySubjectRDD.collect()])
avgMarksAndCount.saveAsTextFile("Week5_4")
spark.stop()
