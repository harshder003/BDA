from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

textfile = spark.read.text("input5_1.txt").rdd

wordCounts = textfile.flatMap(lambda line:line.value.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)

wordCounts.saveAsTextFile("output")

spark.stop()
