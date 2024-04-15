from pyspark.sql import SparkSession
 
spark = SparkSession.builder.appName("SplitSentences").getOrCreate()
textFileRDD = spark.sparkContext.textFile("input5_1.txt")
wordsRDD = textFileRDD.flatMap(lambda line: line.split(" "))
 
wordsRDD.saveAsTextFile("week5_3")
spark.stop()
