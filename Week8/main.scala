import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace

object ReplaceStringExample {
 def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Replace String Example")
      .getOrCreate()

    // Sample data
    val data = """
Customer_NO,Card_type,Date,Category,Transaction Type,Amount
1000210,Platinum Card,3/17/2018,Fast Food,Debit,23.34
1000210,Silver Card,3/19/2018,Restaurants,Debit,36.48
1000210,Checking,3/19/2018,Utilities,Debit,35
1000210,Platinum Card,3/20/2018,Shopping,Debit,14.97
1000210,Silver Card,3/22/2018,Gas & Fuel,Debit,30.55
1000210,Platinum Card,3/23/2018,Credit Card Payment,Debit,559.91
1000210,Checking,3/23/2018,Credit Card Payment,Debit,559.91
"""

    // Load the data into a DataFrame
    val df = spark.read.option("header", "true").csv(Seq(data).toDS)

    // Replace "Checking" with "Cash" in the Card_type column
    val dfWithReplacedString = df.withColumn("Card_type", regexp_replace($"Card_type", "Checking", "Cash"))

    // Show the result
    dfWithReplacedString.show()
 }
}
