import org.apache.spark.sql.SparkSession

object Runner extends App{

  //Core spark session
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Word Count")
    .getOrCreate()

  //Reading csv file
  val df = sparkSession.read.csv("test.csv")

  //Reading ignoring the header
  val df_no_header = sparkSession.read.option("header","true").csv("test.csv")

  //Print content
  df.show()

  df_no_header.show()

  //Print the number of rows
  println(df.count())

  println(df_no_header.count())

}

