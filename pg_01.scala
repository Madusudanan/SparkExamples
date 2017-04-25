import org.apache.spark.sql.SparkSession

object Runner extends App{

  //Core spark session
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("Word Count")
    .getOrCreate()

  //Reading csv file
  val df = sparkSession.read.csv("test.csv")

  //Print content
  df.show()

  //Print the number of rows
  println(df.count())

}
