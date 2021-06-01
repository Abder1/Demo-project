import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object TPStreaming_part2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val userSchema = new StructType().add("year", "string")
      .add("name", "string")
      .add("country", "string")
      .add("count", "string")

    val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv("file:///csv2")
    csvDF.printSchema()
    csvDF.groupBy("name").count().writeStream.outputMode("complete").format("console").start().awaitTermination()
  }
}
