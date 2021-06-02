import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TPStreaming_part2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")


    val userSchema = new StructType().add("year", "string")
      .add("name", "string")
      .add("country", "string")
      .add("project", "string")


    val csvDF = spark.readStream.option("sep", ",").schema(userSchema).csv("file:///csv2")
    csvDF.createOrReplaceGlobalTempView("people")
    val req1 = spark.sql("SELECT * FROM global_temp.people where project like 'Spark'")
    val r1 = req1.writeStream.outputMode("append").option("truncate","false").format("console").start()
    val req2 = spark.sql("SELECT * FROM global_temp.people where name like 'john'")
    val r2 = req2.writeStream.outputMode("append").option("truncate","false").format("console").start()
    r1.awaitTermination()
    r2.awaitTermination()

  }
}
