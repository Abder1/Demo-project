import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DemoSpakStream {

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))
    spark.sparkContext.setLogLevel("ERROR")

    val lines = ssc.socketTextStream("localhost",9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val hashtags = words.filter(w=>w.contains("WARN"))
    val pairs = hashtags.map(w=>(w,1))
    val wordcounts1 = pairs.reduceByKeyAndWindow((x: Int, y:Int) =>
    x+y, Seconds(50))
    wordcounts1.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
