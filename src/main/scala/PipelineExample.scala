import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

object PipelineExample {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val training = spark.createDataFrame(Seq(
      (0L, "This is spark book", 1.0),
      (1L, "published by Apress publications", 0.0),
      (2L, "authors are Dharanitharan", 1.0),
      (3L, "and Subhashini", 0.0)))
      .toDF("id", "text", "label")
    val tokenizer = new Tokenizer().setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF().setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val logitreg = new LogisticRegression().setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, logitreg))
    val model = pipeline.fit(training)
    val test = spark.createDataFrame(Seq(
      (4L, "spark book"),
      (5L, "Subhashini"),
      (6L, "Dharanitharan wrote this book")))
      .toDF("id", "text")
    val transformed = model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
    transformed.foreach {
      case Row(id: Long, text: String, prob: Vector, prediction: Double)
      =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
    }
  }
}
