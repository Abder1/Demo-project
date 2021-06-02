import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object TokenizerExample {
  def main(args: Array[String]): Unit = {
  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._
  val sentenceDataFrame = spark.createDataFrame(Seq(
    (0, "Hi I heard about you"),
    (1, "Hello heard about you"),
    (2, "Hi,I,heard,about,you"))).toDF("id", "sentence")
  val remover = new StopWordsRemover()
    .setInputCol("word")
    .setOutputCol("filtered")
  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("word")
  val countTokens = udf { (words: Seq[String]) => words.length}
  val tokenized = tokenizer.transform(sentenceDataFrame)
  tokenized.select("sentence", "word")
    .withColumn("tokens", countTokens(col("word"))).show(false)

    remover.transform(tokenized).show(false)
}
}
