
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object CorelationExample {
  def main(args: Array[String]): Unit = {
  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkByExample")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(
    Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
    Vectors.dense(4.0, 5.0, 0.0, 3.0),
    Vectors.dense(6.0, 7.0, 0.0, 8.0),
    Vectors.sparse(4, Seq((0, 3.0), (3, 1.0)))
  )

  val df = data.map(Tuple1.apply).toDF("features")
  val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
  println(s"Person correlation mtrix:\n $coeff1")

}
}
