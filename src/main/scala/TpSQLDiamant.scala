import org.apache.spark.sql.SparkSession

object TpSQLDiamant {
  case class Diamond(_c0: Int, carat: Double, cut: String, color: String, clarity: String, depth: Double, table: Double, price: Int, x: Double, y: Double, z: Double)
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._
    val diamants = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///csv/export-diamants.csv")
    diamants.show
    diamants.printSchema
    diamants.createGlobalTempView("Diamants_Global")
    val result1 = spark.sql("SELECT * FROM global_temp.Diamants_Global WHERE color != 'E'")
    result1.show
    val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///csv/export-diamants.csv").as[Diamond]
    val result1_1 = diamonds.filter(diamond => { diamond.color != "E" })
    result1_1.show
    val result2 = spark.sql(" SELECT min(price) as minPrice, max(price) as maxPrice, round(avg(price)) as avgPrice FROM global_temp.Diamants_Global")
    result2.show
    val result3 = spark.sql(" SELECT min(price) as minPrice, max(price) as maxPrice, round(avg(price)) as avgPrice, color FROM global_temp.Diamants_Global GROUP BY color ORDER By avgPrice ")
    result3.show
    println(diamants.count)
    val result4 = spark.sql(" SELECT cut FROM global_temp.Diamants_Global ")
    println(result4.distinct.show)
  }

}
