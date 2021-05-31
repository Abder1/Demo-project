import org.apache.spark.{SparkConf, SparkContext}

object TpCore1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCounter")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:///csv/people12.csv") //Line 1
    val splitRdd = rdd.filter(line => !line.contains("year")).map(line => line.split(",")) //Line 2
    val fieldRdd = splitRdd.map(f => (f(1),f(3).toInt)) //Line 3
    val groupNamesByCountry = fieldRdd.groupByKey //Line 4
    groupNamesByCountry.foreach(println)

  }
}
