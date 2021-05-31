import org.apache.spark.{SparkConf, SparkContext}

object TpCore2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCounter")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:///csv/people12.csv") //Line 1
    val splitRdd = rdd.filter(line => !line.contains ("year")).map(line => line.split(",")) //Line 2
    val fieldRdd = splitRdd.map(f => (f(1),f(3).toInt)) //Line 3
    val namesCount=fieldRdd.reduceByKey((v1,v2) => v1 + v2) //Line 4
    namesCount.foreach(println) //Line 5

  }

}
