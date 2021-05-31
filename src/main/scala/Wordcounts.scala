import org.apache.spark.{SparkConf, SparkContext}

object Wordcounts {
  def main(args: Array[String]) {
    //Create conf object
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCounter")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("file:///spark-2.4.7-bin-hadoop2.7/README.md")
    val tokenizedData = textFile.flatMap(line=>line.split(" "))
    val countprep = tokenizedData.map(word=>(word,1)).reduceByKey(_+_)
    val sortedCounts = countprep.sortBy(kvPairs=>kvPairs._2, false)
    sortedCounts.saveAsTextFile("file:///Spark-test/Readmecounts_12")
    sc.stop
  }

}
