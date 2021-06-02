import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphExample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(
      Array((3L, ("rxjn", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "pof")), (2L, ("istoica", "prof")),
      (4L, ("peter", "student"))))

    val relashionships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(2L, 5L, "colleague"), Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"), Edge(5L, 7L, "pi"), Edge(4L, 0L, "student"),
        Edge(0L, 5L, "colleague")))

    val defaulUser = ("John", "Missing")

    val graph = Graph(users, relashionships, defaulUser)
    graph.triplets.map(
      triplet => triplet.srcAttr._1 + " est le " + triplet.attr + " de " + triplet.dstAttr._1
    ).collect().foreach(println(_))

  }
}
