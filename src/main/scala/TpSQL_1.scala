import org.apache.spark.sql.SparkSession

object TpSQL_1 {

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()
    val bookDetails = spark.read.csv("file:///csv/logData.csv")
    bookDetails.createTempView("logData")
    bookDetails.createGlobalTempView("LogData_Global")
    val req = spark.sql("SELECT * FROM global_temp.LogData_Global")
    req.show(false)

    val req1 = spark.sql("SELECT count(*) as flikpartsCount FROM global_temp.LogData_Global where _c2 like '%flipkart%'")
    req1.show()

    val req2 = spark.sql("SELECT count(*) as flikpartsCount, _c1 as ipAdress FROM global_temp.LogData_Global where _c2 like '%flipkart%' group by _c1")
    req2.show()

    val req3 = spark.sql("SELECT distinct count(*) as count FROM global_temp.LogData_Global group by _c3")
    req3.show()

    val req4 = spark.sql("SELECT distinct _c1, count(*) as countIp FROM global_temp.LogData_Global group by _c1")
    req4.show()

    req.write.format("json").save("file:///csv/logData100.json")

    val sqlDF = spark.sql("SELECT * FROM json.`file:///csv/logData100.json`")
    sqlDF.show(false)
  }
}
