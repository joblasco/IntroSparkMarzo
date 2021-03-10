package SparkIntro

import org.apache.spark.sql.SparkSession

object TestRDD extends App {

  val session = SparkSession.builder().master("local").appName("testRDD").getOrCreate()
  val sc = session.sparkContext

  val data = Array(1, 2, 3, 4, 5)

  val rddData = sc.parallelize(data, 5).foreach(println(_))

}
