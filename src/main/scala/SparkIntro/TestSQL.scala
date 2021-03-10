package SparkIntro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType

object TestSQL extends App {

  val session = SparkSession.builder().master("local").appName("testRDD").getOrCreate()

  val nombres = session.read.option("header", true).option("inferSchema", true)
    .csv("src/main/resources/baby-names.csv")

  //nombres.show()

  nombres.printSchema()

  val castNombres = nombres.withColumn("año", col("year").cast(StringType))

  val addlit = nombres.withColumn("zeros", lit(0))

  val dropCol = nombres.drop("año")


}
