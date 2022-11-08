package milano

import org.apache.spark.sql.SparkSession

object MilanoPollutionStats {
  def main(arg: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder.master("local").appName("Milano Pollution Stats").getOrCreate()
    //val csvPO = sparkSession.read.option("inferSchema", true).option("header", false).csv("pollution-legend-mi.csv")
    //csvPO.createOrReplaceTempView("tabPO")
    //val count = sparkSession.sql("select * from tabPO").count()
    //print(count)
    //sparkSession.sql("select * from tabPO").show(10)

    val csvP1 = sparkSession.read.option("inferSchema", true).option("header", false).csv("pollution-mi/mi_pollution_5504.csv")
    csvP1.createOrReplaceTempView("tabP1")
    sparkSession.sql("select * from tabP1").show(10)
  }
}
