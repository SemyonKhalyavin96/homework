package milano

import org.apache.spark
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
object MilanoPollutionStats {
  val sparkSession: SparkSession = SparkSession.builder.master("local").appName("Milano Pollution Stats").getOrCreate()
  import sparkSession.implicits._
  case class PollutionMi(
                          sensor_id:    String,
                          time_instant: String,
                          measurement:  java.math.BigDecimal
                        )
  case class Legend(
                     sensor_id:           Int,
                     sensor_street_name:  String,
                     sensor_lat:          java.math.BigDecimal,
                     sensor_long:         java.math.BigDecimal,
                     sensor_type:         String,
                     uom:                 String,
                     time_instant_format: String
                   )
  case class TelecommunicationsMi(
                                   square_id:                 Int,
                                   time_interval:             Int,
                                   country_code:              Int,
                                   sms_in_activity:           Int,
                                   sms_out_activity:          Int,
                                   call_in_activity:          Int,
                                   call_out_activity:         Int,
                                   internet_traffic_activity: Int
                                 )

  val PollutionMiSchema: StructType           = Encoders.product[PollutionMi].schema
  val LegendSchema: StructType                = Encoders.product[Legend].schema
  //val TelecommunicationsMiSchema: StructType  = Encoders.product[TelecommunicationsMi].schema
  def getDatasetFromCSV[T](path: String, delimiter: String, header: Boolean, encoding: String, schema: StructType)(implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] = {
    //def getDatasetFromCSV[T](path: String, delimiter: String, header: Boolean, encoding: String, schema: StructType)(implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T]
    val dataset = sparkSession.read.format("csv").option("delimiter", delimiter).option("header", header).option("encoding", encoding).schema(schema).load(path).as[T]
    dataset
  }
  def main(arg: Array[String]): Unit = {
    var pollution_mi_path = "source/mi_pollution/pollution_mi"
    var legend_path       = "source/mi_pollution/legend"
    //var telecommunications_mi_path = "source/telecommunications_mi"
    //var mi_grid_path = "source/mi_grid"

    val pollution_mi_csv  = getDatasetFromCSV[PollutionMi](pollution_mi_path, delimiter = ",", header = false, encoding = "windows-1252", PollutionMiSchema)(sparkSession, Encoders.product[PollutionMi])
    val legend_csv        = getDatasetFromCSV[Legend](legend_path, delimiter = ",", header = false, encoding = "windows-1252", LegendSchema)(sparkSession, Encoders.product[Legend])
    //val telecommunications_mi_csv = getDatasetFromCSV[TelecommunicationsMi](telecommunications_mi_path, ",", false, "windows-1252", TelecommunicationsMiSchema)(sparkSession, Encoders.product[TelecommunicationsMi])

    pollution_mi_csv.createOrReplaceTempView("pollution_mi")
    legend_csv.createOrReplaceTempView("legend")
    //telecommunications_mi_csv.createOrReplaceTempView("telecommunications_mi")

    sparkSession.sql("select * from pollution_mi").show(10)
    sparkSession.sql("select * from legend").show(10)
  }
}
