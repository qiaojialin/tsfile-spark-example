package wolf


import org.apache.spark.sql.SparkSession
import cn.edu.thu.tsfile._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {

    val argument = new ArrayBuffer[String]()
    argument += "a"
    args.foreach(f => argument += f)
    if(argument.length != 1) {
      println("need 1 argument: column_name")
      return
    }

    val columnName = argument(0)

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("test connector")
      .getOrCreate()
    val df = spark.read.tsfile("src/main/resources/out.tsfile")
    df.createOrReplaceTempView("tsfile_table")

    val select = "select time, " + columnName + " from tsfile_table"

    val newDf = spark.sql(select)

    newDf.show()
    val s = newDf.collect()
    var i = 0
    var count = 0
    var start = ""
    var stop = ""
    while(i < 100) {
      val fieldType = newDf.schema.fields(1)

      val flag = fieldType.dataType match {
        case IntegerType => s(i).getInt(1) < 0
        case FloatType => s(i).getFloat(1) < 0
        case DoubleType => s(i).getDouble(1) < 0
        case LongType => s(i).getLong(1) < 0
      }

      if(flag) {
        if(count == 0)
          start = s(i).get(0).toString
        count = count + 1
      } else {
        if(count >= 5) {
          stop = s(i-1).get(0).toString
          println("time interval: " + start + " - " + stop)
        }
        count = 0
      }

      i = i + 1
    }
    spark.stop()
  }

}
