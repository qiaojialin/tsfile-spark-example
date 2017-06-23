package wolf

import org.apache.spark.sql.SparkSession
import cn.edu.thu.tsfile._

/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {
    var spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("test connector")
      .getOrCreate()
    val df = spark.read.tsfile("src/main/resources/test1.tsfile")

    println(df.schema)

    df.show()
  }
}
