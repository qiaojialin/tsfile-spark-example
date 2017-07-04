package wolf


import org.apache.spark.sql.SparkSession
import cn.edu.thu.tsfile._

/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("test connector")
      .getOrCreate()
    val df = spark.read.tsfile("src/main/resources/out.tsfile")

    df.createOrReplaceTempView("tsfile_table")
    println(df.schema)
    df.show()

    spark.sql("select * from tsfile_table where time < 1458891904000").show()

  }
}
