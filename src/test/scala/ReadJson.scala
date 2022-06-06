import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author sunguorui
 * @date 2022年04月14日 6:54 下午
 */
object ReadJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("json")
      .master("local[*]")
      .getOrCreate()
    val df1 = spark.read.format("json").load("file:/Users/guorui/testJsonFile")
    df1.show(false)
    df1.printSchema()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df1Trans = df1.select(explode($"params_list")).toDF("params_list")
    df1Trans.show(100, false)
    df1Trans.printSchema()
    val result = df1Trans.select($"params_list.key" as "key",
      $"params_list.value" as "value")
    result.show(false)
    result.printSchema()
    // TODO df2想采用jackson与StructType（StructField（））的方式应该也可以做到但是未验证


  }

}
