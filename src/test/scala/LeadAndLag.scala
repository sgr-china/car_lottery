import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window

/**
 * @author sunguorui
 * @date 2022年04月15日 1:47 下午
 */
object LeadAndLag {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("json")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = Seq(("A", "2019-01-01", "2019-01-02"),
      ("A", "2019-01-02", "2019-01-03"),
      ("A", "2019-01-04", "2019-01-05"),
      ("A", "2019-01-08", "2019-01-09"),
      ("A", "2019-01-09", "2019-01-10"),
      ("B", "2019-01-01", "2019-01-02"),
      ("B", "2019-01-02", "2019-01-03"),
      ("B", "2019-01-04", "2019-01-05"),
      ("B", "2019-01-08", "2019-01-09"),
      ("B", "2019-01-09", "2019-01-10"))
      .toDF("user_id", "start_time", "end_time")
      .select($"user_id", $"start_time".cast("timestamp"), $"end_time".cast("timestamp"))
    df.printSchema()
    df.show()
  val win = Window.partitionBy("user_id").orderBy("start_time")

    val dfWithLag = df.withColumn("befor_end_time", lag($"end_time", 1).over(win))
    dfWithLag.show()
    dfWithLag.printSchema()
    val dfDaysDiff = dfWithLag.withColumn("day", datediff($"start_time", $"befor_end_time"))
    dfDaysDiff.show()
    dfDaysDiff.printSchema()
    val dfTag = dfDaysDiff.withColumn("flag", when($"day" <= 1, 0).otherwise(1))
    dfTag.show()
    dfTag.printSchema()
    val dfx = dfTag.withColumn("session", sum("flag").over(win))
    dfx.printSchema()
    dfx.show()

  }

}
