import com.sgr.spark.SparkSQLEnv

import java.util.Properties

/**
 * @author sunguorui
 * @date 2022年06月24日 2:45 下午
 */
object ReadJDBC {
  SparkSQLEnv.init()
  val spark = SparkSQLEnv.sparkSession
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    val start = System.nanoTime()
    val partitionNums = method3()
    val end = System.nanoTime()

    println("partitionNums => " + partitionNums + " time => " + ((end - start) / 1000000000d).formatted("%.4f") + "s")
  }

  def method1(): Int = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "RedBull.sales")
      .option("user", "root")
      .option("password", "sgrsgrsgr")
      .load()
    jdbcDF.rdd.getNumPartitions
  }

  def method2(): Int = {
    val partitions = Array("sums >= 5000 and sums < 10000")
    val dbProps = new Properties()
    dbProps.put("user", "root")
    dbProps.put("password", "sgrsgrsgr")
    dbProps.put("driver", "com.mysql.cj.jdbc.Driver")
    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/RedBull?useUnicode=true&characterEncoding=utf8",
      "sales", partitions, dbProps)
    tableDf.rdd.getNumPartitions
  }

  def method3(): Int = {
    val dbProps = new Properties()
    dbProps.put("user", "root")
    dbProps.put("password", "sgrsgrsgr")
    dbProps.put("driver", "com.mysql.cj.jdbc.Driver")
    val tableDf = spark.read.jdbc("jdbc:mysql://localhost:3306/RedBull?useUnicode=true&characterEncoding=utf8",
      "sales", "Id", 0, 1500, 15, dbProps)
    tableDf.rdd.getNumPartitions
  }

}
