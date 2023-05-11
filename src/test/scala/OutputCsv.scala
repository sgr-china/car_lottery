import com.sgr.spark.SparkSQLEnv
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

import java.util

/**
 * @author sunguorui
 * @date 2022年07月04日 2:16 下午
 */
object OutputCsv {
  SparkSQLEnv.init()
  val spark = SparkSQLEnv.sparkSession

  def main(args: Array[String]): Unit = {

    val data = new util.ArrayList[String]()
    data.add("hello")
    data.add(null)

    val ls = new util.ArrayList[Row]()
    val row = RowFactory.create(data.toArray)
    ls.add(row)

    val datatype = new util.ArrayList[DataType]()
    datatype.add(DataTypes.StringType)
    datatype.add(DataTypes.IntegerType)
    val header = new util.ArrayList[String]()
    header.add("Field_1_string")
    header.add("Field_1_integer")

    val structField1 = StructField(header.get(0), datatype.get(0), true, org.apache.spark.sql.types.Metadata.empty)

    val structField2 = StructField(header.get(1), datatype.get(1), true, org.apache.spark.sql.types.Metadata.empty)
    val structFieldsList = Array(structField1, structField2)

    val schema = new StructType(structFieldsList)

    val dataset = spark.createDataFrame(ls, schema)

    dataset.show()
  }
}
