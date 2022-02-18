package com.sgr.MachineLearning

import com.sgr.util.{LazyLogging, SparkSQLEnv}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

/**
 * @author sunguorui
 * @date 2022年02月17日 5:45 下午
 */
object HousePrediction extends LazyLogging{

  private val sparkSession = SparkSQLEnv.sparkSession

  def main(args: Array[String]): Unit = {
    val rootPath: String = "/house-prices"
    val filePath: String = s"$rootPath/train.csv"

    val trainDF: DataFrame = sparkSession.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", true).load(filePath)

//    trainDF.show()
//    trainDF.printSchema()

    // 提取用于训练的特征字段与预测标的（房价SalePrice）
    val selectedFields: DataFrame = trainDF.select("LotArea", "GrLivArea", "TotalBsmtSF", "GarageArea", "SalePrice")

    // 将所有字段都转换为整型Int
    val typedFields = selectedFields
      .withColumn("LotAreaInt", col("LotArea").cast(IntegerType)).drop("LotArea")
      .withColumn("GrLivAreaInt", col("GrLivArea").cast(IntegerType)).drop("GrLivArea")
      .withColumn("TotalBsmtSFInt", col("TotalBsmtSF").cast(IntegerType)).drop("TotalBsmtSF")
      .withColumn("GarageAreaInt", col("GarageArea").cast(IntegerType)).drop("GarageArea")
      .withColumn("SalePriceInt", col("SalePrice").cast(IntegerType)).drop("SalePrice")


    // 待捏合的特征字段集合
    val features: Array[String] = Array("LotAreaInt", "GrLivAreaInt", "TotalBsmtSFInt", "GarageAreaInt")

    // 准备“捏合器”，指定输入特征字段集合，与捏合后的特征向量字段名
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")

    // 调用捏合器的transform函数，完成特征向量的捏合
    val featuresAdded: DataFrame = assembler.transform(typedFields)
      .drop("LotAreaInt")
      .drop("GrLivAreaInt")
      .drop("TotalBsmtSFInt")
      .drop("GarageAreaInt")

    // 把训练样本成比例地分成两份，一份用于模型训练，剩下的部分用于初步验证模型效果。
    val Array(trainSet, testSet) = featuresAdded.randomSplit(Array(0.7, 0.3))

    // 构建线性回归模型，指定特征向量、预测标的与迭代次数
    val lr = new LinearRegression()
      // 指定预测标的字段
      .setLabelCol("SalePriceInt")
      // 特征向量字段
      .setFeaturesCol("features")
      // 指定模型训练的迭代次数
      .setMaxIter(10)

    // 使用训练集trainSet训练线性回归模型
    val lrModel = lr.fit(trainSet)

    val trainingSummary = lrModel.summary
    // scalastyle:off println
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
  }

}
