package com.sgr.demand

import com.sgr.util.GetData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit, max}

/**
 * @author sunguorui
 * @date 2021年10月21日 12:52 下午
 */
object Car {
  def Winning_Rate(): DataFrame = {
    // 需求 1：计算中签率与倍率之间的量化关系
    val luckyDogsDF: DataFrame = GetData.luckyNumbersDf
    val applyNumbersDF: DataFrame = GetData.applyNumbersDF

    // 过滤2016年以后的中签数据，且仅抽取中签号码carNum字段
    val filterLuckyDogs: DataFrame = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")
    // 摇号数据与中签数据做内关联，Join Key为中签号码carNum
    val joinDf: DataFrame = applyNumbersDF.join(filterLuckyDogs, Seq("carNum"), "inner")
    // 统计中签者的倍率 同一个号码在不同的批次中签率是不同的
    // |  201706|0007100221303|         9|
    // |  201705|0007100221303|         9|
    // |  201704|0007100221303|         9|
    val multipliers: DataFrame = joinDf.groupBy(col("batchNum"), col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))

    // 只需要关注中签者在那个倍率下中的签即可  也就是中签号码的最大倍率
    val uniqueMultipliers: DataFrame = multipliers.groupBy("carNum")
      .agg(max("multiplier").alias("multiplier"))

    val result: DataFrame = uniqueMultipliers.groupBy(col("multiplier"))
      .agg(count(lit(1)).alias("cnt"))
      .orderBy("multiplier")
    result
  }
}
