package com.sgr

import com.sgr.demand.Car
import com.sgr.spark.SparkSQLEnv


object Main {

  def main(args: Array[String]): Unit = {
    SparkSQLEnv.init()
    Car.Winning_Rate().show()
  }
}
