package com.sgr.test

object ScalaYield {

  def main(args: Array[String]): Unit = {
    //    val a = (1 to 10)
    //    val b = for (value <- a) yield value * 2
    //    println(b)

    val filesHere = Map("java" -> 22, "scala" -> 6, "spark" -> 5)
    val scalaFiles = for {
      file <- filesHere
      if file._1.startsWith("java")
      if file._2 == 22
    } yield file
    println(scalaFiles)
  }

}
