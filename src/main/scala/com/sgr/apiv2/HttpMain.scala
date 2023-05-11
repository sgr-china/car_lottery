package com.sgr.apiv2

/**
 * @author sunguorui
 * @date 2022年07月05日 6:29 下午
 */
object HttpMain {

  def main(args: Array[String]): Unit = {
    new WebServer().startServer("localhost", 8888)
  }

}
