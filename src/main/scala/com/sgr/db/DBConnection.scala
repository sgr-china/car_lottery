package com.sgr.db

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.sgr.util.Conf

/**
 * @author sunguorui
 * @date 2021年10月21日 12:05 下午
 */
object DBConnection {
  val ds = new ComboPooledDataSource
  init()
  def databasePool: Connection = {
    ds.getConnection
  }

  def resetPoolManager() {
    ds.resetPoolManager()
  }

  private def init() {
    val poolSize = Conf.databasePoolSize

    val (driverClass, dbUrl, user, password) = loadConf
    ds.setDriverClass(driverClass)
    ds.setJdbcUrl(dbUrl)
    ds.setUser(user)
    ds.setPassword(password)
    ds.setMinPoolSize(poolSize)
    ds.setMaxPoolSize(if (poolSize < 5) 5 else poolSize + 2)
    ds.setAcquireIncrement(5)
    // 自动重连机制，防止长时间不用数据库连接断开
    ds.setTestConnectionOnCheckin(true)
    ds.setIdleConnectionTestPeriod(300)
  }

  private def loadConf = {
    val driver = Conf.databaseDriver
    val url = Conf.databaseUrl
    val user = Conf.databaseUser
    val password = Conf.databasePassword
    (driver, url, user, password)
  }
}
