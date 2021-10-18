package com.sgr.util

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.config.ConfigException.Missing
import org.apache.log4j.PropertyConfigurator

object Conf {
  private val confDir = {
    val path = System.getProperty("mobius.conf.dir")
    if (path == null) {
      "conf/"
    } else {
      path + "/"
    }
  }

  def init(): Config = {
    // log4j
    PropertyConfigurator.configure(s"${confDir}log4j.properties")

    val currentUser = System.getProperty("user.name")
    val userConfig = s"$confDir$currentUser.conf"
    val specialConfigFile = if (new File(userConfig).exists()) userConfig else s"${confDir}application.conf"
    val specialConfig = ConfigFactory.parseFile(new File(specialConfigFile))
    val commonConfig = ConfigFactory.parseFile(new File(s"${confDir}common.conf"))

    val cusConfigFile = new File(s"${confDir}cus.conf")
    val conf = if (cusConfigFile.exists()) {
      val cusConfig = ConfigFactory.parseFile(cusConfigFile)
      ConfigFactory.load(cusConfig).withFallback(specialConfig).withFallback(commonConfig)
    } else {
      ConfigFactory.load(specialConfig).withFallback(commonConfig)
    }

    conf
  }

  def parseSparkConf(): Map[String, String] = {
    val sparkExtraPath = s"${confDir}spark-extra.conf"
    getPropertiesFromFile(sparkExtraPath)
  }

  def getPropertiesFromFile(fileName: String): Map[String, String] = {
    val file = new File(fileName)

    if (!file.exists) {
      Map[String, String]()
    } else {
      if (!file.isFile) {
//        throw new MobiusException(s"Properties file $file is not a normal file")
      }

      val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
      try {
        val properties = new Properties()
        properties.load(inReader)
        properties.stringPropertyNames().asScala.map(
          k => (k, properties.getProperty(k).trim)).toMap
      } catch {
        case e: IOException =>
//          throw new MobiusException(s"Failed when loading Spark properties from $fileName: $e")
          throw e
      } finally {
        inReader.close()
      }
    }
  }

  private val conf = init()

  private def getConf[T](name: String, default: T): T = {
    try {
      conf.getValue(name).unwrapped().asInstanceOf[T]
    } catch {
      case _: Missing => default
    }
  }

  private def getConf[T](name: String): T = {
    conf.getValue(name).unwrapped().asInstanceOf[T]
  }

  val mobiusServerHost = getConf[String]("server.host")
  val mobiusServerPort = getConf[Int]("server.port")
  val mobiusServerMaxThreads = getConf[Int]("server.maxThreads")

  val mobiusAppName = getConf[String]("app.name", "mobius")
  val sparkMachineCount = getConf[Int]("app.machineCount")
  val isMergeContentCheck = getConf[Boolean]("app.contentCheck")
  val mergeContentCheckLimit = getConf[Int]("app.contentCheckLimit", 10000)
  val csvColumnCnt = getConf[Int]("app.csvColumnCnt", 10240)
  val combineFilePartFileFactor = getConf[Double]("app.combinePartFileFactor")
  val sparkAutoBrodcastJoinThreshold = getConf[Int]("app.autoBroadcastJoinThreshold")
  val mobiusMaxSqlSize = getConf[Int]("app.maxSqlSize")
  val errorOpen = getConf[Boolean]("error.open", false)
  val redisKey = getConf[String]("error.redisKey", "m:view:ec:")
  val ignoreError = getConf[util.ArrayList[String]]("error.ignoreError")
  val shufflePartitions = getConf[Boolean]("app.shufflePartitions", true)
  val minPartFileLen = getConf[Int]("app.minPartFileLen", 10485760)
  val shufflePartitionCount = getConf[Int]("app.shufflePartitionCount", 128)   // 超过128，不适用shuffe操作
  val maxPartitionCount = getConf[Int]("app.maxPartitionCount", 2000)   // 按天分区可以存5年的数据了
  val divideDataOfBatch = getConf[Int]("app.divideDataOfBatch", 2048).toLong * 1024 * 1024 // M cast to byte
  val divideBatchThreshold = getConf[Int]("app.divideBatchThreshold", 10)
  val mergeTableExpireTime = getConf[Int]("app.mergeTableExpireTime", 7200)
  val ignoreExcelColTypeError = getConf[Boolean]("app.ignoreExcelColTypeError", false)

  // monitor begin
  val sparkJobTimeout = getConf[Int]("monitor.sparkJobTimeout", 3000).toLong // sparkJob超时时间, 秒
  val mergeJobTimeout = getConf[Int]("monitor.mergeJobTimeout", 3000).toLong // mergeJob超时时间, 秒
  val activeJobThreshold = getConf[Int]("monitor.activeJobThreshold", 80) // activeSparkJob上限
  val durationForCheck = getConf[Int]("monitor.durationForCheck", 1800).toLong // job执行多久才列入检测范围, 秒
  val blockJobThreshold = getConf[Int]("monitor.blockJobThreshold", 5) // 卡住任务上限
  val commands = { // 如果mobius卡住执行的命令
    import scala.collection.JavaConverters._
    val defaultCommand = new util.ArrayList[String]()
    defaultCommand.add("./bin/restart.sh")
    getConf[util.ArrayList[String]]("monitor.commandsIfBlock", defaultCommand).asScala.toArray
  }

  val maxSkewScale = getConf[Int]("monitor.maxSkewScale", 100) // 最大任务倾斜倍数
  val expandCheckEnabled = getConf[Boolean]("monitor.expandCheckEnabled", true) // 膨胀检测是否开启
  val maxExpandRatio = getConf[Int]("monitor.maxExpandRatio", 10) // 最大膨胀比例
  val minNumInput = getConf[Int]("monitor.minNumInput", 100000) // 膨胀检测最小输入值
  val loadBalanceOn = getConf[Boolean]("monitor.loadBalanceOn", false) // 合表负载均衡策略
  val maxRunningJob = getConf[Int]("monitor.maxRunningJob", 16) // 配合负载均衡使用, 最大执行Job个数, 大于返回too busy 错误
  val memUsedPercentThreshold = getConf[Int]("monitor.memUsedPercentThreshold", 95) // 内存使用上限（百分比）
  // monitor end

  val hiveAuth = getConf[Boolean]("hive.auth", false)
  val hiveDbName = getConf[String]("hive.db")
  val hiveUrl = getConf[String]("hive.url")
  val hiveDataDir = getConf[String]("hive.data-dir")
  val hiveWebUrl = getConf[String]("hive.web.url", "http://172.16.34.148:3000/hive/query")
  val hiveMaxTableCountOfView = getConf[Int]("hive.maxTableCountOfView")
  val hiveMacTableSizeOfView = getConf[Long]("hive.maxTableSizeOfView", 21474836480L)
  val hiveWarehousePath = getConf[String]("hive.warehouse")
  val hiveVersion = getConf[String]("hive.version")
  val hiveThriftServerEnabled = getConf[Boolean]("hive.thriftServer.enabled", false)

  val databasePoolSize = getConf[Int]("db.poolSize")
  val databaseDriver = getConf[String]("db.driver")
  val databaseUrl = getConf[String]("db.url")
  val databaseUser = getConf[String]("db.user")
  val databasePassword = getConf[String]("db.password")

  /**
   * 合表任务并发上限
   */
  val maxViewTask: Int = getConf[Int]("job.view.maxTask", 10)
  // 合表任务等待超时时间，代码里没用到
  val viewTaskWaitTimeout = getConf[Int]("job.view.waitTimeout")
  val skewDetectionThreshold = getConf[Int]("job.view.skewDetectionThreshold", 500000)
  val expandingDetectionThreshold = getConf[Int]("job.view.expandingDetectionThreshold", 10000000)
  val skewThreshold = getConf[Double]("job.view.skewThreshold", 3)
  val expandingThreshold = getConf[Double]("job.view.expandingThreshold", 5)
  val jobInterruptOnCancel = getConf[Boolean]("job.view.interruptOnCancel", false)
  val maxLimitParam = getConf[Int]("job.view.maxLimitParam", 8) // 限制是8位数，也就是1000万

  val dataCountLevel1 = getConf[Int]("partFiles.dataCountLevel1", 10000)
  val dataCountLevel2 = getConf[Int]("partFiles.dataCountLevel2", 200000)
  val dataCountLevel3 = getConf[Int]("partFiles.dataCountLevel3", 1000000)
  val minPartFiles = getConf[Int]("partFiles.minCount", 2)
  val maxPartFiles = getConf[Int]("partFiles.maxCount", 1000)
  val partFileMinRecordNum = getConf[Int]("partFiles.minRecordNum", 100000)

  val redisHost = getConf[String]("redis.host")
  val redisPort = getConf[Int]("redis.port")
  val redisPassword = getConf[String]("redis.password", "")
  val redisSentinelEnabled = getConf[Boolean]("redis.sentinel.enabled", true)
  val redisSentinelMaterId = if (redisSentinelEnabled) getConf[String]("redis.sentinel.masterId") else null
  val redisSentinelHostPort = {
    if (redisSentinelEnabled) {
      val cluterList = getConf[util.ArrayList[String]]("redis.sentinel.cluster")
      val HostAndPosts = for (i <- 0 until cluterList.size()) yield {
        val str = cluterList.get(i).split(":")
        (str(0), str(1).toInt)
      }
      if (HostAndPosts.isEmpty) {
        throw new IllegalArgumentException("redis cluster list cannot be empty")
      }
      HostAndPosts
    } else {
      null
    }
  }
  val redisKeyPrefix = getConf[String]("redis.keyPrefix", "test")
  /**
   * 字段缓存过期时间，默认一天，可以设置为 -1 永不过期
   */
  val redisFieldExpireTime: Int = getConf[Int]("redis.fieldExpireTime", 86400)


  val sqoopExpire = getConf[Int]("sqoop.expire", 18000000)
  val sqoopOpen = getConf[Boolean]("sqoop.open", false)
  val sqoopPath = getConf[String]("sqoop.path", "")

  val poolOpen = getConf[Boolean]("sparkPool.open")
  val poolQueryName = getConf[String]("sparkPool.query")
  val poolViewName = getConf[String]("sparkPool.view")
  val poolMergeName = getConf[String]("sparkPool.merge")

  val loadCurrent = getConf[Boolean]("app.loadCurrent", false)
  val enable_patch = getConf[Boolean]("app.enable_patch", true)
  val forceRepartition = getConf[Boolean]("app.forceRepartition", false)
  val dataSkewOptimizeEnabled = getConf[Boolean]("app.dataSkewOptimizeEnabled", true)

  val udtJarsDir = getConf[String]("udt.jarsDir", s"${System.getProperty("user.dir")}/libs")

  val mlModelJarsDir = getConf[String]("ml.model.jarsDir", s"${System.getProperty("user.dir")}/libs")

  val mappingTableDB = getConf[String]("mappingTable.db", "bdp_external")


  // kafka
  val kafkaServers = getConf[String]("kafka.kafkaServers")
  val zkUrl = getConf[String]("kafka.zkUrl")
  val kafkaGroupIdPrefix = getConf[String]("kafka.groupIdPrefix", "bdp")
  val kafkaEnabledKerberos = getConf[Boolean]("kafka.kerberos.enabled", false)
  val kafkaKerberosDomain = Option(getConf[String]("kafka.kerberos.domain", null))
  val kafkaKerberosKinitCmd = getConf[String]("kafka.kerberos.kinitCmd", "kinit")

  // streaming
  val previewRecordRatio = getConf[Int]("streaming.preview.recordRatio", 2)

  // geomesa
  val geomesaZookeeper = getConf[String]("geomesa.zookeeper")

  // export
  val exportMode = getConf[String]("export.mode", "distributed")

  val streamingHdfsMergerInterval = getConf[Int]("streaming.hdfs.mergerInterval", 60 * 30)
  // minio
  val minioEndpoint = getConf[String]("minio.endpoint")
  val minioport = getConf[Int]("minio.port")
  val minioAccessKey = getConf[String]("minio.accessKey")
  val minioSecretKey = getConf[String]("minio.secretKey")
  val minioBucketName = getConf[String]("minio.bucketName", "bdp")
  val minioRegion = getConf[String]("minio.region", null)
  val minioSecure = getConf[Boolean]("minio.existsHttp", true)

  val tableLockTimeOut = getConf[Int]("task.tableLockTimeOut", 600000).toLong // 锁表时间600s

  val prefixExportDataToKafka = getConf[String]("prefix.exportDataToKafka", "prefix:exportDataToKafka")

  val prefixExportDataToHive = getConf[String]("prefix.exportDataToHive", "prefix:exportDataToHive")
  val waitTimeToHive = getConf[Int]("job.hive.waitTime", 6000000).toLong

  val maxOffsetsPerTrigger = getConf[Int]("kafka.maxOffsetsPerTrigger", 10000)

  /**
   * 异步处理计算任务队列，目前只有合表任务是通过异步计算，所以这里默认值使用合表的并发控制
   * 以后如果有其他的计算任务也需要通过异步执行，可能需要改写这部分，每种计算任务队列单独做并发控制
   */
  val asyncRequestMaxTask: Int = getConf[Int]("request.async.maxTask", maxViewTask)
}
