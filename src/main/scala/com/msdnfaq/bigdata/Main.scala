package com.msdnfaq.bigdata

import com.msdnfaq.bigdata.userprofile.InitGenerator
import com.msdnfaq.bigdata.utils.{ConfigUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN) // 将日志的级别调整减少不必要的日志显示在控制台
    try {
      //一、初始化数据
      //1. 解析参数
      val params: ConfigUtils = ConfigUtils.parseConfig(InitGenerator, args)
      logger.info("job is running, please wait for a moment")

      //2. 获取到sparkSession
      val ss: SparkSession = SparkUtils.getSparkSession(
        params.env,
        InitGenerator.getClass.getSimpleName)
      //2.1 设置spark操作的参数:读取hudi和hdfs的时候采取指定的过滤器来消除读取表的时候的无用的信息
      ss.sparkContext.hadoopConfiguration
        .setClass("mapreduce.input.pathFilter.class",
          classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter],
          classOf[org.apache.hadoop.fs.PathFilter])

      import ss.implicits._

      //初始化用户基础表
      InitGenerator.initUserBaseFeature(ss, params)

      //初始化用户行为表
      InitGenerator.initUserAction(ss, params)

      ss.stop()
      logger.warn("init done...")
    } catch {
      case ex: Exception => {
        logger.error("Main.main error: " + ex.getMessage, ex)
      }
    }

  }
}
