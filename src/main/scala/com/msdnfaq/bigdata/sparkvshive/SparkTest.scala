package com.msdnfaq.bigdata.sparkvshive

import com.msdnfaq.bigdata.utils.{ConfigUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkTest {
  private val logger = LoggerFactory.getLogger(SparkTest.getClass)

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

      import ss.implicits._
      InitGenerator.dws_cstm_new_user_order_conversion_data_df(ss, params)

      ss.stop()
      logger.warn("init done...")
    } catch {
      case ex: Exception => {
        logger.error("SparkTest.main error: " + ex.getMessage, ex)
      }
    }

  }
}
