package com.msdnfaq.bigdata.userprofile

import com.msdnfaq.bigdata.utils.{ConfigUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField}
import org.slf4j.LoggerFactory

object InitGenerator {
  private val logger = LoggerFactory.getLogger(InitGenerator.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN) // 将日志的级别调整减少不必要的日志显示在控制台
    //1. 解析参数
    val params: ConfigUtils = ConfigUtils.parseConfig(InitGenerator, args)
    logger.info("job is running, please wait for a moment")

    //val path = InitGenerator.getClass.getResource("data.csv").getPath
    val path = Thread.currentThread().getContextClassLoader.getResource("data.csv").getPath
    logger.warn("path:" + path)

    //2. 获取到sparkSession
    val ss: SparkSession = SparkUtils.getSparkSession(params.env, InitGenerator.getClass.getSimpleName)
    //2.1 设置spark操作的参数:读取hudi和hdfs的时候采取指定的过滤器来消除读取表的时候的无用的信息
    ss.sparkContext.hadoopConfiguration
      .setClass("mapreduce.input.pathFilter.class",
        classOf[org.apache.hudi.hadoop.HoodieROTablePathFilter],
        classOf[org.apache.hadoop.fs.PathFilter])

    import ss.implicits._

    //ss.sparkContext.textFile("file:///" + path + "data.json")
    ss.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("sep", ",") //数据的分隔符
      .option("header", "true") //首行是否字段元数据
      .load(path).show()

    ss.stop()
  }
}
