package com.msdnfaq.bigdata.userprofile

import com.msdnfaq.bigdata.utils.{ConfigUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField}
import org.slf4j.LoggerFactory

object InitGenerator {
  private val logger = LoggerFactory.getLogger(InitGenerator.getClass)

  def initUserBaseFeature(ss: SparkSession): Unit = {
    logger.warn("init UserBaseFeature...")
    //val path = InitGenerator.getClass.getResource("data.csv").getPath
    //val path = Thread.currentThread().getContextClassLoader.getResource("data.csv").getPath
    val path = "/data/data.csv"
    //logger.warn("hdfs path: " + path)
    //建库
    logger.warn("create db...")
    var dbCreateSql =
      """
        |create database if not exists hive.dwd
        |""".stripMargin

    ss.sql(dbCreateSql)

    //清空数据
    logger.warn("drop table...")
    var dropTableSql =
      """
        |drop table if exists hive.dwd.dwd_user_base_feature
        |""".stripMargin
    ss.sql(dropTableSql)

    //建表
    logger.warn("create table...")
    var createTableSql =
      """
        |create table if not exists hive.dwd.dwd_user_base_feature (
        |distinct_id string,
        |gender string,
        |age string,
        |phonenum string,
        |email string,
        |network_type string,
        |phone_type string,
        |phone_model string
        |)
        |using iceberg
        |""".stripMargin

    ss.sql(createTableSql)

    //插入数据
    logger.warn("insert table...")

    val userBaseFeatureDF = ss.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("sep", ",") //数据的分隔符
      .option("header", "true") //首行是否字段元数据
      .load(path)

    userBaseFeatureDF.show()

    userBaseFeatureDF.cache()
    userBaseFeatureDF.write
      .format("iceberg")
      .mode(SaveMode.Overwrite)
      .saveAsTable("hive.dwd.dwd_user_base_feature")
    //userBaseFeatherDF.show()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN) // 将日志的级别调整减少不必要的日志显示在控制台

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
    InitGenerator.initUserBaseFeature(ss)

    ss.stop()
    logger.warn("init done...")
  }
}
