package com.msdnfaq.bigdata.userprofile

import com.msdnfaq.bigdata.utils.{ConfigUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField}
import org.slf4j.LoggerFactory

import scala.util.Random

object InitGenerator {
  private val logger = LoggerFactory.getLogger(InitGenerator.getClass)

  def initUserBaseFeature(ss: SparkSession, params: ConfigUtils): Unit = {
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

    if (params.env.trim.equalsIgnoreCase("dev")) {
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
    } else if (params.env.trim.equalsIgnoreCase("prod")) {

      val limit = params.limit
      val users = new Array[UserProfile](limit)
      for (i <- 0 to limit - 1) {
        users.update(i, randomUser(i))
      }
      val userDF = ss.createDataFrame[UserProfile](users)
      userDF.writeTo("hive.dwd.dwd_user_base_feature").append() // 使用DataFrameWriterV2 API  spark3.0
      //userDF.write.format("iceberg").mode("append").save("hive.dwd.dwd_user_base_feature") //使用DataFrameWriterV1 API spark2.4
    } else {
      logger.warn("do not notice env")
    }

  }

  def randomUser(num: Int): UserProfile = {
    //随机用户名
    val xingRandom = Array(
      "高",
      "张",
      "王",
      "李",
      "赵")
    val email_suffix_Random = Array(
      "@163.com",
      "@qq.com",
      "@gmail.com",
      "@yahoo.com")

    val v1 = new Random().nextInt(xingRandom.size - 1)
    val v2 = new Random().nextInt(email_suffix_Random.size - 1)
    val email = xingRandom(v1) + num + email_suffix_Random(v2)

    val genderRandom = Array(
      "男",
      "女")
    val v3 = new Random().nextInt(genderRandom.size - 1)
    val gender = genderRandom(v3)
    val age = new Random().nextInt(80)

    //随机电话号码
    val phonePrefix = Array(
      "138",
      "139",
      "136",
      "135",
      "137"
    )
    val v4 = new Random().nextInt(phonePrefix.size - 1)

    val str = "0123456789";
    val random1 = new Random();
    //指定字符串长度，拼接字符并toString
    var sb = new StringBuilder();
    for (i <- 0 to 8) {
      //获取指定长度的字符串中任意一个字符的索引值
      var number = random1.nextInt(str.length());
      //根据索引值获取对应的字符
      val charAt = str.charAt(number);
      sb.append(charAt);
    }
    val phoneNum = phonePrefix(v4) + sb.toString();

    //随机网络制式
    val networksRandom = Array(
      "wifi",
      "4g",
      "5g",
      "3g",
      "2g"
    )
    val v5 = new Random().nextInt(networksRandom.size - 1)
    val network = networksRandom(v5)

    //电话类型
    val phoneModel = Array(
      ("iphone", "iphone13"),
      ("iphone", "iphone12"),
      ("iphone", "iphone11"),
      ("android", "huawei met13"),
      ("android", "oppo 9"),
      ("android", "oppo 10"),
      ("android", "neo 3"),
      ("android", "neo 4"),
      ("android", "xiaomi 13"),
      ("android", "xiaomi 14")
    )
    val v6 = new Random().nextInt(phoneModel.size - 1)
    val phone = phoneModel(v6)

    UserProfile(
      1000 + num,
      gender,
      age,
      phoneNum,
      email,
      network,
      phone._1,
      phone._2)
  }

  case class UserProfile(distinct_id: Int, gender: String, age: Int, phonenum: String, email: String, network_type: String, phone_type: String, phone_model: String)

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
    InitGenerator.initUserBaseFeature(ss, params)

    ss.stop()
    logger.warn("init done...")
  }
}
