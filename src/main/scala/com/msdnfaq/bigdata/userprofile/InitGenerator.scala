package com.msdnfaq.bigdata.userprofile

import com.msdnfaq.bigdata.utils.{ConfigUtils, SparkUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util
import scala.beans.BeanProperty
import scala.util.Random

object InitGenerator {
  private val logger = LoggerFactory.getLogger(InitGenerator.getClass)

  def initUserAction(ss: SparkSession, params: ConfigUtils) = {
    logger.warn("init UserAction...")

    val dbSuffix = params.dbSuffix

    //    var dbName = ""
    //    if (params.env.trim.equalsIgnoreCase("dlc_prod")) {
    //      dbName = "DataLakeCatalog.dwd_" + dbSuffix
    //    }else{
    //      dbName = "dwd_" + dbSuffix
    //    }
    val dbName = "DataLakeCatalog.dwd_" + dbSuffix
    //建库
    logger.warn("create db...")
    var dbCreateSql =
      "create database if not exists " + dbName.stripMargin
    ss.sql(dbCreateSql)

    val tableName = dbName + ".dwd_user_action"
    //清空数据
    if (!params.dbAppend) {
      logger.warn("drop table...")
      var dropTableSql =
        "drop table if exists " + tableName.stripMargin
      ss.sql(dropTableSql)
    }

    //建表
    logger.warn("create table...")
    var createTableSql =
      "create table if not exists " + tableName +
        """ (
          |distinct_id string,
          |user_id string,
          |version_code string,
          |lang string,
          |os string,
          |model string,
          |brand string,
          |sdk_version string,
          |height_width string,
          |network string,
          |lng string,
          |lat string,
          |action string,
          |phonenum string,
          |goodsid string,
          |create_time long
          |)
          |using iceberg
          |""".stripMargin

    ss.sql(createTableSql)

    //插入数据
    logger.warn("insert table...")

    if (params.env.trim.equalsIgnoreCase("dlc_prod")) {

      //val limit = params.limit
      val limit = 1000
      val userActions: util.List[UserAction] = new util.ArrayList[UserAction](limit)
      for (i <- 0 to limit - 1) {
        //userActions.update(i, randomUserAction(i))
        userActions.add(i, randomUserAction(i))
      }
      val userDF = ss.createDataFrame(userActions, classOf[UserAction])
      if (!params.dbAppend) {
        userDF.writeTo(tableName).overwritePartitions() // 使用DataFrameWriterV2 API  spark3.0
        //userDF.write.format("iceberg").mode("append").save("DataLakeCatalog.dwd.dwd_user_base_feature") //使用DataFrameWriterV1 API spark2.4
      } else {
        userDF.writeTo(tableName).append()
      }
    } else {
      logger.warn("do not notice env")
    }
  }

  /**
   * 初始化用户画像宽表数据
   *
   * @param ss
   * @param params
   */
  def initUserBaseFeature(ss: SparkSession, params: ConfigUtils): Unit = {

    try {
      logger.warn("init UserBaseFeature...")
      //val path = InitGenerator.getClass.getResource("data.csv").getPath
      //val path = Thread.currentThread().getContextClassLoader.getResource("data.csv").getPath
      val path = "/data/data.csv"
      //logger.warn("hdfs path: " + path)
      //建库
      logger.warn("create db...")
      val dbSuffix = params.dbSuffix
      //    var dbName = ""
      //
      //    if (params.env.trim.equalsIgnoreCase("dlc_prod")) {
      //      dbName = "DataLakeCatalog.dwd_" + dbSuffix
      //    } else {
      //      dbName = "dwd_" + dbSuffix
      //    }

      val dbName = "DataLakeCatalog.dwd_" + dbSuffix

      val dbCreateSql = "create database if not exists " + dbName.stripMargin
      ss.sql(dbCreateSql)

      val tableName = dbName + ".dwd_user_base_feature"
      //清空数据
      if (!params.dbAppend) {
        logger.warn("drop table...")
        var dropTableSql =
          "drop table if exists " + tableName
        ss.sql(dropTableSql)
      }

      //建表
      logger.warn("create table...")
      var createTableSql =
        "create table if not exists " + tableName +
          """ (
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
      logger.warn("begin insert table " + tableName)

      if (params.env.trim.equalsIgnoreCase("dev")) {
        logger.warn("env dev start...")
        logger.warn("reading from csv data...")
        val userBaseFeatureDF = ss.read
          .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
          .option("sep", ",") //数据的分隔符
          .option("header", "true") //首行是否字段元数据
          .load(path)

        userBaseFeatureDF.show()
        userBaseFeatureDF.cache()
        if (!params.dbAppend) {
          logger.warn("begin overwrite to iceberg table...")
          userBaseFeatureDF.write
            .format("iceberg")
            .mode(SaveMode.Overwrite)
            .saveAsTable(tableName)

          logger.warn("write to iceberg table done...")
        } else {
          logger.warn("begin append to iceberg table...")
          userBaseFeatureDF.write
            .format("iceberg")
            .mode(SaveMode.Append)
            .saveAsTable(tableName)
          logger.warn("write to iceberg table done...")
        }
        logger.warn("env dev end...")

        //userBaseFeatherDF.show()
      } else if (params.env.trim.equalsIgnoreCase("dlc_prod")) {
        logger.warn("env prod start...")
        val limit = params.limit
        logger.warn("making data: " + limit.toString)
        //Minimum partition unit
        val once = 100000
        //The remaining quantity is initially equal to 'limit' from input paramers
        var leftcnt = limit

        var users: util.List[UserProfile] = new util.ArrayList[UserProfile]()
        do {
          logger.warn("begin create Array...")
          var arrayCnt = 0
          if (leftcnt >= once) {
            arrayCnt = once
          } else {
            arrayCnt = leftcnt
          }
          users = new util.ArrayList[UserProfile](arrayCnt)

          for (i <- 0 to arrayCnt - 1) {
            //users.update(i, randomUser(i))
            users.add(i, randomUser(i))
          }
          logger.warn("create Array done...")

          logger.warn("begin create RDD...")

          val userDF = ss.createDataFrame(users, classOf[UserProfile])
          logger.warn("create RDD done...")
          if (!params.dbAppend) {
            logger.warn("begin overwrite iceberg table...")
            userDF.writeTo(tableName).overwritePartitions() // 使用DataFrameWriterV2 API  spark3.0
            //userDF.write.format("iceberg").mode("append").save("DataLakeCatalog.dwd.dwd_user_base_feature") //使用DataFrameWriterV1 API spark2.4
            logger.warn("overwrite iceberg table done...")
          } else {
            logger.warn("begin append iceberg table...")
            userDF.writeTo(tableName).append()
            logger.warn("overwrite iceberg table done...")
          }

          leftcnt = leftcnt - once
        }
        while (leftcnt > 0)

      } else {
        logger.warn("do not notice env")
      }
      logger.warn("insert table " + tableName + " done")
    }
    catch {
      case ex: Exception => {
        logger.error("InitGenerator.initUserBaseFeature error: " + ex.getMessage, ex)
      }
    }
  }

  def randomUser(num: Int): UserProfile = {
    try {
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

      //用户ID
      val v7 = new Random().nextInt(1000)
      val userId = 1000000 + v7

      new UserProfile(
        userId,
        gender,
        age,
        phoneNum,
        email,
        network,
        phone._1,
        phone._2)
    } catch {
      case ex: Exception => {
        logger.error("InitGenerator.randomUser error: " + ex.getMessage, ex)
        null
      }
    }
  }

  case class UserProfile(@BeanProperty val distinct_id: Int, @BeanProperty val gender: String, @BeanProperty val age: Int, @BeanProperty val phonenum: String, @BeanProperty val email: String, @BeanProperty val network_type: String, @BeanProperty val phone_type: String, @BeanProperty val phone_model: String)

  def randomUserAction(num: Int): UserAction = {
    try {
      //版本
      val versionRandom = Array(
        "v1.0",
        "v1.4",
        "v1.4.1",
        "v2.0",
        "v2.1"
      )
      val v1 = new Random().nextInt(versionRandom.size - 1)
      val version = versionRandom(v1)

      //语言
      val langRandom = Array(
        "en",
        "chn",
        "jpn",
        "kro"
      )
      val v2 = new Random().nextInt(langRandom.size - 1)
      val lang = langRandom(v2)

      //电话类型
      val phoneModel = Array(
        ("iphone", "iphone", "13"),
        ("iphone", "iphone", "12"),
        ("iphone", "iphone", "11"),
        ("android", "huawei", "met13"),
        ("android", "oppo", "v9"),
        ("android", "oppo", "v10"),
        ("android", "neo", "v3"),
        ("android", "neo", "v4"),
        ("android", "xiaomi", "v13"),
        ("android", "xiaomi", "v14")
      )
      val v3 = new Random().nextInt(phoneModel.size - 1)
      val phone = phoneModel(v3)

      //屏幕尺寸
      val screemRondom = Array(
        ("1024", "768"),
        ("1920", "1360"),
        ("768", "456")
      )
      val v4 = new Random().nextInt(screemRondom.size - 1)
      val scream = screemRondom(v4)

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

      //经度
      val lngRandom = Array(
        "49",
        "100",
        "120",
        "78",
        "99"
      )
      val v6 = new Random().nextInt(lngRandom.size - 1)
      val lng = lngRandom(v6)

      //纬度
      val latRandom = Array(
        "564",
        "87",
        "256",
        "12",
        "90"
      )
      val v7 = new Random().nextInt(latRandom.size - 1)
      val lat = lngRandom(v7)

      //动作
      val actionRandom = Array(
        "注册",
        "加购",
        "下单",
        "退货",
        "评论"
      )
      val v8 = new Random().nextInt(actionRandom.size - 1)
      val action = lngRandom(v8)

      //随机电话号码
      val phonePrefix = Array(
        "138",
        "139",
        "136",
        "135",
        "137"
      )
      val v9 = new Random().nextInt(phonePrefix.size - 1)

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
      val phoneNum = phonePrefix(v9) + sb.toString();

      //用户ID
      val v10 = new Random().nextInt(1000)
      val userId = 1000000 + v10

      //随机电话号码
      val goodsRandom = Array(
        "1010",
        "1198",
        "18761",
        "1209",
        "4378"
      )
      val v11 = new Random().nextInt(goodsRandom.size - 1)
      val goodID = goodsRandom(v11)

      new UserAction(
        userId,
        "0012001" + userId,
        version,
        lang,
        phone._1,
        phone._3,
        phone._2,
        "SDK_" + version,
        scream._1 + "_" + scream._2,
        network,
        lng,
        lat,
        action,
        phoneNum,
        goodID,
        System.currentTimeMillis()
      )
    }
    catch {
      case ex: Exception => {
        logger.error("InitGenerator.randomUserAction error: " + ex.getMessage, ex)
        null
      }
    }
  }

  case class UserAction(@BeanProperty val distinct_id: Int, @BeanProperty val user_id: String, @BeanProperty val version_code: String, @BeanProperty val lang: String, @BeanProperty val os: String, @BeanProperty val model: String, @BeanProperty val brand: String, @BeanProperty val sdk_version: String, @BeanProperty val height_width: String, @BeanProperty val network: String, @BeanProperty val lng: String, @BeanProperty val lat: String, @BeanProperty val action: String, @BeanProperty val phonenum: String, @BeanProperty val goodsid: String, @BeanProperty val create_time: Long)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN) // 将日志的级别调整减少不必要的日志显示在控制台
    try {
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
        logger.error("InitGenerator.main error: " + ex.getMessage, ex)
      }
    }

  }
}
