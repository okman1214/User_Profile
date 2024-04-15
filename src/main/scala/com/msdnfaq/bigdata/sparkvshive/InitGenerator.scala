package com.msdnfaq.bigdata.sparkvshive

import com.msdnfaq.bigdata.utils.UtilTools.generateChineseName
import com.msdnfaq.bigdata.utils.{ConfigUtils, DataUtils, UtilTools}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.util
import scala.beans.BeanProperty
import scala.util.Random

object InitGenerator {
  private val logger = LoggerFactory.getLogger(InitGenerator.getClass)

  /**
   * dws_cstm_new_user_order_conversion_data_df
   *
   * @param ss
   * @param params
   */
  def dws_cstm_new_user_order_conversion_data_df(ss: SparkSession, params: ConfigUtils): Unit = {
    try {

      logger.warn("init dws_cstm_new_user_order_conversion_data_df...")
      val dbName = "dw"
      //建库
      logger.warn("create db...")
      var dbCreateSql =
        "create database if not exists " + dbName.stripMargin + " location 'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/'"
//      var dbCreateSql = "create database if not exists " + dbName.stripMargin
      ss.sql(dbCreateSql)

      var tableName = dbName + ".dws_cstm_new_user_order_conversion_data_df"
      //清空数据
      if (!params.dbAppend) {
        logger.warn("drop table...")
        var dropTableSql =
          "drop table if exists " + tableName.stripMargin
        ss.sql(dropTableSql)
      }

      //建表
      logger.warn("create table " + tableName + "...")
      var createTableSql =
        "create table if not exists " + tableName +
          """ (
            |first_date string,
            |product_name string,
            |act_user_type string,
            |course_type string,
            |user_num string,
            |within_0_days_user_num bigint,
            |within_1_days_user_num bigint,
            |within_7_days_user_num bigint,
            |within_30_days_user_num bigint,
            |within_0_days_user_rate decimal(10,6),
            |within_1_days_user_rate decimal(10,6),
            |within_7_days_user_rate decimal(10,6),
            |within_30_days_user_rate decimal(10,6)
            |)
            |PARTITIONED BY (dt STRING)
            |STORED AS PARQUET
            |""".stripMargin

      ss.sql(createTableSql)

      tableName = dbName + ".dwd_odr_detail_mb_df"

      //清空数据
      if (!params.dbAppend) {
        logger.warn("drop table...")
        var dropTableSql =
          "drop table if exists " + tableName.stripMargin
        ss.sql(dropTableSql)
      }

      //建表dwd_odr_detail_mb_df
      logger.warn("create table " + tableName + "...")

      createTableSql =
        "create table if not exists " + tableName +
          """ (
            |order_number bigint,
            |user_number bigint,
            |user_number_boss string,
            |create_time string,
            |source string,
            |paid_time string,
            |course_type string,
            |record_type string,
            |app_id string,
            |order_status string
            |)
            |PARTITIONED BY (dt STRING)
            |STORED AS PARQUET
            |""".stripMargin

      ss.sql(createTableSql)

      //插入数据
      logger.warn("insert table " + tableName + "...")

      if (params.env.trim.equalsIgnoreCase("dlc_prod") || params.env.trim.equalsIgnoreCase("dlc_hive_prod")) {

        val limit = params.limit
        val orderDetail: util.List[DwdOdrDetail] = new util.ArrayList[DwdOdrDetail](limit)
        for (i <- 0 to limit - 1) {
          //userActions.update(i, randomUserAction(i))
          orderDetail.add(i, randoDwdOdrDetail(i))
        }
        val userDF = ss.createDataFrame(orderDetail, classOf[DwdOdrDetail])
        if (!params.dbAppend) {
          userDF.write.format("parquet").mode("overwrite").saveAsTable(tableName) //使用DataFrameWriterV1 API spark2.4
        } else {
          userDF.write.format("parquet").mode("append").saveAsTable(tableName)
        }
      } else {
        logger.warn("do not notice env")
      }

      tableName = dbName + ".ods_gaotu_super_class_user"
      //清空数据
      if (!params.dbAppend) {
        logger.warn("drop table...")
        var dropTableSql =
          "drop table if exists " + tableName.stripMargin
        ss.sql(dropTableSql)
      }

      //建表ods_gaotu_super_class_user
      logger.warn("create table " + tableName + "...")

      //ods_gaotu_super_class_user
      createTableSql =
        "create table if not exists " + tableName +
          """ (
            |user_id bigint,
            |register_product string,
            |register_time bigint,
            |mobile string,
            |last_login bigint,
            |device_type string,
            |name string,
            |grade bigint,
            |sex int,
            |province string,
            |birthday string,
            |subject string
            |)
            |PARTITIONED BY (dt STRING)
            |STORED AS PARQUET
            |""".stripMargin

      ss.sql(createTableSql)

      //插入数据
      logger.warn("insert table " + tableName + "...")

      if (params.env.trim.equalsIgnoreCase("dlc_prod") || params.env.trim.equalsIgnoreCase("dlc_hive_prod")) {

        val limit = params.limit
        val superClassUser: util.List[OdsSuperClassUser] = new util.ArrayList[OdsSuperClassUser](limit)
        for (i <- 0 to limit - 1) {
          //userActions.update(i, randomUserAction(i))
          superClassUser.add(i, randomOdsSuperClassUser(i))
        }
        //logger.warn("superClassUser.get 1:" + superClassUser.get(1).toString)
        val userDF = ss.createDataFrame(superClassUser, classOf[OdsSuperClassUser])
        if (!params.dbAppend) {
          //logger.warn("superClassUser insertInto overwrite" + tableName)
          userDF.write.format("parquet").mode("overwrite").saveAsTable(tableName) //使用DataFrameWriterV1 API spark2.4
        } else {
          //logger.warn("superClassUser insertInto overwrite" + tableName)
          userDF.write.format("parquet").mode("append").saveAsTable(tableName)
        }
      } else {
        logger.warn("do not notice env")
      }

      tableName = dbName + ".dim_cstm_active_user_c_appliction_mb_df"
      //清空数据
      if (!params.dbAppend) {
        logger.warn("drop table...")
        var dropTableSql =
          "drop table if exists " + tableName.stripMargin
        ss.sql(dropTableSql)
      }

      logger.warn("create table " + tableName + "...")

      createTableSql =
        "create table if not exists " + tableName +
          """ (
            |user_number string,
            |product_name string,
            |event_time string,
            |first_date string,
            |first_app_version string,
            |appliction_name string
            |)
            |PARTITIONED BY (dt STRING)
            |STORED AS PARQUET
            |""".stripMargin

      ss.sql(createTableSql)

      //插入数据
      logger.warn("insert table " + tableName + "...")

      if (params.env.trim.equalsIgnoreCase("dlc_prod") || params.env.trim.equalsIgnoreCase("dlc_hive_prod")) {

        val limit = params.limit
        val dimCstmActiveUser: util.List[DimCstmActiveUser] = new util.ArrayList[DimCstmActiveUser](limit)
        for (i <- 0 to limit - 1) {
          dimCstmActiveUser.add(i, randomDimCstmActiveUser(i))
        }
        //logger.warn("insert table get 1:" + dimCstmActiveUser.get(1))
        val userDF = ss.createDataFrame(dimCstmActiveUser, classOf[DimCstmActiveUser])
        if (!params.dbAppend) {
          //logger.warn("insert table overwrite")
          userDF.write.format("parquet").mode("overwrite").saveAsTable(tableName) //使用DataFrameWriterV1 API spark2.4
        } else {
          //logger.warn("insert table append")
          userDF.write.format("parquet").mode("append").saveAsTable(tableName)
        }
      } else {
        logger.warn("do not notice env")
      }


      tableName = dbName + ".dim_mkt_source_df"
      //清空数据
      if (!params.dbAppend) {
        logger.warn("drop table...")
        var dropTableSql =
          "drop table if exists " + tableName.stripMargin
        ss.sql(dropTableSql)
      }

      logger.warn("create table " + tableName + "...")

      createTableSql =
        "create table if not exists " + tableName +
          """ (
            |source string,
            |channel_name string,
            |page_name string,
            |page_url string,
            |first_channel string,
            |second_channel string,
            |third_channel string,
            |appliction_name string
            |)
            |PARTITIONED BY (dt STRING)
            |STORED AS PARQUET
            |""".stripMargin

      ss.sql(createTableSql)

      //插入数据
      logger.warn("insert table " + tableName + "...")

      if (params.env.trim.equalsIgnoreCase("dlc_prod") || params.env.trim.equalsIgnoreCase("dlc_hive_prod")) {

        val limit = params.limit
        val dimMktSource: util.List[DimMktSource] = new util.ArrayList[DimMktSource](limit)
        for (i <- 0 to limit - 1) {
          dimMktSource.add(i, randomDimMktSource(i))
        }
        val userDF = ss.createDataFrame(dimMktSource, classOf[DimMktSource])
        if (!params.dbAppend) {
          userDF.write.format("parquet").mode("overwrite").saveAsTable(tableName) //使用DataFrameWriterV1 API spark2.4
        } else {
          userDF.write.format("parquet").mode("append").saveAsTable(tableName)
        }
      } else {
        logger.warn("do not notice env")
      }

      logger.warn("init dws_cstm_new_user_order_conversion_data_df done!")
    } catch {
      case ex: Exception => {
        logger.error("dws_cstm_new_user_order_conversion_data_df error: " + ex.getMessage, ex)
      }
    }
  }

  //dw.dim_cstm_active_user_c_appliction_mb_df
  case class DimCstmActiveUser(@BeanProperty val user_number: String,
                               @BeanProperty val product_name: String,
                               @BeanProperty val event_time: String,
                               @BeanProperty val first_date: String,
                               @BeanProperty val first_app_version: String,
                               @BeanProperty val appliction_name: String,
                               @BeanProperty val dt: String)

  def randomDimCstmActiveUser(num: Int): DimCstmActiveUser = {
    //user_number
    val userNumber = 1000 + new Random().nextInt(1000).toString

    //product_name
    val productNameRandom = Array(
      "途途",
      "高途",
      "其他"
    )
    val v1 = new Random().nextInt(productNameRandom.size - 1)
    val productName = productNameRandom(v1)

    //event_time
    val eventTimeRandom = 0 + new Random().nextInt(100)
    val eventDate = DataUtils.funAddDate("20240415", eventTimeRandom)
    val eventTime = DataUtils.funStringToTimeStamp(eventDate, "yyyyMMdd")/1000

    val firstDateRandom = 0 + new Random().nextInt(100)
    val firstDate = DataUtils.funAddDate(eventDate, eventTimeRandom)

    //first_app_version
    val firstAppVersionRandom = Array(
      "v1.0",
      "v2.1",
      "v3.5"
    )

    val v2 = new Random().nextInt(firstAppVersionRandom.size - 1)
    val firstAppVersion = firstAppVersionRandom(v2)

    //applicationName
    val applicationNameRandom = Array(
      "APP",
      "weixin",
      "m页",
      "小程序"
    )
    val v3 = new Random().nextInt(applicationNameRandom.size - 1)
    val applicationName = applicationNameRandom(v3)
    val dt = DataUtils.getrealTime("yyyyMMdd")

    new DimCstmActiveUser(userNumber,
      productName,
      eventDate,
      firstDate,
      firstAppVersion,
      applicationName,
      dt)

  }

  //dim_mkt_source_df
  case class DimMktSource(@BeanProperty val source: String,
                          @BeanProperty val channel_name: String,
                          @BeanProperty val page_name: String,
                          @BeanProperty val page_url: String,
                          @BeanProperty val first_channel: String,
                          @BeanProperty val second_channel: String,
                          @BeanProperty val third_channel: String,
                          @BeanProperty val appliction_name: String,
                          @BeanProperty val dt: String
                         )

  def randomDimMktSource(num: Int): DimMktSource = {
    //source
    val sourceRandom = Array(
      "gtfwh02",
      "gtfwh03",
      "gtfwh03"
    )
    val v1 = new Random().nextInt(sourceRandom.size - 1)
    val source = sourceRandom(v1)

    //channel_name
    val channelNameRandom = Array(
      "原子计划",
      "渠道联盟",
      "公众号",
      "信息流"
    )
    val v2 = new Random().nextInt(channelNameRandom.size - 1)
    val channelName = channelNameRandom(v2)

    //page_name
    val pageNameRandom = Array(
      "首页",
      "订单",
      "架构",
      "维护"
    )
    val v3 = new Random().nextInt(pageNameRandom.size - 1)
    val pageName = pageNameRandom(v3)

    val pageUrlRandom = Array(
      "homepage",
      "order",
      "arch",
      "take"
    )

    val pageUrl = pageUrlRandom(v3)

    val v4 = new Random().nextInt(channelNameRandom.size - 1)
    val firstChannel = channelNameRandom(v4)

    val v5 = new Random().nextInt(channelNameRandom.size - 1)
    val secondChannel = channelNameRandom(v5)

    val v6 = new Random().nextInt(channelNameRandom.size - 1)
    val thirdChannel = channelNameRandom(v6)

    //applicationName
    val applicationNameRandom = Array(
      "APP",
      "weixin",
      "m页",
      "小程序"
    )
    val v7 = new Random().nextInt(applicationNameRandom.size - 1)
    val applicationName = applicationNameRandom(v3)

    val dt = DataUtils.getrealTime("yyyyMMdd")

    new DimMktSource(source,
      channelName,
      pageName,
      pageUrl,
      firstChannel,
      secondChannel,
      thirdChannel,
      applicationName,
      dt)
  }

  //ods_gaotu_super_class_user
  case class OdsSuperClassUser(@BeanProperty val user_id: Long,
                               @BeanProperty val register_product: String,
                               @BeanProperty val register_time: Long,
                               @BeanProperty val mobile: String,
                               @BeanProperty val last_login: Long,
                               @BeanProperty val device_type: String,
                               @BeanProperty val name: String,
                               @BeanProperty val grade: Long,
                               @BeanProperty val sex: Int,
                               @BeanProperty val province: String,
                               @BeanProperty val birthday: String,
                               @BeanProperty val subject: String,
                               @BeanProperty val dt: String
                              )

  def randomOdsSuperClassUser(num: Int): OdsSuperClassUser = {
    //user_id
    val userId = 1000 + new Random().nextInt(1000)

    //register_product
    //注册学科
    val registerProductRandom = Array(
      "gaotu:tutu:ketang",
      "gaotu:tutu:zhiboke",
      "gaotu:tutu:chengrenjiaoyu"
    )
    val v1 = new Random().nextInt(registerProductRandom.size - 1)
    val registerProduct = registerProductRandom(v1)

    val registerTimeRandom = 0 + new Random().nextInt(100)
    val registerTime = (DataUtils.funStringToTimeStamp(DataUtils.funAddDate("20240415", registerTimeRandom), "yyyyMMdd"))/1000

    //随机电话号码
    val phonePrefix = Array(
      "138",
      "139",
      "136",
      "135",
      "137"
    )
    val v2 = new Random().nextInt(phonePrefix.size - 1)

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
    val mobile = phonePrefix(v2) + sb.toString();

    //lastLogin random btw 0 ~ 30 days
    val lastLoginRandom = 0 + new Random().nextInt(3600*24*30)
    //val lastLogin = (DataUtils.funStringToTimeStamp(DataUtils.funAddDate("20240415", lastLoginRandom), "yyyyMMdd"))/1000
    val lastLogin = registerTime + lastLoginRandom

    //device_type
    val phoneModel = Array(
      "iphone13",
      "iphone12",
      "iphone11",
      "huawei met13",
      "oppo 9",
      "oppo 10",
      "neo 3",
      "neo 4",
      "xiaomi 13",
      "xiaomi 14"
    )
    val v3 = new Random().nextInt(phoneModel.size - 1)
    val deviceType = phoneModel(v3)

    //name
    //随机用户名
    val name = UtilTools.generateChineseName

    //grade
    val grade = 1 + new Random().nextInt(10 - 1)

    //sex
    val sex = 1 + new Random().nextInt(2 - 1)

    //province
    val provinceRandom = Array(
      "北京-昌平",
      "北京-海淀",
      "北京-西城",
      "北京-房山",
      "上海-虹桥",
      "上海-杭州路",
      "上海-西广场",
      "广东-深圳",
      "广东-广州",
      "广东-东莞",
      "杭州-西湖"
    )
    val v4 = new Random().nextInt(provinceRandom.size - 1)
    val province = provinceRandom(v4)

    //birthday
    val birthdayRandom = -3000 + new Random().nextInt(3000)
    val birthday = DataUtils.funAddDate("20240415", birthdayRandom)

    //subject
    val subjectRandom = Array(
      "数学",
      "语文",
      "体育",
      "英语",
      "手工",
      "课程1",
      "课程2",
      "课程3",
      "课程4"
    )
    val v5 = new Random().nextInt(subjectRandom.size - 1)
    val subject = subjectRandom(v5)

    val dt = DataUtils.getrealTime("yyyyMMdd")

    new OdsSuperClassUser(
      userId,
      registerProduct,
      registerTime,
      mobile,
      lastLogin,
      deviceType,
      name,
      grade,
      sex,
      province,
      birthday,
      subject,
      dt
    )

  }

  //dwd_odr_detail_mb_df
  case class DwdOdrDetail(@BeanProperty val order_number: Long,
                          @BeanProperty val user_number: Long,
                          @BeanProperty val user_number_boss: String,
                          @BeanProperty val create_time: String,
                          @BeanProperty val source: String,
                          @BeanProperty val paid_time: String,
                          @BeanProperty val course_type: Int,
                          @BeanProperty val record_type: Int,
                          @BeanProperty val app_id: Int,
                          @BeanProperty val order_status: Int,
                          @BeanProperty val dt: String)

  def randoDwdOdrDetail(num: Int): DwdOdrDetail = {
    try {

      //order_number
      val orderNumber = 100 + new Random().nextInt(10000 - 100)

      //user_number
      val userNumber = 1000 + new Random().nextInt(1000)

      //user_number_boss
      val userNumberBoss = userNumber.toString

      //create_time random btw 1 ~ 30 days
      val createTimeRandom = 1 + new Random().nextInt(100)
      val creatTime = DataUtils.funAddDate("20240415", createTimeRandom)

      //source
      val source_arry = Array(
        "douyin",
        "weixin",
        "kuaishou",
        "wxgzh",
        "txshipin"
      )
      val v2 = new Random().nextInt(source_arry.size - 1)
      val source = source_arry(v2)

      //paid_time bigger than create_time 1 to 100
      val paidTimeRandom = 1 + new Random().nextInt(100)
      val paidTime = DataUtils.funAddDate(creatTime, paidTimeRandom)

      val courseType = 10 + new Random().nextInt(30 - 10)

      val recordType = 0 + new Random().nextInt(2 - 0)

      val appID = 0 + new Random().nextInt(5 - 0)

      val orderStatus = 0 + new Random().nextInt(10 - 0)

      val dt = DataUtils.getrealTime("yyyyMMdd")

      new DwdOdrDetail(
        orderNumber,
        userNumber,
        userNumberBoss,
        creatTime,
        source,
        paidTime,
        courseType,
        recordType,
        appID,
        orderStatus,
        dt
      )
    }
    catch {
      case ex: Exception => {
        logger.error("InitGenerator.randoDwdOdrDetail error: " + ex.getMessage, ex)
        null
      }
    }
  }
}
