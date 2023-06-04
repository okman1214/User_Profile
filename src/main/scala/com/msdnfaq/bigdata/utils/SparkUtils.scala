package com.msdnfaq.bigdata.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkUtils {
  private val logger = LoggerFactory.getLogger(SparkUtils.getClass)

  def getSparkSession(env: String, appName: String): SparkSession = {
    env match {
      case "prod" => {
        SparkSession.builder().appName(appName)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.hive.metastore.version", "2.3.9")
          .config("spark.sql.cbo.enabled", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .enableHiveSupport()
          .getOrCreate()
      }
      case "dev" => {
        SparkSession.builder()
          .appName(appName + "_" + "dev")
          .master("local[*]")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.sql.hive.metastore.version", "2.3.9")
          .config("spark.sql.cbo.enabled", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .enableHiveSupport()
          .getOrCreate()
      }
      case _ => {
        logger.error("not match env, errors")
        System.exit(-1)
        null
      }
    }
  }

}
