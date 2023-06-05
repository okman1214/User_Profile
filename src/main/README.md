//上传文件到hdfs
hdfs dfs -mkdir /data
hdfs dfs -put data.csv /data

//root账户创建相关路径
mkdir /home/hadoop/project
上传任务脚本
下载hudi jar
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/0.8.0/hudi-spark3-bundle_2.12-0.8.0.jar
上传iceberg新版包
iceberg-spark-runtime-3.2_2.12-1.2.1.jar，如果还是用旧版，需要替换下面包名

//初始化需要的数据到hive表
${SPARK_HOME}/bin/spark-submit \
--jars /usr/local/service/iceberg/iceberg-spark-runtime-3.2_2.12-1.2.1.jar \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.sql.catalogImplementation=hive \
--conf spark.sql.shuffle.partitions=20 \
--conf spark.yarn.submit.waitAppCompletion=true \
--name user-profile_base \
--master yarn \
--deploy-mode client \
--driver-memory 2G \
--executor-memory 2G \
--num-executors 4 \
--class com.msdnfaq.bigdata.userprofile.InitGenerator \
/home/hadoop/User_Profile-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e prod -x hadoop


//用sparksql测试语法
spark-sql \
  --jars /usr/local/service/iceberg/iceberg-spark-runtime-3.2_2.12-1.2.1.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hive.type=hive \
  
  
CREATE TABLE hive.dwd.t1 (id int, name string) USING iceberg;
INSERT INTO hive.dwd.dwd_user_base_feature values("1", "男","20","1309181881","ok@124.com","4g","iphone","iphone13");