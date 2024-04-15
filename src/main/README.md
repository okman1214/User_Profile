1、EMR DLC 测试Iceberg
//上传文件到hdfs
hdfs dfs -mkdir /data
hdfs dfs -put data.csv /data

//初始化需要的数据到hive iceberg表
${SPARK_HOME}/bin/spark-submit \
--jars /usr/local/service/iceberg/iceberg-spark-runtime-3.2_2.12-0.13.1.jar \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.sql.catalog.DataLakeCatalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.DataLakeCatalog.type=hive \
--conf spark.sql.catalogImplementation=hive \
--conf spark.sql.shuffle.partitions=20 \
--conf spark.yarn.submit.waitAppCompletion=true \
--name user-profile_base \
--master yarn \
--deploy-mode client \
--driver-memory 10G \
--executor-memory 2G \
--num-executors 4 \
--class com.msdnfaq.bigdata.Main \
/home/hadoop/User_Profile-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e dlc_prod -x hadoop -l 100000 -u baidu.com -n hadoop -p 123 -c yarn -d lynchgao -a true

 //用sparksql测试语法
 spark-sql \
   --jars /usr/local/service/iceberg/iceberg-spark-runtime-3.2_2.12-0.13.1.jar \
   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
   --conf spark.sql.catalog.DataLakeCatalog=org.apache.iceberg.spark.SparkCatalog \
   --conf spark.sql.catalog.DataLakeCatalog.type=hive \
  
  
CREATE TABLE hive.dwd.t1 (id int, name string) USING iceberg;
INSERT INTO hive.dwd.dwd_user_base_feature values("1", "男","20","1309181881","ok@124.com","4g","iphone","iphone13");

2、EMR 测试Spark
nohup ${SPARK_HOME}/bin/spark-submit \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.rpc.message.maxSize=1024 \
--conf spark.network.timeout=600s \
--conf spark.sql.shuffle.partitions=20 \
--conf spark.yarn.submit.waitAppCompletion=true \
--name user-profile_base \
--master yarn \
--deploy-mode client \
--driver-memory 10G \
--executor-memory 2G \
--num-executors 4 \
--class com.msdnfaq.bigdata.sparkvshive.SparkTest \
/home/hadoop/User_Profile-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e dlc_hive_prod -x hadoop -l 10000000 -u baidu.com -n hadoop -p 123 -c yarn -d lynchgao -a true >> error.log 2>&1 &

bin/beeline -u "jdbc:hive2://10.18.0.221:2181,10.18.0.233:2181,10.18.0.211:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=kyuubi;#spark.driver.memory=10g;spark.executor.memory=2g;spark.executor.cores=3;spark.executor.instances=1;kyuubi.engine.share.level=CONNECTION" -n hadoop -f /home/hadoop/spark.sql