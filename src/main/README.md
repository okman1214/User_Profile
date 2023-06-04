//root账户创建相关路径
mkdir /home/hadoop/project
上传任务脚本
下载hudi jar
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/0.8.0/hudi-spark3-bundle_2.12-0.8.0.jar

${SPARK_HOME}/bin/spark-submit \
--jars /home/hadoop/project/hudi-spark3-bundle_2.12-0.8.0.jar \
--conf spark.executor.heartbeatInterval=120s \
--conf spark.network.timeout=600s \
--conf spark.sql.catalogImplementation=hive \
--conf spark.sql.shuffle.partitions=20 \
--conf spark.yarn.submit.waitAppCompletion=true \
--conf spark.sql.hive.convertMetastoreParquet=false \
--name user-profile_base \
--master yarn \
--deploy-mode client \
--driver-memory 512M \
--executor-memory 1G \
--num-executors 1 \
--class com.msdnfaq.bigdata.userprofile.InitGenerator \
/home/hadoop/project/User_Profile-1.0-SNAPSHOT-jar-with-dependencies.jar \
-e prod -x hadoop
