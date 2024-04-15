create database if not exists dw location 'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/';
USE dw;
CREATE TABLE `dim_cstm_active_user_c_appliction_mb_df`(
  `appliction_name` string, 
  `dt` string, 
  `event_time` string, 
  `first_app_version` string, 
  `first_date` string, 
  `product_name` string, 
  `user_number` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dim_cstm_active_user_c_appliction_mb_df')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dim_cstm_active_user_c_appliction_mb_df'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.2', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"appliction_name","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"event_time","type":"string","nullable":true,"metadata":{}},{"name":"first_app_version","type":"string","nullable":true,"metadata":{}},{"name":"first_date","type":"string","nullable":true,"metadata":{}},{"name":"product_name","type":"string","nullable":true,"metadata":{}},{"name":"user_number","type":"string","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1713055209');

CREATE TABLE `dim_mkt_source_df`(
  `appliction_name` string, 
  `channel_name` string, 
  `dt` string, 
  `first_channel` string, 
  `page_name` string, 
  `page_url` string, 
  `second_channel` string, 
  `source` string, 
  `third_channel` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dim_mkt_source_df')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dim_mkt_source_df'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.2', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"appliction_name","type":"string","nullable":true,"metadata":{}},{"name":"channel_name","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"first_channel","type":"string","nullable":true,"metadata":{}},{"name":"page_name","type":"string","nullable":true,"metadata":{}},{"name":"page_url","type":"string","nullable":true,"metadata":{}},{"name":"second_channel","type":"string","nullable":true,"metadata":{}},{"name":"source","type":"string","nullable":true,"metadata":{}},{"name":"third_channel","type":"string","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1713055210');

CREATE TABLE `dwd_odr_detail_mb_df`(
  `app_id` int, 
  `course_type` int, 
  `create_time` string, 
  `dt` string, 
  `order_number` bigint, 
  `order_status` int, 
  `paid_time` string, 
  `record_type` int, 
  `source` string, 
  `user_number` bigint, 
  `user_number_boss` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dwd_odr_detail_mb_df')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dwd_odr_detail_mb_df'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.2', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"app_id","type":"integer","nullable":true,"metadata":{}},{"name":"course_type","type":"integer","nullable":true,"metadata":{}},{"name":"create_time","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"order_number","type":"long","nullable":true,"metadata":{}},{"name":"order_status","type":"integer","nullable":true,"metadata":{}},{"name":"paid_time","type":"string","nullable":true,"metadata":{}},{"name":"record_type","type":"integer","nullable":true,"metadata":{}},{"name":"source","type":"string","nullable":true,"metadata":{}},{"name":"user_number","type":"long","nullable":true,"metadata":{}},{"name":"user_number_boss","type":"string","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1713055205');

CREATE TABLE `dws_cstm_new_user_order_conversion_data_df`(
  `first_date` string, 
  `product_name` string, 
  `act_user_type` string, 
  `course_type` string, 
  `user_num` string, 
  `within_0_days_user_num` bigint, 
  `within_1_days_user_num` bigint, 
  `within_7_days_user_num` bigint, 
  `within_30_days_user_num` bigint, 
  `within_0_days_user_rate` decimal(10,6), 
  `within_1_days_user_rate` decimal(10,6), 
  `within_7_days_user_rate` decimal(10,6), 
  `within_30_days_user_rate` decimal(10,6))
PARTITIONED BY ( 
  `dt` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/dws_cstm_new_user_order_conversion_data_df'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.2', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"first_date","type":"string","nullable":true,"metadata":{}},{"name":"product_name","type":"string","nullable":true,"metadata":{}},{"name":"act_user_type","type":"string","nullable":true,"metadata":{}},{"name":"course_type","type":"string","nullable":true,"metadata":{}},{"name":"user_num","type":"string","nullable":true,"metadata":{}},{"name":"within_0_days_user_num","type":"long","nullable":true,"metadata":{}},{"name":"within_1_days_user_num","type":"long","nullable":true,"metadata":{}},{"name":"within_7_days_user_num","type":"long","nullable":true,"metadata":{}},{"name":"within_30_days_user_num","type":"long","nullable":true,"metadata":{}},{"name":"within_0_days_user_rate","type":"decimal(10,6)","nullable":true,"metadata":{}},{"name":"within_1_days_user_rate","type":"decimal(10,6)","nullable":true,"metadata":{}},{"name":"within_7_days_user_rate","type":"decimal(10,6)","nullable":true,"metadata":{}},{"name":"within_30_days_user_rate","type":"decimal(10,6)","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}}]}', 
  'spark.sql.sources.schema.numPartCols'='1', 
  'spark.sql.sources.schema.partCol.0'='dt', 
  'transient_lastDdlTime'='1713055199');

CREATE TABLE `ods_gaotu_super_class_user`(
  `birthday` string, 
  `device_type` string, 
  `dt` string, 
  `grade` bigint, 
  `last_login` bigint, 
  `mobile` string, 
  `name` string, 
  `province` string, 
  `register_product` string, 
  `register_time` bigint, 
  `sex` int, 
  `subject` string, 
  `user_id` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES ( 
  'path'='cosn://lynchgao-cos-1253240642/hive_warehouse/dw/ods_gaotu_super_class_user')
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'cosn://lynchgao-cos-1253240642/hive_warehouse/dw/ods_gaotu_super_class_user'
TBLPROPERTIES (
  'spark.sql.create.version'='3.2.2', 
  'spark.sql.sources.provider'='parquet', 
  'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"birthday","type":"string","nullable":true,"metadata":{}},{"name":"device_type","type":"string","nullable":true,"metadata":{}},{"name":"dt","type":"string","nullable":true,"metadata":{}},{"name":"grade","type":"long","nullable":true,"metadata":{}},{"name":"last_login","type":"long","nullable":true,"metadata":{}},{"name":"mobile","type":"string","nullable":true,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"province","type":"string","nullable":true,"metadata":{}},{"name":"register_product","type":"string","nullable":true,"metadata":{}},{"name":"register_time","type":"long","nullable":true,"metadata":{}},{"name":"sex","type":"integer","nullable":true,"metadata":{}},{"name":"subject","type":"string","nullable":true,"metadata":{}},{"name":"user_id","type":"long","nullable":true,"metadata":{}}]}', 
  'transient_lastDdlTime'='1713055208');
