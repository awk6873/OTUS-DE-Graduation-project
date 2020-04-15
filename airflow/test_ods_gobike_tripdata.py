#-*- coding: utf-8 -*-
import sys

# инициализация spark
def spark_build(config, app_name):
	import os
	import sys
	import pytz
	import multiprocessing
	import random
	import pandas as pd
	
	pd.set_option('display.max_colwidth', -1)
	pd.set_option('display.max_columns', 10000)
	pd.set_option('display.max_rows', 20000)
	
	os.environ["PATH"] = "/opt/anaconda/bin:" + os.environ["PATH"]
	os.environ["CAPTURE_STANDARD_OUT"] = "true"
	os.environ["CAPTURE_STANDARD_ERR"] = "true"
	os.environ["SEND_EMPTY_OUTPUT"] = "false"
	os.environ["SPARK_HOME"] = "/usr/hdp/current/spark2-client"
	os.environ["PYSPARK_PYTHON"] ="/opt/anaconda/envs/py27/bin/python" if sys.version[:3] == "3.6" else "/opt/anaconda/bin/python"
	os.environ["PYSPARK_DRIVER_PYTHON"] ="/opt/anaconda/envs/py27/bin/python" if sys.version[:3] == "3.6" else "/opt/anaconda/bin/python"
	
	py4j_path = list(filter(lambda x: "py4j" in x, os.listdir(os.environ["SPARK_HOME"] + "/python/lib/")))
	if not py4j_path:
		raise "py4j not found"
	py4j_path = "python/lib/" + py4j_path[0]
	
	spark_python_paths = [os.environ["SPARK_HOME"] + "/" + p for p in [
		py4j_path,
		"python",
		"python/build",
		"libexec/python",
		"libexec/python/build"
	]]
	
	graphframes_python_paths = [
		"/home/shared/graphframes-release-0.5.0/python"
	]

	teradata_lib_paths = [
		"/data/share/terajdbc4.jar",
		"/data/share/tdgssconfig.jar"
	]

	graphframes_lib_paths = [
		"/data/share/graphframes-0.5.0-spark2.1-s_2.11.jar",
		"/data/share/scala-logging-slf4j_2.11-2.1.2.jar",
		"/data/share/scala-logging-api_2.11-2.1.2.jar"
	]

	python_paths = spark_python_paths + graphframes_python_paths
	lib_paths = teradata_lib_paths + graphframes_lib_paths
	
	sys.path = python_paths + lib_paths + sys.path

	from pyspark import SparkConf
	from pyspark.sql import SparkSession
	
	default_config = {
		"spark.task.maxFailures": "15",
		"spark.driver.cores": "4",
		"spark.driver.memory": "12g",
		"spark.driver.maxResultSize": "16g",
		"spark.executor.cores": "4",
		"spark.executor.memory": "16g",
		"spark.sql.shuffle.partitions": "200",
		"spark.default.parallelism": "200",
		"spark.debug.maxToStringFields": "10000",
		"spark.yarn.am.memory": "10g",
		"spark.shuffle.service.enabled": "true",
		"spark.dynamicAllocation.enabled": "true",
		"spark.dynamicAllocation.minExecutors": "5",
		"spark.dynamicAllocation.initialExecutors": "5",
		"spark.dynamicAllocation.maxExecutors": "40",
		"spark.dynamicAllocation.cachedExecutorIdleTimeout": "1200s",
		"spark.locality.wait": "0s",
		"spark.sql.parquet.writeLegacyFormat": "true",
		"spark.hadoop.ipc.client.fallback-to-simple-auth-allowed": "true",
		"spark.pyspark.virtualenv.enabled": "false",
		"spark.pyspark.virtualenv.type": "native",
		"spark.pyspark.virtualenv.bin.path": "/opt/anaconda/bin/virtualenv",
		"spark.sql.execution.pandas.respectSessionTimeZone": "false",
		"spark.sql.execution.arrow.enabled": "false",
		"spark.sql.hive.convertMetastoreOrc": "true",
		"mapred.input.dir.recursive": "true",
		"spark.driver.extraJavaOptions": "-XX:ParallelGCThreads=8",
		"spark.executor.extraJavaOptions": "-XX:ParallelGCThreads=8",
		"spark.driver.extraClassPath": ":".join(lib_paths),
		"spark.executor.extraClassPath": ":".join(lib_paths),
		"spark.jars": ",".join(lib_paths),
		"spark.ui.port": str(4040 + random.randint(16,200)),
		"spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive": "true"
		}
	
	default_config.update(config)
	config = default_config

	spark = SparkSession.builder\
	.appName(app_name)

	for k, v in config.items():
		spark = spark.config(k, v)
		
	spark = spark\
	.enableHiveSupport()\
	.getOrCreate()
	
	return spark

# spark контекст
spark = spark_build(
		{'spark.master': 'yarn'},
		app_name = 'test_ods_gobike_tripdata')
sc = spark.sparkContext

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from uuid import uuid4

# имена таблиц
etl_session_table =  'device_sb.test_sys_etl_session'
raw_datatrip_table = 'device_sb.test_raw_gobike_tripdata'
ods_datatrip_table = 'device_sb.test_ods_gobike_tripdata'

# загрузка raw -> ods

# генерируем ID сеанса загрузки
etl_session_id = str(uuid4())

ods_df = spark.sql("""
-- выборка новых данных из raw за 30 дней
with raw_data as (
    select distinct
        cast(start_station_id as integer),
        start_station_name,
        cast(start_station_latitude as decimal(12, 7)),
        cast(start_station_longitude as decimal(12, 7)),
        cast(end_station_id as integer),
        end_station_name,
        cast(end_station_latitude as decimal(12, 7)),
        cast(end_station_longitude as decimal(12, 7)),
        cast(bike_id as integer),
        cast(start_time as timestamp),
        cast(end_time as timestamp),
        user_type,
        cast(duration_sec as integer),
        hash(start_station_id, end_station_id, bike_id, start_time, end_time) as hash,
        '{2}' as etl_session_id,
        current_timestamp() as load_ts,
        cast(business_dt as date)
    from {0}
    where start_station_id > 0 and end_station_id > 0 and bike_id > 0 and duration_sec > 0
      --and business_dt >= date_add(current_date(), -30)
)

select raw_data.*
from raw_data

-- отбрасываем дубли строк, уже загруженные в ODS
left join {1} ods_data
  on ods_data.business_dt = raw_data.business_dt and ods_data.hash = raw_data.hash
where ods_data.hash is null  

""".format(raw_datatrip_table, ods_datatrip_table, etl_session_id))

num_records = ods_df.count()

ods_df.repartition(3)\
    .write.format("orc")\
    .partitionBy('business_dt')\
    .mode('append')\
    .saveAsTable(ods_datatrip_table)
    
# создаем запись в логе загрузок
spark.sql("""
insert into {0}
select 
  '{1}' as etl_session_id,
  current_timestamp() as start_time,
  '{2}' as table_name,
  '{3}' as file_name,
   {4} as num_records
""".format(etl_session_table, etl_session_id, ods_datatrip_table, '', num_records))