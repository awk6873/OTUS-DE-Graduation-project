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
		app_name = 'test_raw_gobike_tripdata')
sc = spark.sparkContext

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from uuid import uuid4

# имена таблиц
etl_session_table =  'device_sb.test_sys_etl_session'
raw_datatrip_table = 'device_sb.test_raw_gobike_tripdata'


# загрузка csv в raw

# пути к папкам на HDFS
dataset_dir =          '/user/avkabay1/OTUS/Project/dataset/'
dataset_dir_procesed = '/user/avkabay1/OTUS/Project/dataset/processed/'
file_name_template = 'tripdata.csv'

# объекты для работы файлами на HDFS
URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs://rnd-dwh-nn-002:8020"), sc._jsc.hadoopConfiguration())

# обработка csv файлов 
for f in fs.listStatus(Path(dataset_dir)):
    
    f_name = f.getPath().getName()

    # проверяем имя файла по шаблону
    if file_name_template in f_name:
        # print f_name
    
        # генерируем ID сеанса загрузки
        etl_session_id = str(uuid4())
        
        # загружаем файл
        raw_df = spark.read\
        .format('csv')\
        .option("header", "true")\
        .load(dataset_dir + f_name)\
        .select('duration_sec', 'start_time', 'end_time', 
                'start_station_id', 'start_station_name', 'start_station_latitude', 'start_station_longitude',
                'end_station_id', 'end_station_name', 'end_station_latitude', 'end_station_longitude',
                'bike_id', 'user_type')\
        .withColumn('business_dt', substring(col('start_time'), 1, 10))\
        .withColumn('etl_session_id', lit(etl_session_id))\
        .withColumn('load_ts', current_timestamp())
        
        num_records = raw_df.count()
        
        raw_df\
        .repartition(1)\
        .write.format("orc")\
        .partitionBy('business_dt')\
        .mode('append')\
        .saveAsTable(raw_datatrip_table)
        
        # создаем запись в логе загрузок
        spark.sql("""
        insert into {0}
        select 
          '{1}' as etl_session_id,
          current_timestamp() as start_time,
          '{2}' as table_name,
          '{3}' as file_name,
          {4} as num_records
        """.format(etl_session_table, etl_session_id, raw_datatrip_table, f_name, num_records))
        
        # обработанный файл перемещаем в папку processed
        fs.rename(f.getPath(), Path(dataset_dir_procesed))
