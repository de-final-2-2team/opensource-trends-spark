# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.sql.functions import lit
import github_schema
from github_pddf import PD_df
from awsfunc import awsfunc
from datetime import datetime

# spark session 설정 및 생성
conf = SparkConf()
conf.set("spark.app.name", "Repository List Data Process")
conf.set("spark.master", "local[*]")
conf.set("spark.sql.execution.arrow.enabled", "true")
conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

timestamp = datetime.now()

# padnas dataframe to spark dataframe
pd_df = PD_df()
license_list_df = spark.createDataFrame(pd_df.license_list(), schema = github_schema.license_list)

license_list_df.printSchema()


# 중복 제거
license_list_df.dropDuplicates()

# 수집날짜 추가
license_list_df = license_list_df.withColumn("COLLECTED_AT", lit(timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")))

license_list_df.show()


# dataframe to parquet

path = f's3://de-2-2/analytics/github/license_list/{timestamp.strftime("%Y/%m/%d")}'
license_list_df.coalesce(1).write.mode("append").parquet(path)

spark.stop()