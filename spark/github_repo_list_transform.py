# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf 
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


# padnas dataframe to spark dataframe
pd_df = PD_df()
repo_list_df = spark.createDataFrame(pd_df.repo_list(), schema = github_schema.repo_list)

repo_list_df.printSchema()


# 중복 제거
repo_list_df.dropDuplicates()

repo_list_df.show()


# dataframe to parquet
timestamp = datetime.now().strftime("%Y/%m/%d")
path = f's3://de-2-2/analytics/github/repository_list/{timestamp}.parquet'
repo_list_df.coalesce(1).write.parquet(path)

spark.stop()