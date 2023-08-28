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
language_list_df= spark.createDataFrame(pd_df.language_list(), schema = github_schema.language_list)
release_tag_df= spark.createDataFrame(pd_df.release_tag(), schema = github_schema.release_tag)
project_list_df= spark.createDataFrame(pd_df.project_list(), schema = github_schema.project_list)
fork_list_df= spark.createDataFrame(pd_df.fork_list(), schema = github_schema.fork_list)

language_list_df.printSchema()
release_tag_df.printSchema()
project_list_df.printSchema()
fork_list_df.printSchema()


# 중복 제거
language_list_df.dropDuplicates()
release_tag_df.dropDuplicates()
project_list_df.dropDuplicates()
fork_list_df.dropDuplicates()

language_list_df.show()
release_tag_df.show()
project_list_df.show()
fork_list_df.show()


# dataframe to parquet
timestamp = datetime.now().strftime("%Y/%m/%d")
language_list_path = f's3://de-2-2/analytics/github/language_list/{timestamp}.parquet'
release_tag_path = f's3://de-2-2/analytics/github/release_tag/{timestamp}.parquet'
project_list_path = f's3://de-2-2/analytics/github/project_list/{timestamp}.parquet'
fork_list_path = f's3://de-2-2/analytics/github/fork_list/{timestamp}.parquet'

language_list_df.coalesce(1).write.parquet(language_list_path)
release_tag_df.coalesce(1).write.parquet(release_tag_path)
project_list_df.coalesce(1).write.parquet(project_list_path)
fork_list_df.coalesce(1).write.parquet(fork_list_path)

spark.stop()