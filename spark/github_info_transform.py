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

timestamp = datetime.now().strftime("%Y/%m/%d")

# padnas dataframe to spark dataframe
pd_df = PD_df()
release_list, project_list, language_list, fork_list = pd_df.info_list()

if not release_list.empty: 
    release_tag_df= spark.createDataFrame(release_list, schema = github_schema.release_tag)

    release_tag_df.printSchema()
    
    # 중복 제거
    release_tag_df.dropDuplicates()

    # 수집날짜 추가
    release_tag_df = release_tag_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    release_tag_df.show()

    # dataframe to parquet
    release_tag_path = f's3://de-2-2/analytics/github/release_tag_list/{timestamp}'
    release_tag_df.coalesce(1).write.parquet(release_tag_path)



if not project_list.empty:
    project_list_df= spark.createDataFrame(project_list, schema = github_schema.project_list)

    project_list_df.printSchema()

    # 중복 제거
    project_list_df.dropDuplicates()

    # 수집날짜 추가
    project_list_df = project_list_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    project_list_df.show()

    # dataframe to parquet
    project_list_path = f's3://de-2-2/analytics/github/project_list/{timestamp}'
    project_list_df.coalesce(1).write.parquet(project_list_path)


if not language_list.empty:
    language_list_df= spark.createDataFrame(language_list, schema = github_schema.language_list)

    language_list_df.printSchema()

    # 중복 제거
    language_list_df.dropDuplicates()

    # 수집날짜 추가
    language_list_df = language_list_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    language_list_df.show()

    # dataframe to parquet
    language_list_path = f's3://de-2-2/analytics/github/language_list/{timestamp}'
    language_list_df.coalesce(1).write.parquet(language_list_path)


if not fork_list.empty:
    fork_list_df= spark.createDataFrame(fork_list, schema = github_schema.fork_list)

    fork_list_df.printSchema()

    # 중복 제거
    fork_list_df.dropDuplicates()

    # 수집날짜 추가
    fork_list_df = fork_list_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    fork_list_df.show()

    # dataframe to parquet
    fork_list_path = f's3://de-2-2/analytics/github/fork_list/{timestamp}'
    fork_list_df.coalesce(1).write.parquet(fork_list_path)




spark.stop()