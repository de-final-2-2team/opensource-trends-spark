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
issue_list, pr_list, commit_list = pd_df.detail_list()


if not issue_list.empty:
    issue_list_df= spark.createDataFrame(issue_list, schema = github_schema.issue_and_pr)

    issue_list_df.printSchema()

    # 중복 제거
    issue_list_df.dropDuplicates()

    # 수집날짜 추가
    issue_list_df = issue_list_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    issue_list_df.show()

    # dataframe to parquet
    issue_list_path = f's3://de-2-2/analytics/github/issue_list/{timestamp}'
    issue_list_df.coalesce(1).write.parquet(issue_list_path)

if not pr_list.empty:
    pr_list_df= spark.createDataFrame(pr_list, schema = github_schema.issue_and_pr)

    pr_list_df.printSchema()

    # 중복 제거
    pr_list_df.dropDuplicates()

    # 수집날짜 추가
    pr_list_df = pr_list_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    pr_list_df.show()

    # dataframe to parquet
    pr_list_path = f's3://de-2-2/analytics/github/pr_list/{timestamp}'
    pr_list_df.coalesce(1).write.parquet(pr_list_path)
    
if not commit_list.empty:
    commit_list_df= spark.createDataFrame(commit_list, schema = github_schema.commit_list)

    commit_list_df.printSchema()

    # 중복 제거
    commit_list_df.dropDuplicates()

    # 수집날짜 추가
    commit_list_df = commit_list_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

    commit_list_df.show()

    # dataframe to parquet
    commit_list_path = f's3://de-2-2/analytics/github/commit_list/{timestamp}'
    commit_list_df.coalesce(1).write.parquet(commit_list_path)
    


spark.stop()