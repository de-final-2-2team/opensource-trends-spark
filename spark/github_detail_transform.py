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
issue_list, pr_list, commit_list = pd_df.detail_list()
commit_df= spark.createDataFrame(commit_list, schema = github_schema.commit_list)
issue_df= spark.createDataFrame(issue_list, schema = github_schema.issue_and_pr)
pr_df= spark.createDataFrame(pr_list, schema = github_schema.issue_and_pr)


commit_df.printSchema()
issue_df.printSchema()
pr_df.printSchema()

# 중복 제거
commit_df.dropDuplicates()
issue_df.dropDuplicates()
pr_df.dropDuplicates()

# 수집날짜 추가
commit_df.withColumn("COLLECTED_AT", datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
issue_df.withColumn("COLLECTED_AT", datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
pr_df.withColumn("COLLECTED_AT", datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))

commit_df.show()
issue_df.show()
pr_df.show()

# dataframe to json
timestamp = datetime.now().strftime("%Y/%m/%d")
commit_path = f's3://de-2-2/analytics/github/commit/{timestamp}'
issue_path = f's3://de-2-2/analytics/github/issue/{timestamp}'
pr_path = f's3://de-2-2/analytics/github/pr/{timestamp}'



commit_df.coalesce(1).write.parquet(commit_path)
issue_df.coalesce(1).write.parquet(issue_path)
pr_df.coalesce(1).write.parquet(pr_path)

spark.stop()