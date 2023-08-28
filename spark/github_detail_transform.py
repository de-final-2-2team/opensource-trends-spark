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
commit_list_df= spark.createDataFrame(pd_df.commit_list(), schema = github_schema.commit_list)
issue_and_pr_df= spark.createDataFrame(pd_df.issue_and_pr(), schema = github_schema.issue_and_pr)

commit_list_df.printSchema()
issue_and_pr_df.printSchema()

# 중복 제거
issue_and_pr_df.dropDuplicates()

commit_list_df.show()
issue_and_pr_df.show()

# dataframe to json
timestamp = datetime.now().strftime("%Y/%m/%d")
commit_list_path = f's3://de-2-2/analytics/github/commit_list/{timestamp}.parquet'
issue_and_pr_path = f's3://de-2-2/analytics/github/issue_and_pr/{timestamp}.parquet'


commit_list_df.coalesce(1).write.parquet(commit_list_path)
issue_and_pr_df.coalesce(1).write.parquet(issue_and_pr_path)

spark.stop()