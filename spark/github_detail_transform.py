# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkConf 
import github_schema
import github_pddf
from awsfunc import awsfunc

# spark session 설정 및 생성
conf = SparkConf()
conf.set("spark.app.name", "Repository List Data Process")
conf.set("spark.master", "local[*]")
conf.set("spark.sql.execution.arrow.enabled", "true")
conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

spark = SparkSession.builder.config(conf=conf).getOrCreate()


# padnas dataframe to spark dataframe
commit_list_df= spark.createDataFrame(github_pddf.commit_list, schema = github_schema.commit_list)
issue_and_pr_df= spark.createDataFrame(github_pddf.issue_and_pr, schema = github_schema.issue_and_pr)

commit_list_df.printSchema()
issue_and_pr_df.printSchema()

# 중복 제거
issue_and_pr_df.dropDuplicates()


# dataframe to json
commit_list_df.show()
issue_and_pr_df.show()

commit_list_path = 's3a://de-2-2/analytics/github/commit_list/2023/08/24.json'
issue_and_pr_path = 's3a://de-2-2/analytics/github/issue_and_pr/2023/08/24.json'


commit_list_json = commit_list_df.toJSON().collect()
issue_and_pr_json = issue_and_pr_df.toJSON().collect()

s3_client = awsfunc('s3')

s3_client.ec2tos3('\n'.join(commit_list_json),'de-2-2', 'analytic/github/commit_list/2023/08/24.json')
s3_client.ec2tos3('\n'.join(issue_and_pr_json),'de-2-2', 'analytic/github/issue_and_pr/2023/08/24.json')

spark.stop()