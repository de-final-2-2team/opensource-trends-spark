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
language_list_df= spark.createDataFrame(github_pddf.language_list, schema = github_schema.language_list)
release_tag_df= spark.createDataFrame(github_pddf.release_tag, schema = github_schema.release_tag)
project_list_df= spark.createDataFrame(github_pddf.project_list, schema = github_schema.project_list)
fork_list_df= spark.createDataFrame(github_pddf.fork_list, schema = github_schema.fork_list)

language_list_df.printSchema()
release_tag_df.printSchema()
project_list_df.printSchema()
fork_list_df.printSchema()

# 중복 제거
language_list_df.dropDuplicates()
release_tag_df.dropDuplicates()
project_list_df.dropDuplicates()
fork_list_df.dropDuplicates()

# dataframe to json

language_list_df.show()
release_tag_df.show()
project_list_df.show()
fork_list_df.show()

language_list_path = 's3a://de-2-2/analytics/github/language_list/2023/08/24.json'
release_tag_path = 's3a://de-2-2/analytics/github/release_tag/2023/08/24.json'
project_list_path = 's3a://de-2-2/analytics/github/project_list/2023/08/24.json'
fork_list_path = 's3a://de-2-2/analytics/github/fork_list/2023/08/24.json'

language_list_json = language_list_df.toJSON().collect()
release_tag_json = language_list_df.toJSON().collect()
project_list_json = language_list_df.toJSON().collect()
fork_list_json = language_list_df.toJSON().collect()

s3_client = awsfunc('s3')

s3_client.ec2tos3('\n'.join(language_list_json),'de-2-2', 'analytic/github/language_list/2023/08/24.json')
s3_client.ec2tos3('\n'.join(release_tag_json),'de-2-2', 'analytic/github/release_tag/2023/08/24.json')
s3_client.ec2tos3('\n'.join(project_list_json),'de-2-2', 'analytic/github/project_list/2023/08/24.json')
s3_client.ec2tos3('\n'.join(fork_list_json),'de-2-2', 'analytic/github/fork_list/2023/08/24.json')

spark.stop()