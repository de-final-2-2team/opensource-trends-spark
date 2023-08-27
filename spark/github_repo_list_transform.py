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
repo_list_df = spark.createDataFrame(github_pddf.repo_list, schema = github_schema.repo_list)

repo_list_df.printSchema()

# 중복 제거
repo_list_df.dropDuplicates()

# dataframe to json
repo_list_df.show()
path = 's3a://de-2-2/analytics/github/repository_list/2023/08/27.json'

repo_list_json = repo_list_df.toJSON().collect()
s3_client = awsfunc('s3')
print('\n'.join(repo_list_json))
s3_client.ec2tos3('\n'.join(repo_list_json),'de-2-2', 'analytic/github/repository_list/2023/08/27.json')


spark.stop()