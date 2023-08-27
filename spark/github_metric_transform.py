# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkConf 
import github_schema
import github_pddf
from awsfunc import awsfunc


# spark session 설정 및 생성
conf = SparkConf()
conf.set("spark.app.name", "Metric Data Process")
conf.set("spark.master", "local[*]")
conf.set("spark.sql.execution.arrow.enabled", "true")
conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

spark = SparkSession.builder.config(conf=conf).getOrCreate()


# padnas dataframe to spark dataframe
commit_activity_df= spark.createDataFrame(github_pddf.commit_activity, schema = github_schema.commit_activity)

commit_activity_df.printSchema()

# 중복 제거
commit_activity_df.dropDuplicates()

# dataframe to json
commit_activity_df.show()
path = 's3a://de-2-2/analytics/github/commit_activity/2023/08/24.json'

commit_activity_json = commit_activity_df.toJSON().collect()
s3_client = awsfunc('s3')
print('\n'.join(commit_activity_json))
s3_client.ec2tos3('\n'.join(commit_activity_json),'de-2-2', 'analytic/github/commit_activity/2023/08/24.json')

spark.stop()