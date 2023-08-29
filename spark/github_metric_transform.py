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
conf.set("spark.app.name", "Metric Data Process")
conf.set("spark.master", "local[*]")
conf.set("spark.sql.execution.arrow.enabled", "true")
conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

pd_df = PD_df()

# padnas dataframe to spark dataframe
commit_activity_df= spark.createDataFrame(pd_df.commit_activity(), schema = github_schema.commit_activity)

commit_activity_df.printSchema()

# 중복 제거
commit_activity_df.dropDuplicates()

# 수집날짜 추가
commit_activity_df = commit_activity_df.withColumn("COLLECTED_AT", lit(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

commit_activity_df.show()


# dataframe to parquet
timestamp = datetime.now().strftime("%Y/%m/%d")
path = f's3://de-2-2/analytics/github/commit_activity/{timestamp}'
commit_activity_df.coalesce(1).write.parquet(path)

spark.stop()