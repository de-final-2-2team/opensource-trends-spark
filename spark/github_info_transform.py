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

timestamp = datetime.now()

# padnas dataframe to spark dataframe
pd_df = PD_df()
release_tag_list, project_list, language_list, fork_list = pd_df.info_list()

pd_df_list = [release_tag_list, project_list, language_list, fork_list]
pd_df_str_list = ['release_tag_list', 'project_list', 'language_list', 'fork_list']
schema_list = [github_schema.release_tag_list, github_schema.project_list, github_schema.language_list, github_schema.fork_list]

for i in range(len(pd_df_list)):
    if not pd_df_list[i].empty:
        df = spark.createDataFrame(pd_df_list[i], schema = schema_list[i])
        df.printSchema()

        # 중복 제거
        df.dropDuplicates()

        # 수집 날짜 추가
        df = df.withColumn("COLLECTED_AT", lit(timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")))
        df.show()

        # spark dataframe to parquet
        path = f's3://de-2-2/analytics/github/{pd_df_str_list[i]}/{timestamp.strftime("%Y/%m/%d")}'
        df.coalesce(1).write.mode("append").parquet(path)
        
spark.stop()




