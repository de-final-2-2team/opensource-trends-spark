# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.sql.functions import lit, col
import stackoverflow_schema
from stackoverflow_pddf import PD_df
from awsfunc import awsfunc
from datetime import datetime

# spark session 설정 및 생성
conf = SparkConf()
conf.set("spark.app.name", "Metric Data Process")
conf.set("spark.master", "local[*]")
conf.set("spark.sql.execution.arrow.enabled", "true")
conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

timestamp = datetime.now()

pd_df = PD_df()

# padnas dataframe to spark dataframe
question_list_df= spark.createDataFrame(pd_df.question_list(), schema = stackoverflow_schema.question_list)

question_list_df.printSchema()

# USER_ID가 None이 Row 제거
question_list_df = question_list_df.filter(col("USER_ID").isNotNull())

# 중복 제거
question_list_df.dropDuplicates()

# 수집날짜 추가
question_list_df = question_list_df.withColumn("COLLECTED_AT", lit(timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")))

question_list_df.show()


# dataframe to parquet
path = f's3://de-2-2/analytics/stackoverflow/question_list/{timestamp.strftime("%Y/%m/%d")}'
question_list_df.coalesce(1).write.mode("append").parquet(path)
spark.stop()