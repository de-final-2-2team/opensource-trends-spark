# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# question list spark dataframe schema 설정
question_list = StructType([
    StructField("ID", StringType(), False),
    StructField("TITLE", StringType(), False),
    StructField("USER_ID", StringType(), True),
    StructField("USER_NM", StringType(), False),
    StructField("URL", StringType(), True),
    StructField("CREATED_AT", StringType(), False),
    StructField("UPDATED_AT", StringType(), True),
    StructField("REPO_ID", StringType(), False)
])
