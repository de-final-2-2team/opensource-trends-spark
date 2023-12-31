# -*- coding: utf-8 -*-
import findspark
findspark.init()

from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# repo list spark dataframe schema 설정
license_list = StructType([
    StructField("LIC_KEY", StringType(), False),
    StructField("LIC_NM", StringType(), False),
    StructField("SPDX_ID", StringType(), True), 
])


repo_list = StructType([
    StructField("NODE_ID", StringType(), False),
    StructField("REPO_NM", StringType(), False),
    StructField("FULL_NM", StringType(), False),
    StructField("OWNER_ID", StringType(), False),
    StructField("OWNER_NM", StringType(), True),
    StructField("CREATED_AT", StringType(), False),
    StructField("UPDATED_AT", StringType(), False),
    StructField("PUSHED_AT", StringType(), False),
    StructField("STARGAZERS_CNT", IntegerType(), False),
    StructField("WATCHERS_CNT", IntegerType(), False),
    StructField("LANG_NM", StringType(), True),
    StructField("FORKS_CNT", IntegerType(), False),
    StructField("OPEN_ISSUE_CNT", IntegerType(), False),
    StructField("SCORE", FloatType(), False),
    StructField("LIC_ID", StringType(), True)  
])

# commit activity spark dataframe schema 설정
commit_activity = StructType([
    StructField("ID", StringType(), False),
    StructField("AUTHOR_NM", StringType(), True),
    StructField("AUTHOR_TYPE", StringType(), False),
    StructField("TOTAL_CNT", IntegerType(), False),
    StructField("WEEK_UTC", IntegerType(), False),
    StructField("ADD_CNT", IntegerType(), False),
    StructField("DEL_CNT", IntegerType(), False),
    StructField("COMMIT_CNT", IntegerType(), False),
    StructField("REPO_ID", StringType(), False)
])

# language_list_spark dataframe schema 설정
language_list = StructType([
    StructField("LANG_NM", StringType(), True),
    StructField("LANG_BYTE", IntegerType(), False), 
    StructField("REPO_ID", StringType(), False)
])

# release_tag_ spark dataframe schema 설정
release_tag_list = StructType([
    StructField("ID", StringType(), False),
    StructField("REL_NM", StringType(), False), 
    StructField("REPO_ID", StringType(), False)
])

# project list spark dataframe schema 설정
project_list = StructType([
    StructField("ID", StringType(), False),
    StructField("PROJ_NM", StringType(), False),
    StructField("BODY", StringType(), False),
    StructField("PROJ_NO", IntegerType(), False),
    StructField("PROJ_ST", StringType(), False),
    StructField("CREATED_AT", StringType(), False),
    StructField("UPDATED_AT", StringType(), False),
    StructField("REPO_ID", StringType(), False)
])

# fork list spark dataframe schema 설정
fork_list = StructType([
    StructField("ID", StringType(), False),
    StructField("FORK_NM", StringType(), False),
    StructField("OWNER_ID", StringType(), False),
    StructField("OWNER_NM", StringType(), False),
    StructField("URL", StringType(), False),
    StructField("CREATED_AT", StringType(), False),
    StructField("UPDATED_AT", StringType(), True),
    StructField("REPO_ID", StringType(), False)
])

# commit list spark dataframe schema 설정
commit_list = StructType([
    StructField("ID", StringType(), False),
    StructField("URL", StringType(), False),
    StructField("AUTHOR_ID", StringType(), True),
    StructField("AUTHOR_NM", StringType(), True),
    StructField("MASSAGE", StringType(), True),
    StructField("REPO_ID", StringType(), False)
])

# issue list spark dataframe schema 설정
issue_list = StructType([
    StructField("ID", StringType(), False),
    StructField("URL", StringType(), False),
    StructField("TITLE", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("USER_NM", StringType(), False),
    StructField("STATE", StringType(), True),
    StructField("CREATED_AT", StringType(), False),
    StructField("UPDATED_AT", StringType(), True),
    StructField("CLOSED_AT", StringType(), True),
    StructField("REPO_ID", StringType(), False)
])

# pr list spark dataframe schema 설정
pr_list = StructType([
    StructField("ID", StringType(), False),
    StructField("URL", StringType(), False),
    StructField("TITLE", StringType(), False),
    StructField("USER_ID", StringType(), False),
    StructField("USER_NM", StringType(), False),
    StructField("STATE", StringType(), True),
    StructField("CREATED_AT", StringType(), False),
    StructField("UPDATED_AT", StringType(), True),
    StructField("CLOSED_AT", StringType(), True),
    StructField("REPO_ID", StringType(), False)
])



