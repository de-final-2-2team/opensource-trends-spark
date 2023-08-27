# -*- coding: utf-8 -*-
import json
import pandas as pd
from awsfunc import awsfunc

s3_client = awsfunc('s3')

# s3 to emr
repo_json = s3_client.read_json_from_s3('de-2-2', 'raw/github/repository_list/2023/08/27.json')
metric_json = s3_client.read_json_from_s3('de-2-2', 'raw/github/metric/2023/08/27.json')
info_json = s3_client.read_json_from_s3('de-2-2', 'raw/github/info/2023/08/27.json')
detail_json = s3_client.read_json_from_s3('de-2-2', 'raw/github/detail/2023/08/27.json')

# json to pandas DF
commit_activity_json = metric_json['commit_activity']
release_json = info_json['release']        
project_json = info_json['project']        
language_json = info_json['language']       
fork_json = info_json['fork']      
issue_and_pr_json = detail_json['issue_and_pr']
commit_json = detail_json['commit']

# padns DF to spark DF 
repo_list = pd.DataFrame(repo_json[0])
commit_activity = pd.DataFrame(commit_activity_json)
release_tag = pd.DataFrame(release_json)
project_list = pd.DataFrame(project_json)
language_list = pd.DataFrame(language_json)
fork_list = pd.DataFrame(fork_json)
issue_and_pr = pd.DataFrame(issue_and_pr_json)
commit_list = pd.DataFrame(commit_json)