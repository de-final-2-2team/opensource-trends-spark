# -*- coding: utf-8 -*-
import json
import pandas as pd
from awsfunc import awsfunc
from datetime import datetime


class PD_df:
    def __init__(self):
        self.s3_client = awsfunc('s3')
        self.timestamp = datetime.now().strftime("%Y/%m/%d")

    def repo_list(self):
        repo_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/repository_list/{self.timestamp}.json')
        return pd.DataFrame(repo_json)
    
    def commit_activity(self):
        metric_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/metric/{self.timestamp}.json')
        return pd.DataFrame(metric_json)

    def release_tag(self):
        info_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/info/{self.timestamp}.json')
        release_json = info_json['release'] 

        return pd.DataFrame(release_json)
    
    def project_list(self):
        info_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/info/{self.timestamp}.json')
        project_json = info_json['project'] 

        return pd.DataFrame(project_json)
    
    def language_list(self):
        info_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/info/{self.timestamp}.json')
        language_json = info_json['language']
        
        return pd.DataFrame(language_json)
    
    def fork_list(self):
        info_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/info/{self.timestamp}.json')
        fork_json = info_json['fork'] 

        return pd.DataFrame(fork_json)
    
    def issue_and_pr(self):
        detail_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/detail/{self.timestamp}.json')
        issue_and_pr_json = detail_json['issue_and_pr']

        return pd.DataFrame(issue_and_pr_json)

    def commit_list(self):
        detail_json = self.s3_client.read_json_from_s3('de-2-2', f'raw/github/detail/{self.timestamp}.json')
        commit_json = detail_json['commit']

        return pd.DataFrame(commit_json)

