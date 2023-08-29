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
        file_path = self.s3_client.get_file_name_from_s3('de-2-2', 'raw/github/repo/')
        repo_json = self.s3_client.read_json_from_s3('de-2-2', file_path)
        return pd.DataFrame(repo_json)
    
    def commit_activity(self):
        file_path = self.s3_client.get_file_name_from_s3('de-2-2', 'raw/github/metric/')
 
        metric_json = self.s3_client.read_json_from_s3('de-2-2', file_path)
        matric_json_transformed = []
        for REPO_ID, commit_activities in metric_json.items():
            for commit_activity in commit_activities['commit_activity']:
                matric_json_transformed.append(
                    {
                        'ID' : commit_activity['ID'],
                        'AUTHOR_NM' : commit_activity['AUTHOR_NM'],
                        'AUTHOR_TYPE' : commit_activity['AUTHOR_TYPE'],
                        'TOTAL_CNT' : commit_activity['TOTAL_CNT'],
                        'WEEK_UTC' : commit_activity['WEEK_UTC'],
                        'ADD_CNT' : commit_activity['ADD_CNT'],
                        'DEL_CNT' : commit_activity['DEL_CNT'],
                        'COMMIT_CNT' : commit_activity['COMMIT_CNT'],
                        'REPO_ID' : commit_activity['REPO_ID'],
                })

        return pd.DataFrame(matric_json_transformed)

    def info_list(self):
        file_path = self.s3_client.get_file_name_from_s3('de-2-2', 'raw/github/info/')
        info_json = self.s3_client.read_json_from_s3('de-2-2', file_path)

        release_json = []
        project_json = []
        language_json = []
        fork_json = []

        for REPO_ID in info_json:
            if info_json[REPO_ID]['release']:
                for release_dict in info_json[REPO_ID]['release']:
                    release_json.append({
                        'ID' : release_dict['ID'],
                        'REL_NM' : release_dict['REL_NM'],
                        'REPO_ID': REPO_ID
                    })
            if info_json[REPO_ID]['project']:
                for project_dict in info_json[REPO_ID]['project']:
                    project_json.append({
                        'ID' : project_dict['ID'],
                        'PROJ_NM' : project_dict['PROJ_NM'],
                        'BODY' : project_dict['BODY'],
                        'PROJ_NO' : project_dict['PROJ_NO'],
                        'PROJ_ST' : project_dict['PROJ_ST'],
                        'CREATED_AT' : project_dict['CREATED_AT'],
                        'UPDATED_AT' : project_dict['UPDATED_AT'],
                        'REPO_ID': REPO_ID
                    })
            if info_json[REPO_ID]['language']:
                for language_dict in info_json[REPO_ID]['language']:
                    language_json.append({
                        'LANG_NM' : language_dict['LANG_NM'],
                        'LANG_BYTE' : language_dict['LANG_BYTE'],
                        'REPO_ID': REPO_ID
                    })
            if info_json[REPO_ID]['fork']:
                for fork_dict in info_json[REPO_ID]['fork']:
                    fork_json.append({
                        'ID' : fork_dict['ID'],
                        'FORK_NM' : fork_dict['FORK_NM'],
                        'OWNER_ID' : fork_dict['OWNER_ID'],
                        'OWNER_NM' : fork_dict['OWNER_NM'],
                        'URL' : fork_dict['URL'],
                        'CREATED_AT' : fork_dict['CREATED_AT'],
                        'UPDATED_AT' : fork_dict['UPDATED_AT'],
                        'REPO_ID': REPO_ID
                    })

        release_tag_df = pd.DataFrame(release_json)
        project_df = pd.DataFrame(project_json)
        language_df = pd.DataFrame(language_json)
        fork_df = pd.DataFrame(fork_json)

        return release_tag_df, project_df, language_df, fork_df

    
    def detail_list(self):
        file_path = self.s3_client.get_file_name_from_s3('de-2-2', 'raw/github/detail/')
        detail_json = self.s3_client.read_json_from_s3('de-2-2', file_path)

        issue_json = []
        pr_json = []
        commit_json = []
        for REPO_ID in detail_json:
            if detail_json[REPO_ID]['issue']:
                for issue_dict in detail_json[REPO_ID]['issue']:
                    issue_json.append({
                        'ID' : issue_dict['ID'],
                        'URL' : issue_dict['URL'],
                        'TITLE' : issue_dict['TITLE'],
                        'USER_ID' : issue_dict['USER_ID'],
                        'USER_NM' : issue_dict['USER_NM'],
                        'STATE' : issue_dict['STATE'],
                        'CREATED_AT' : issue_dict['CREATED_AT'],
                        'UPDATED_AT' : issue_dict['UPDATED_AT'],
                        'CLOSED_AT' : issue_dict['CLOSED_AT'],
                        'REPO_ID': REPO_ID
                    })
            if detail_json[REPO_ID]['pr']:
                for pr_dict in detail_json[REPO_ID]['pr']:
                    pr_json.append({
                        'ID' : pr_dict['ID'],
                        'URL' : pr_dict['URL'],
                        'TITLE' : pr_dict['TITLE'],
                        'USER_ID' : pr_dict['USER_ID'],
                        'USER_NM' : pr_dict['USER_NM'],
                        'STATE' : pr_dict['STATE'],
                        'CREATED_AT' : pr_dict['CREATED_AT'],
                        'UPDATED_AT' : pr_dict['UPDATED_AT'],
                        'CLOSED_AT' : pr_dict['CLOSED_AT'],
                        'REPO_ID': REPO_ID
                    })
            if detail_json[REPO_ID]['commit']:
                for commit_dict in detail_json[REPO_ID]['commit']:
                    commit_json.append({
                        'ID' : commit_dict['ID'],
                        'URL' : commit_dict['URL'],
                        'AUTHOR_ID' : commit_dict['AUTHOR_ID'],
                        'AUTHOR_NM' : commit_dict['AUTHOR_NM'],
                        'MESSAGE' : commit_dict['MESSAGE'],
                        'REPO_ID': REPO_ID
                    })
            
            
        issue_df = pd.DataFrame(issue_json)
        pr_df = pd.DataFrame(pr_json)
        commit_df = pd.DataFrame(commit_json)

        return issue_df, pr_df, commit_df

