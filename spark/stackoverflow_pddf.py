# -*- coding: utf-8 -*-
import json
import pandas as pd
from awsfunc import awsfunc
from datetime import datetime


class PD_df:
    def __init__(self):
        self.s3_client = awsfunc('s3')
        self.timestamp = datetime.now().strftime("%Y/%m/%d")
    
    def totimestamp(self, unixtime):
        if unixtime is None:
            return None
        else:
            return  str(datetime.utcfromtimestamp(unixtime))


    def question_list(self):
        file_path = self.s3_client.get_file_name_from_s3('de-2-2', 'raw/stackoverflow/question_list/')
 
        question_json = self.s3_client.read_json_from_s3('de-2-2', file_path)
        question_json_transformed = []
        for REPO_ID, questions in question_json.items():
            for question in questions['question']:
                question_json_transformed.append(
                    {
                        'ID' : question['ID'],
                        'TITLE' : question['TITLE'],
                        'USER_ID' : str(question['USER_ID']),
                        'USER_NM' : question['USER_NM'],
                        'URL' : question['URL'],
                        'CREATED_AT' : self.totimestamp(question['CREATED_AT']),
                        'UPDATED_AT' : self.totimestamp(question['UPDATED_AT']),
                        'REPO_ID' : question['REPO_ID'],
                })


        return pd.DataFrame(question_json_transformed)

    