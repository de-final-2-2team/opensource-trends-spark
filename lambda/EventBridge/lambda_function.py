# -*- coding: utf-8 -*-
import json, random
import httplib
from botocore.vendored import requests

def lambda_handler(event, context):
    slack_url = 'https://hooks.slack.com/services/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'

    payloads = {
            "attachments":[{
                "pretext": "AWS EC2 관리 봇",
                "color":"#0099A6",
                "fields": [{
                    "title": "EC2 상태가 변경되었습니다.",
                    "value": json.dumps(event["detail"]["state"]), #EC2 state 정보
                    "short": False
                }]
            }]
        }
    response = requests.post(
        slack_url, data=json.dumps(payloads),
        headers={'Content-Type': 'application/json'}
    )
    if response.status_code != 200:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (response.status_code, response.text)
        )