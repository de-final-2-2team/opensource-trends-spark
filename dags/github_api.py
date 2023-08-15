from sre_parse import State
from requests.exceptions import HTTPError
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task, task_group

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_header():
    token = Variable.get("github_token")
    api_version = Variable.get("git_api_version")
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": api_version
    }


def exception_parser(context):
    """
    A function that checks the class name of the Exception thrown.
    
    Different exceptions trigger behaviour of allowing the task to fail, retry or succeed
    """
    ti = context["task_instance"]
    exc = context.get('exception')
    if isinstance(exc, HTTPError):
        logging.error(repr(exc))
        if exc.response.status_code == 401:
            logging.warning("Retry Task")
        elif exc.response.status_code == 404:
            ti.set_state(State.SUCCESS)
        else:
            ti.set_state(State.FAILED)
    else:
        logging.error("Unknown Error. " + repr(exc))
        ti.set_state(State.FAILED)


@task(
    description="api 요청 결과를 반환",
    retries=1,
    retry_delay=timedelta(seconds=3),
    on_retry_callback=exception_parser,
)
def extract(kind, url, params=None):
    try:
        logging.info(f"[{kind}] 데이터 수집 시작")
        headers = get_header()
        response = requests.get(url=url, 
                                headers=headers,
                                params=params)
        # 200이 아닌 경우 에러 발생
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"[{kind}] 데이터 수집 실패\\n" + repr(e))
        raise e


@task(
    description="json 파일 형태로 저장",
)
def load_as_json(kind, content):
    with open(f"./{kind}.json", 'w') as f:
        json.dump(content, f)
    if isinstance(content, list):
        logging.info(f"[{kind}] 데이터 수집 완료 | 데이터 수: {len(content)}")
    else:
        logging.info(f"[{kind}] 데이터 수집 완료")


"""
DAG 스케줄 간격 설정
1. 매크로 프리셋 활용
    schedule_interval = '@daily'
2. cron 활용
    schedule = 'miniute(0-59) hour(0-23) day_of_month(1-31) month(1-12) day_of_week(0-6, 0:sunday)'
3. timedelta 활용
    schedule_interval = timedelta(days=3)

DAG 설정
참고: https://airflow.apache.org/docs/apache-airflow/2.2.3/_api/airflow/models/dag/index.html#airflow.models.dag.DAG
1. catchup
    - True(default): 이전 DAG 종료된 이후 다음 DAG를 실행
    - False: 이전 DAG가 실행 중이더라도 예정대로 다음 DAG를 실
"""
with DAG(
    dag_id='github_meta',
    description='github API meta 데이터 저장',
    start_date=datetime(2023, 8, 15, 21),  # 날짜가 미래인 경우 실행이 안됨
    schedule='10 * * * *',
    max_active_runs=1,
    catchup=False,
) as dag:
    url = Variable.get("meta_url")
    params = Variable.get("meta_params")

    load_as_json(dag.dag_id, extract(dag.dag_id, url, params))