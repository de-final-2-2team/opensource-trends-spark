from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator, EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor
from datetime import datetime
from plugins.slack import SlackAlert

def send_slack_message():
    from plugins.awsfunc import awsfunc

    s3_handler = awsfunc('secretsmanager')
    slack_token = s3_handler.getapikey(secret_id="slack-token")
    slack_alert = SlackAlert(channel="#monitoring_airflow", token=slack_token)
    return slack_alert

SPARK_STEPS = [
    {
        "Name": "emr_repo_list",      # 시스팀 업데이트
        "ActionOnFailure": "CONTINUE",  # 실패해도 다음 스텝 실행
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                'bash', '-c',
                'pip3 install findspark PyArrow pandas boto3 --use-feature=2020-resolver && ' +
                'aws s3 cp s3://de-2-2/spark_scripts/github_repo_list_transform.py ./ && ' +
                'aws s3 cp s3://de-2-2/spark_scripts/awsfunc.py ./ && ' +
                'aws s3 cp s3://de-2-2/spark_scripts/github_schema.py ./ && ' +
                'aws s3 cp s3://de-2-2/spark_scripts/github_pddf.py ./ && ' +
                'python3 github_repo_list_transform.py'
            ],
        },
    },
]


JOB_FLOW_OVERRIDES = {
    "Name": "DE-2-2-EMR_Repo_List",
    "ReleaseLabel": "emr-6.12.0",
    "Applications": [{"Name": "Spark"}],
    "LogUri": "s3://de-2-2/cluster-log",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        'Ec2SubnetId': 'subnet-0ff43f8d26bd81499',
    },
        
    "Steps": SPARK_STEPS,
    "JobFlowRole": "Amazone-DE-2-2-EMR_EC2_DefaultRole",
    "ServiceRole": "Amazone-DE-2-2-EMR_DefaultRole",
}

with DAG(
    dag_id="emr_repo_list",
    start_date=datetime(2023, 8, 29),
    schedule='50 */12 * * *',
    catchup=False,
    on_success_callback=send_slack_message().success_alert,
    on_failure_callback=send_slack_message().fail_alert
) as dag:
    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='AWS_CONN_DE-2-2',
        region_name='us-east-1',
    )

    wait_for_emr_cluster = EmrJobFlowSensor(
        task_id='wait_for_emr_cluster',
        aws_conn_id="AWS_CONN_DE-2-2",
        job_flow_id=create_job_flow.output,
        target_states='WAITING',
        failed_states='TERMINATED_WITH_ERRORS',
    )

    remove_job_flow = EmrTerminateJobFlowOperator(
        task_id="remove_job_flow",
        job_flow_id=create_job_flow.output,
        aws_conn_id="AWS_CONN_DE-2-2",
    )

    create_job_flow >> wait_for_emr_cluster >> remove_job_flow