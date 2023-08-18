from flask import Flask, jsonify
import requests
import redis
import json
import boto3
from botocore.exceptions import ClientError
import logging


app = Flask(__name__)
redis = redis.StrictRedis(host='localhost', port=6379, db=5, decode_responses=True)

AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SECRET_REGION="us-east-1"
TOKEN_NAME = "github_token"

def get_aws_secret(secret_name: str) -> dict:
    """
    # AWS Secret Manager에서 값을 가져옴
    :param secret_name: 시크릿 이름
    :param secret_key: 시크릿에서 가져올 키
    :return: 시크릿 값 (없을 시 None 반환)
    """
    try:
        session = boto3.session.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        client = session.client(
            service_name='secretsmanager',
            region_name=AWS_SECRET_REGION
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret_str = get_secret_value_response['SecretString']
        if secret_str:
            secret_data = json.loads(secret_str)
            return secret_data
        else:
            logging.info("SecretString not found in the response.")
            return {}
    except ClientError as e:
        logging.error(f"Error retrieving secret: {e}") # 로깅
        return None

def update_tokens_usage(tokens: dict, is_update:bool):
    """
    # 토큰의 사용량을 가져와 Redis에 처음 저장

    :param tokens: 토큰 정보가 담긴 딕셔너리
    """
    for token_key, token_value in tokens.items():
        token_usage = get_token_usage(token_value)
        logging.info(f"조회 완료: {token_key} {token_usage}")
        if token_usage:
            save_tokens_info(token_key, token_value, token_usage, is_update)
        else:
            logging.error(f"Failed to fetch usage for token: {token_key}")

def get_token_usage(token_value: str):
    """
    # 토큰의 사용량 정보를 가져옴
    https://docs.github.com/en/free-pro-team@latest/rest/rate-limit/rate-limit?apiVersion=2022-11-28#get-rate-limit-status-for-the-authenticated-user
    
    :param token_value: API 토큰 값
    :return: 사용량 정보 딕셔너리 또는 None (에러 시)
    """
    headers = {
        "Authorization": f"Bearer {token_value}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    try:
        response = requests.get("https://api.github.com/rate_limit", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to fetch rate limit for token: {token_value}. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error while fetching rate limit for token: {token_value}\nError: {e}")
    return None

def save_tokens_info(token_key: str, token_value: str, token_usage, is_update:bool=False):
    """
    # 토큰의 사용량 정보를 Redis에 저장.
    
    :param token_key: 토큰의 키
    :param token_value: 토큰의 값
    :param token_usage: 사용량 정보 딕셔너리(json)
    """
    redis.hset(token_key, "token_key", token_key)
    redis.hset(token_key, "token_value", token_value)
    redis.hset(token_key, "total_limit", token_usage.get("rate").get("limit"))
    redis.hset(token_key, "total_remaining", token_usage.get("rate").get("remaining"))
    redis.hset(token_key, "total_reset", token_usage.get("rate").get("reset"))
    redis.hset(token_key, "core_limit", token_usage.get("resources").get("core").get("limit"))
    redis.hset(token_key, "core_remaining", token_usage.get("resources").get("core").get("remaining"))
    redis.hset(token_key, "core_reset", token_usage.get("resources").get("core").get("reset"))
    redis.hset(token_key, "search_limit", token_usage.get("resources").get("search").get("limit"))
    redis.hset(token_key, "search_remaining", token_usage.get("resources").get("search").get("remaining"))
    redis.hset(token_key, "search_reset", token_usage.get("resources").get("search").get("reset"))
    if is_update == False:
        redis.hset(token_key, "allocated", "false")

@app.before_request
def init_tokens():
    """
    # AWS Secret Manager에서 토큰 정보 가져와 redis에 업데이트
    token 정보 최신 상태 유지: 다른 api 호출될때 마다 해당 함수도 update됨
    """
    git_tokens = get_aws_secret(TOKEN_NAME)
    update_tokens_usage(git_tokens, is_update=True)


def execute_once():
    """
    # 초기 1회 셋팅 (서비스 재시작시 가동)
    """
    git_tokens = get_aws_secret(TOKEN_NAME)
    update_tokens_usage(git_tokens, is_update=False)

@app.route('/get_all_tokens', methods=['GET'])
def get_all_tokens():
    """
    # 모든 토큰의 정보를 가져옴
    
    :return: 토큰 정보 딕셔너리
    """
    all_tokens = {}
    keys = redis.keys("token*")
    for key in keys:
        token_info = redis.hgetall(key)
        all_tokens[key] = token_info
    return jsonify(all_tokens)

@app.route('/get_token_info/<token_key>', methods=['GET'])
def get_token_info(token_key: str):
    """
    # 특정 토큰의 정보를 가져옴
    
    :param token_key: 토큰의 키
    :return: 토큰 정보 딕셔너리
    """
    token_info = redis.hgetall(token_key)
    return jsonify(token_info) 

@app.route('/get_available_token/<api_type>', methods=['GET'])
def get_available_token(api_type : str):
    """
    # 사용 가능한 토큰 가져오기

    :param api_type: 가져올 토큰 타입 ('total', 'core', 'search')
    """
    logging.debug(f"api_type :{api_type}")
    if api_type not in ("total", "core", "search"):
        return jsonify("Please enter a valid <api_type>. <api_type> should be 'total', 'core', or 'search'")

    tokens = []
    for key in redis.keys("*"):
        token_info = redis.hgetall(key)
        allocated = token_info.get('allocated')
        remaining = int(token_info.get(f"{api_type}_remaining"))
        reset = int(token_info.get(f"{api_type}_reset"))
        logging.debug(f"{key} 확인 : {allocated} {remaining} {reset}")
        if allocated == "false" and remaining > 0:
            tokens.append({
                "key": key,
                "remaining": remaining,
                "reset": reset
            })
    
    if tokens:
        tokens = sorted(tokens, key=lambda x: (x["reset"], -x["remaining"]))
        logging.debug(tokens)
        token_info = tokens[0]
        redis.hset(token_info["key"], "allocated", "true")
        token_all_info = redis.hgetall(token_info['key'])
        return jsonify(token_all_info)
    else:
        return jsonify(message="No available tokens")


@app.route('/return_token/<token_key>', methods=['GET'])
def return_token(token_key: str):
    """
    # 토큰 반납하기

    :param token_key: 반납할 토큰의 키
    """
    is_allocated = redis.hget(token_key, "allocated")
    if is_allocated == "false":
        return jsonify("Not Access. You did not have permission to return token")
    redis.hset(token_key, "allocated", "false")
    return jsonify("Succees to Return token")

@app.route('/delete_all_tokens', methods=['POST'])
def delete_all_tokens():
    """
    # 모든 토큰 삭제하기
    """
    keys = redis.keys("*")
    for key in keys:
        redis.delete(key)
    return jsonify("All tokens have been deleted.")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
