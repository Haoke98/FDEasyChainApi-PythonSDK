import hashlib
import json
import os
import time

import requests
from dotenv import load_dotenv

from FDEasyChainSDK.utils import debug

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_NAME = BASE_DIR.split(os.path.sep)[-1]
USER_HOME_DIR = os.path.expanduser("~")
USER_CONFIG_DIR = os.path.join(USER_HOME_DIR, ".config")
PROJ_CONFIG_DIR = os.path.join(USER_CONFIG_DIR, "FDEasyChain")
print("PROJ_CONFIG_DIR:", PROJ_CONFIG_DIR)
if not os.path.exists(PROJ_CONFIG_DIR):
    os.makedirs(PROJ_CONFIG_DIR)
ENV_FILE_PATH = os.path.join(PROJ_CONFIG_DIR, ".env")
print("ENV_FILE_PATH:", ENV_FILE_PATH)
load_dotenv(ENV_FILE_PATH)

APP_ID = os.getenv("DATA_DO_WELL_API_KEY")
SECRET = os.getenv("DATA_DO_WELL_API_SECRET")
debug("APP_ID:", APP_ID)
debug("SECRET(已脱敏):", SECRET[0] + "*" * (len(SECRET) - 2) + SECRET[-1])
debug("API_ID:", APP_ID)
API_ENDPOINT = "https://gateway.qyxqk.com/wdyl/openapi/company_impawn_query/"
debug("API_ENDPOINT:", API_ENDPOINT)


def generate_timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def calculate_sign(app_id, timestamp, secret, request_body):
    payload = json.loads(request_body)

    # 构建拼接字符串

    concat_str = ''.join(payload.values())

    # 计算签名

    sign_string = app_id + timestamp + secret + concat_str

    md5_hash = hashlib.md5()

    md5_hash.update(sign_string.encode('utf-8'))

    sign = md5_hash.hexdigest()

    return sign


def main():
    request_body = '{"key": "91110115782522603X","page_index": "1","page_size": "20"}'

    timestamp = generate_timestamp()

    sign = calculate_sign(APP_ID, timestamp, SECRET, request_body)

    headers = {

        "APPID": APP_ID,

        "TIMESTAMP": timestamp,

        "SIGN": sign,

        "Content-Type": "application/json"

    }
    debug("Headers:",headers)
    debug("RequestBody:",request_body)
    response = requests.post(API_ENDPOINT, headers=headers, data=request_body)

    debug("Response:", response.text)


if __name__ == "__main__":
    main()
    # resp = requests.get("https://ip.useragentinfo.com/json")
    # debug("请求地IP地址: ", resp.json())
