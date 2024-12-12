import os
import time

import requests
from dotenv import load_dotenv

from fiveDegreeEasyChainSDK import calculate_sign, debug

load_dotenv("/Users/shadikesadamu/.config/HZXY-DataHandling/.env")
APP_ID = os.getenv("DATA_DO_WELL_API_KEY")

SECRET = os.getenv("DATA_DO_WELL_API_SECRET")
debug("APP_ID:", APP_ID)
debug("SECRET(已脱敏):", SECRET[0] + "*" * (len(SECRET) - 2) + SECRET[-1])
debug("API_ID:", APP_ID)
API_ENDPOINT = "https://gateway.qyxqk.com/wdyl/openapi/company_impawn_query/"
debug("API_ENDPOINT:", API_ENDPOINT)


def generate_timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def main():
    request_body = '{"key": "91110115782522603X"}'

    timestamp = generate_timestamp()

    sign = calculate_sign(APP_ID, timestamp, SECRET, request_body)

    headers = {

        "APPID": APP_ID,

        "TIMESTAMP": timestamp,

        "SIGN": sign,

        "Content-Type": "application/json"

    }

    response = requests.post(API_ENDPOINT, headers=headers, data=request_body)

    debug("Response:", response.text)


if __name__ == "__main__":
    main()
    resp = requests.get("https://ip.useragentinfo.com/json")
    debug("请求地IP地址: ", resp.json())
