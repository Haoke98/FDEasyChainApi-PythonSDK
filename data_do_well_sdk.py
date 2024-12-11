# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/11
@Software: PyCharm
@disc:
======================================="""
import hashlib

import json
import os

import time

import requests


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


class DataDoWellCli:
    def __init__(self, debug: bool = False):
        self.app_id = os.getenv("DATA_DO_WELL_API_KEY")
        self.app_secret = os.getenv("DATA_DO_WELL_API_SECRET")
        self.api_endpoint = "https://gateway.qyxqk.com/wdyl/openapi"
        self.debug = debug

    def __calculate_sign__(self, request_body, timestamp):
        return calculate_sign(self.app_id, timestamp, self.app_secret, request_body)

    def __post__(self, api_path, request_body):
        url = self.api_endpoint + api_path

        timestamp = generate_timestamp()
        sign = self.__calculate_sign__(request_body, timestamp)
        headers = {
            "APPID": self.app_id,
            "TIMESTAMP": timestamp,
            "SIGN": sign,
            "Content-Type": "application/json"
        }
        if self.debug:
            print("(调试信息) URL:", url)
            print("(调试信息) Headers:", headers)
            print("(调试信息) RequestBody:", request_body)
        response = requests.post(url, headers=headers, data=request_body)
        if self.debug:
            print(f"(调试信息) Response({response.status_code}):", response.text)
        if response.status_code == 200:
            resp_json = response.json()
            service_code = resp_json.get("code")
            if service_code == 200:
                return resp_json["data"]
            else:
                raise Exception("业务异常")
        else:
            raise Exception("请求异常")

    def company_certificate_query(self, uscc: str):
        """
        行政许可证
        :param uscc: 社会统一信用代码
        """
        request_body = '{"key": "%s"}' % uscc
        # api_path 的最后斜杠后缀必须要带
        return self.__post__('/companyCertificate/', request_body)

    def company_impawn_query(self, uscc: str):
        """
        股权质押
        :param uscc:
        :return:
        """
        request_body = '{"key": "%s"}' % uscc
        return self.__post__('/company_impawn_query/', request_body)
