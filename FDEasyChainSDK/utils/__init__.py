# _*_ codign:utf8 _*_
"""====================================
@Author:Sadam·Sadik
@Email：1903249375@qq.com
@Date：2024/12/17
@Software: PyCharm
@disc:
======================================="""
import hashlib
import json
import time


def generate_timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def debug(*args, **kwargs):
    print("(调试信息) ", *args)


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
