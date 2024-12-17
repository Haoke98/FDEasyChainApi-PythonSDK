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
from pathlib import Path
from typing import Any
import requests

from FDEasyChainSDK.utils import calculate_sign, generate_timestamp


class APICache:
    def __init__(self, expire_seconds: int = 30 * 24 * 3600):  # 默认30天
        self.expire_seconds = expire_seconds
        # 在用户主目录下创建缓存目录
        self.cache_dir = Path.home() / '.data_do_well_cache'
        self.cache_dir.mkdir(exist_ok=True)

    def _get_cache_file(self, key: str) -> Path:
        # 使用MD5对缓存键进行哈希，避免文件名过长或包含特殊字符
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.json"

    def get(self, key: str) -> Any:
        cache_file = self._get_cache_file(key)
        if not cache_file.exists():
            return None

        try:
            with cache_file.open('r', encoding='utf-8') as f:
                cache_data = json.load(f)
                timestamp = cache_data['timestamp']
                if time.time() - timestamp < self.expire_seconds:
                    return cache_data['value']
                else:
                    # 过期则删除缓存文件
                    cache_file.unlink(missing_ok=True)
        except (json.JSONDecodeError, KeyError, OSError):
            # 如果读取出错，删除可能损坏的缓存文件
            cache_file.unlink(missing_ok=True)
        return None

    def set(self, key: str, value: Any):
        cache_file = self._get_cache_file(key)
        cache_data = {
            'timestamp': time.time(),
            'value': value
        }
        try:
            with cache_file.open('w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except OSError:
            # 写入失败时，确保不会留下损坏的缓存文件
            cache_file.unlink(missing_ok=True)


# FiveDegreeEasyChain 5度易链
class EasyChainCli:
    def __init__(self, debug: bool = False, cache_expire_seconds: int = 30 * 24 * 3600):  # 默认30天
        self.app_id = os.getenv("DATA_DO_WELL_API_KEY")
        self.app_secret = os.getenv("DATA_DO_WELL_API_SECRET")
        self.api_endpoint = "https://gateway.qyxqk.com/wdyl/openapi"
        self.debug = debug
        self._cache = APICache(expire_seconds=cache_expire_seconds)

    def __calculate_sign__(self, request_body, timestamp):
        return calculate_sign(self.app_id, timestamp, self.app_secret, request_body)

    def __post__(self, api_path, request_body):
        url = self.api_endpoint + api_path
        if self.debug:
            print("(调试信息) URL:", url)
            print("(调试信息) RequestBody:", request_body)
        # 标准化请求体，确保相同参数生成相同的缓存键
        try:
            # 解析JSON字符串为字典
            body_dict = json.loads(request_body)
            # 将字典按键排序后重新序列化为JSON字符串，确保顺序一致性
            normalized_body = json.dumps(body_dict, sort_keys=True)
            # 生成缓存键
            cache_key = f"{api_path}:{normalized_body}"
        except json.JSONDecodeError:
            # 如果请求体不是有效的JSON，就使用原始请求体
            cache_key = f"{api_path}:{request_body}"

        # 检查缓存
        cached_result = self._cache.get(cache_key)
        if cached_result is not None:
            if self.debug:
                print(f"(调试信息) Response(缓存):", cached_result)
            return cached_result

        timestamp = generate_timestamp()
        sign = self.__calculate_sign__(request_body, timestamp)
        headers = {
            "APPID": self.app_id,
            "TIMESTAMP": timestamp,
            "SIGN": sign,
            "Content-Type": "application/json"
        }

        if self.debug:
            print("(调试信息) Headers:", headers)
        response = requests.post(url, headers=headers, data=request_body)

        if self.debug:
            print(f"(调试信息) Response({response.status_code}):", response.text)

        if response.status_code == 200:
            resp_json = response.json()
            service_code = resp_json.get("code")
            if service_code == 200:
                result = resp_json["data"]
                # 存入缓存
                self._cache.set(cache_key, result)
                return result
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
        return self.__post__('/company_certificate_query/', request_body)

    def company_impawn_query(self, uscc: str):
        """
        股权质押
        :param uscc:
        :return:
        """
        request_body = '{"key": "%s"}' % uscc
        return self.__post__('/company_impawn_query/', request_body)
