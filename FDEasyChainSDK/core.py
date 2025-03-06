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
import logging
import os
import time
from pathlib import Path
from typing import Any
import requests

from FDEasyChainSDK.exceptions import create_exception
from FDEasyChainSDK.utils import calculate_sign, generate_timestamp


class APICache:
    def __init__(self, expire_seconds: int = 30 * 24 * 3600):  # 默认30天
        self.expire_seconds = expire_seconds
        # 在用户主目录下创建缓存目录
        self.cache_dir = Path.home() / '.data-crawled' / 'FDEasyChain'
        print("CacheDir:", self.cache_dir)
        self.cache_dir.mkdir(exist_ok=True, parents=True)

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

    def __calculate_sign__(self, payload: dict, timestamp):
        return calculate_sign(self.app_id, timestamp, self.app_secret, payload)

    def __post__(self, api_path, payload: dict):
        url = self.api_endpoint + api_path
        # 标准化请求体，确保相同参数生成相同的缓存键
        try:
            # 将字典按键排序后重新序列化为JSON字符串，确保顺序一致性
            normalized_body = json.dumps(payload, sort_keys=True)
            # 生成缓存键
            cache_key = f"{api_path}:{normalized_body}"
        except json.JSONDecodeError:
            # 如果请求体不是有效的JSON，就使用原始请求体
            cache_key = f"{api_path}:{payload}"

        # 检查缓存
        cached_result = self._cache.get(cache_key)
        if cached_result is not None:
            logging.info(f"(缓存:Ok!) {url}")
            return cached_result, True

        timestamp = generate_timestamp()
        sign = self.__calculate_sign__(payload, timestamp)
        headers = {
            "APPID": self.app_id,
            "TIMESTAMP": timestamp,
            "SIGN": sign
        }
        n = 1
        while True:
            try:
                response = requests.post(url, headers=headers, json=payload)
                break
            except requests.exceptions.ConnectionError as e:
                delay = n * 1
                logging.error(e)
                print(f"等待{delay}s 后再进行请求....")
                time.sleep(delay)

        if response.status_code == 200:
            resp_json = response.json()
            service_code = resp_json.get("code")
            if service_code == 200:
                if "data" not in resp_json:
                    raise create_exception(
                        status_code=404,
                        message="响应中缺少 data 字段",
                        request=response.request,
                        response=response
                    )
                
                result = resp_json.get("data")
                if result is None:
                    raise create_exception(
                        status_code=404,
                        message="响应中 data 字段为空",
                        request=response.request,
                        response=response
                    )
                
                # 存入缓存
                self._cache.set(cache_key, result)
                logging.info(f"(200:Ok!) {url}")
                return result, False
            else:
                msg = resp_json.get("msg")
                # 使用 create_exception 创建异常，传入完整的请求和响应信息
                raise create_exception(
                    status_code=service_code,
                    message=msg,
                    request=response.request,  # requests 会在 response 中保存对应的 request
                    response=response
                )
        else:
            # 处理 HTTP 错误状态码
            raise create_exception(
                status_code=response.status_code,
                message=f"HTTP请求失败: {response.text}",
                request=response.request,
                response=response
            )

    def company_certificate_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        行政许可证
        :param key: 关键词(企业id/ 企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 当前企业的许可证信息列表，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - FILENO: 文件编号
                    - FILENAME: 许可文件名称
                    - VALFROM: 有效期自
                    - VALTO: 有效期至
                    - LICAUTH: 许可机关
                    - LICCONTENT: 许可内容
        """
        request_body = {"key": key}
        if page_index != 1:
            request_body["page_index"] = page_index
        if page_size != 20:
            request_body["page_size"] = page_size
        # api_path 的最后斜杠后缀必须要带
        return self.__post__('/company_certificate_query/', request_body)

    def company_impawn_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        股权质押
        :param key: 关键词(企业id/ 企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 当前企业的股权质押信息列表
        """
        request_body = {"key": key}
        if page_index != 1:
            request_body["page_index"] = page_index
        if page_size != 20:
            request_body["page_size"] = page_size
        return self.__post__('/company_impawn_query/', request_body)

    def company_bid_list_query(self, key: str, noticetype: str = None, btype: str = None,
                               gdate: str = None, page_index: int = 1, page_size: int = 20):
        """
        公司招投标信息查询
        :param key: 关键词(企业id/企业完整名称/统一社会信用代码)
        :param noticetype: 公告类型，可选值：
                          01招标公告、02中标公告、03废标公告、
                          04更正公告、05延期公告、06终止公告、
                          07资格预审、08询价公告、09竞争性谈判、
                          10竞争性磋商、11单一来源、12其他、
                          13成交公告、14流标公告、15结果公告、
                          16合同公告、17解除公告、18答疑澄清、
                          19资格预审
        :param btype: 角色类型，可选值：
                       95项目、01供应方、1中标方、2投标方、3代理方
        :param gdate: 公告年份，如2021，可选
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 招投标信息列表，包含以下字段：
                - data: 返回的数据对象
                - BIDLIST: 企业招标信息数据
                - total: 返回总数
                - datalist: 数据列表
                    - title: 公告标题
                    - noticetype: 公告类型
                    - region_name: 地区名称
                    - btype: 角色
                    - bidInviteList: 招标方列表
                        - entid: 企业id
                        - ENTNAME: 企业名称
                    - bidWinList: 中标方列表
                        - entid: 企业id
                        - ENTNAME: 企业名称
                    - agentList: 代理方列表
                        - entid: 企业id
                        - ENTNAME: 企业名称
        """
        params = {"key": key}
        if noticetype:
            params["noticetype"] = noticetype
        if btype:
            params["btype"] = btype
        if gdate:
            params["gdate"] = gdate
        params["page_index"] = page_index
        params["page_size"] = page_size

        request_body = params
        return self.__post__('/company_bid_list_query/', request_body)

    def company_news_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业新闻舆情查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业新闻舆情数据列表，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - author: 作者/来源平台
                    - title: 标题
                    - url: 来源URL
                    - event_time: 事件时间
                    - category: 新闻分类
                    - impact: 舆情倾向
                    - keywords: 文章关键词
                    - content: 新闻正文
                    - ENTNAME: 主体名称
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_news_query/', request_body)

    def company_fc_thirdtop_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业上榜榜单查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业上榜榜单数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - bangdan_name: 榜单名称
                    - bangdan_type: 榜单类型
                    - url: 来源url
                    - ENTNAME: 企业名称
                    - ranking: 排名(0表示榜单中企业排名不分先后)
                    - pdate: 发布日期
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_fc_thirdtop_query/', request_body)

    def company_billboard_golory_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业荣誉资质查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业荣誉资质数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - datefrom: 有效期起
                    - dateto: 有效期至
                    - ENTNAME: 企业名称
                    - golory_name: 荣誉名称
                    - pdate: 发布日期
                    - plevel: 荣誉级别
                    - status: 1有效3已期未知
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_billboard_golory_query/', request_body)

    def company_most_scitech_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业科技成果查询
        :param key: 关键词(企业id/企业完整名称)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业科技成果数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - QRYENTNAME: 企业名称
                    - desno: 登记号
                    - ENTNAME: 第一完成单位
                    - names: 成果完成人
                    - pname: 成果名称
                    - year: 年份
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_most_scitech_query/', request_body)

    def company_vc_inv_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业融资信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业融资数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 融资公司全称
                    - investdate: 融资发生日期
                    - invse_similar_money_name: 融资金额范围（如：'1000万-2000万', '数千万人民币'）
                    - invse_detail_money: 融资具体金额（单位：元）
                    - invse_guess_particulars:  本轮融资后的企业估值
                    - invse_round_name: 融资轮次（如：'天使轮','A轮', 'Pre-A轮'）
                    - org_name: 投资/融资机构名称
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_vc_inv_query/', request_body)

    def company_cnca5_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业认证认可查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业认证认可数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - cert_project: 认证项目
                    - cert_type: 证书类型
                    - award_date: 颁证日期
                    - expire_date: 证书到期日期
                    - cert_num: 证书编号
                    - org_num: 机构批准号
                    - org_name: 机构名称
                    - cert_status: 证书状态
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_cnca5_query/', request_body)

    def company_aggre_cert_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业电信许可证查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业电信许可证数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - LICSCOPE: 许可范围
                    - LICNAME: 许可文件名称
                    - LICNO: 许可文件编号
                    - VALFROM: 有效期自
                    - VALTO: 有效期至
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_aggre_cert_query/', request_body)

    def company_mlrland_transfer_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业土地转让查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业土地转让数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - address: 宗地地址
                    - city: 行政区
                    - ENTNAME_A: 原土地使用权人
                    - ENTNAME_B: 现土地使用权人
                    - trans_date: 成交时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_mlrland_transfer_query/', request_body)

    def company_job_info_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业招聘信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业招聘数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 公司名称
                    - title: 招聘标题
                    - pdate: 发布日期
                    - salary: 薪资
                    - province: 工作省份
                    - city: 工作城市
                    - experience: 工作年限
                    - education: 学历
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_job_info_query/', request_body)

    def company_tax_rating_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业纳税信用等级查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业纳税信用等级数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - TAXID: 纳税人识别号
                    - ENTNAME: 企业名称
                    - tyear: 评定年份
                    - rating: 评级
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_tax_rating_query/', request_body)

    def company_case_randomcheck_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业双随机抽查查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业双随机抽查数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - CheckPlanNo: 计划编号
                    - CheckTaskName: 任务名称
                    - CheckBelongOrg: 抽查机关
                    - CheckDoneDate: 完成日期
                    - detal_list: 双随机抽查明细数据
                    - CheckItem: 抽查事项
                    - CheckResult: 抽查结果
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_case_randomcheck_query/', request_body)

    def company_case_check_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业抽查检查查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业抽查检查数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - CHECKDATE: 巡查日期
                    - INSTYPE: 巡查类型
                    - LOCALADM: 属地监管工商所
                    - FOUNDPROB: 监管发现问题
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_case_check_query/', request_body)

    def company_case_abnormity_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业经营异常查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业经营异常数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - INDATE: 列入日期
                    - INREASON: 列入原因
                    - OUTDATE: 移出日期
                    - OUTREASON: 移出原因
                    - YC_REGORG: 列入/移出机关
                    - YR_REGORG: 登记/核入机关
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_case_abnormity_query/', request_body)

    def company_land_mort_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业土地抵押查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业土地抵押数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 土地抵押人名称
                    - ENTNAME_h: 土地抵押权人
                    - address: 宗地地址
                    - sdate: 起始登记日期
                    - edate: 结束登记日期
                    - mamount: 抵押金额(万元)
                    - moarea: 抵押面积(公顷)
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_land_mort_query/', request_body)

    def company_mort_info_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业动产抵押查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业动产抵押数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - MORTREGCNO: 登记编号
                    - REGDATE: 登记日期/注销日期
                    - REGORG: 登记机关
                    - MORTYPE: 状态(如注销并注明注销原因等)
                    - CANDATE: 注销时间
                    - MORCAREA: 注销范围
                    - PERSON: 抵押权人/出质人信息
                    - BLICNO: 证件号
                    - BLICTYPEPERSON: 证件类型
                    - MORE: 质权人
                    - CLAIM: 被担保债权信息
                    - PEFPERETO: 履行期限
                    - PRICLASSCAM: 被担保债权种类
                    - WAMCON: 担保范围
                    - GUAGES: 抵押物、质物、状况、所在地等信息
                    - GUANAME: 抵押物名称
                    - OWN: 所有权
                    - ALTER: 抵押物变更信息
                    - ALTDATE: 变更日期
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_mort_info_query/', request_body)

    def company_tax_case_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业重大税收违法查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业重大税收违法数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - case_nature: 案件性质
                    - ENTNAME: 纳税人名称
                    - eval_date: 认定日期
                    - puborg: 发布机关
                    - remarks: 主要违法事实、相关法律依据及处理处罚情况说明
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_tax_case_query/', request_body)

    def company_cancel_easy_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业简易注销查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业简易注销数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - filepath: 承诺书路径
                    - REGORG: 登记机关
                    - UNICODE: 统一社会信用码
                    - date_from: 公告自
                    - date_to: 公告至
                    - result: 审核结果
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_cancel_easy_query/', request_body)

    def company_liquidation_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业清算信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业清算信息数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - LICPRINCIPAL: 清算负责人
                    - LIQMEN: 清算组成员
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_liquidation_query/', request_body)

    def company_tax_arrears_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业欠税信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业欠税信息数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 纳税人名称
                    - camount: 本期新欠金额
                    - debt: 总欠税额
                    - pubtime: 发布日期
                    - tax_org: 所属税务机关
                    - taxcate: 纳税人国税/地税
                    - taxtype: 欠税税种
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_tax_arrears_query/', request_body)

    def company_case_yzwfsx_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业严重违法查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业严重违法数据，包含以下字段：
                - total: 返回总数
                - datalist: 数据列表
                    - ENTNAME: 企业名称
                    - indate: 列入日期
                    - inorg: 列入决定机关
                    - inreason: 列入原因
                    - outdate: 列出日期
                    - outorg: 列出决定机关
                    - outreason: 列出原因
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_case_yzwfsx_query/', request_body)

    def company_standard_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业国家标准信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业国家标准信息数据，包含以下字段：
                - data: 返回的数据对象
                    - BzCountry: 国家标准数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - standard_kinds: 标准属性
                            - pdate: 发布日期
                            - link: 全文链接
                            - mid: 国家标准Id
                            - abolish_date: 废止日期
                            - DraftName: 起草单位
                            - jurisdictional_unit: 归口单位
                            - content: 全文
                            - execute_unit: 执行单位
                            - issued_date: 发布日期/实施日期
                            - standard_status: 状态
                            - CSIC: 中国标准分类号
                            - standard_num: 标准号
                            - Drafter: 起草人
                            - id: Id
                            - department: 主管部门
                            - similar: 相近标准(计划)
                            - standard_level: 标准级别
                            - ISIC: 国际标准分类号
                            - created: 输入时间
                            - url: Url
                            - standard_name: 标准名称
                            - ENTNAME: 企业名称
                            - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_bz_country_query/', request_body)

    def company_bz_industry_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业行业标准信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业行业标准信息数据，包含以下字段：
                - data: 返回的数据对象
                    - BzIndustry: 行业标准数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - mid: 行业标准id
                            - standard_num: 标准号
                            - standard_name: 标准名称
                            - pdate: 发布日期
                            - issued_date: 实施日期
                            - standard_status: 状态
                            - standard_level: 标准级别
                            - standard_kinds: 标准属性
                            - PR: 制修订
                            - CSIC: 中国标准分类号
                            - ISIC: 国际标准分类号
                            - jurisdictional_unit: 技术归口
                            - department: 批准发布部门
                            - category: 标准类别
                            - classification: 行业分类
                            - area: 适用范围
                            - bdate: 备案日期
                            - abolish_date: 废止日期
                            - bnum: 备案号
                            - DraftsName: 起草单位
                            - Drafter: 起草人
                            - pdf_path: pdf存储路径
                            - pdf_url: pdf url
                            - url: Url
                            - created: 输入时间
                            - updated: 更新时间
                            - ENTNAME: 企业名称
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_bz_industry_query/', request_body)

    def company_basic_query(self, key: str):
        """
        企业基本信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :return: 企业基本信息数据，包含以下字段：
                - data: 返回的数据对象
                    - BASIC: 基本信息数据
                        - parent: 上级产业id
                        - code: 产业id
                        - entid: 企业id
                        - fulltitle: 完整行业代码的对应中文统名称
                        - REGCAP_CN: 注册资本名称(GS)
                        - UNISCID: 统一信用代码
                        - FRNAME: 法人姓名
                        - REGNO: 工商注册号
                        - faq: 行业代码的INDUSTRYCO字段的门户解释
                        - TAXID: 纳税人识别号
                        - APPRDATE: 核准日期
                        - region_name: 地区名
                        - RECCAP: 实收资本
                        - id: 无意义
                        - OPFROM: 经营期限开始日期
                        - codeNicList: 同行业代码去掉字母
                        - DOM: 地址
                        - scode: 同行业代码去掉字母
                        - REGCAP: 注册资本
                        - level: 行业代码对应层级
                        - NACAOID: 组织机构代码
                        - created: 数据库创建时间
                        - REGCAPCUR: 注册资本单币种
                        - region_id: 地区码
                        - ENTTYPE: 公司类型
                        - params: 参数
                        - version: 码表版本
                        - ENTTYPE_id: 企业类型id
                        - nic_name: 行业名称
                        - OPSCOPE: 经营范围
                        - ESDATE: 成立日期
                        - name: 行业名称
                        - ENTNAME: 企业名称
                        - updated: 数据库更新时间
                        - INDUSTRYCO: 行业代码
        """
        request_body = {"key": key}
        return self.__post__('/company_basic_query/', request_body)

    def company_dishonest_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        失信被执行人查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 失信被执行人数据，包含以下字段：
                - data: 返回的数据对象
                    - LESSCREDIT: 失信被执行人信息数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - CASECODE: 案号
                            - NAME: 被执行人名称
                            - LTYPE: 类别
                            - SEX: 性别
                            - AGE: 年龄
                            - faren: 法定代表人或负责人姓名
                            - LASJ: 立案时间
                            - PDATE: 发布时间
                            - COURT: 执行法院
                            - AREA: 省份
                            - ZXFY: 执行依据文号
                            - AUTHORG: 做出执行依据单位
                            - DUTY: 生效法律文书确定的义务
                            - DISRUPT: 失信被执行人行为具体情形
                            - PERFORMANCE: 被执行人的履行情况
                            - PERFORMED: 已履行部分
                            - UNPERFORM: 未履行部分
                            - EXITDATE: 退出日期
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_dishonest_query/', request_body)

    def company_court_execute_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        被执行人查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 被执行人信息数据，包含以下字段：
                - data: 返回的数据对象
                    - EXECUTE: 被执行人信息数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - FSS_CASENO: 案号
                            - FSS_COURTNAME: 执行法院名称
                            - FSS_LASJ: 立案时间
                            - FSS_MONEY: 执行标的
                            - FSS_NAME: 被执行人姓名/名称
                            - FSS_REGNO: 组织机构代码
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_court_execute_query/', request_body)

    def company_software_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        软件著作权查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 软件著作权数据，包含以下字段：
                - data: 返回的数据对象
                    - CopyrightSoftware: 软件著作权数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - ustatus: 软件状态
                            - SHORTNAME: 软件简称
                            - SNUM: 登记号
                            - ANNDATE: 登记批准日期
                            - REGDATE: 首次发表日期
                            - VNUM: 版本号
                            - author: 著作人
                            - SNAME: 软件全称
                            - ENTNAME: 企业名称
                            - ANNTYPE: 分类号名称
                            - updated: 最后更新时间
                            - TYPENUM: 分类号编号
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_software_query/', request_body)

    def company_patent_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        专利基本信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 专利基本信息数据，包含以下字段：
                - data: 返回列表
                    - PATENTS: 专利数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - GNGKGGH: 公告号国内
                            - GKGGH: 公告号国际
                            - FASQ: 分案申请
                            - GJGB: PCT国际公布
                            - GJSQ: PCT国际申请
                            - GSDM: 国家或地区
                            - ZLDLJG: 专利代理机构
                            - SQGGH: 授权公告号
                            - SQGKRQ: 授权公开日期
                            - ZQX: 主权项
                            - JRGJRQ: 进入国家日期
                            - FCFL: 范畴分类
                            - FLH: 分类号
                            - PTYPE: 专利分类
                            - GKGGR: 公开日期
                            - FMR: 发明/设计人
                            - PID: 专利id
                            - SQH: 申请号
                            - DLR: 代理人
                            - YZWX: 引证文献
                            - PIC: 代表图片
                            - PATNAME: 专利标题
                            - YXQ : 优先权
                            - SQR: 申请/专利权人
                            - DZ: 申请人地址
                            - ENTNAME: 申请企业
                            - SQRQ: 申请日期
                            - ZFLNAME: 主分类号
                            - IPC: 专利行业分类全称
                            - ZY: 摘要
                            - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_patent_query/', request_body)

    def company_copyright_production_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        作品著作权查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 作品著作权数据，包含以下字段：
                - data: 返回列表
                    - CopyrightProduction: 作品著作权数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - wnum: 登记号
                            - wname: 作品名称
                            - wtype: 作品类别
                            - cdate: 创作完成日期
                            - fdate: 首次发表日期
                            - rdate: 登记日期
                            - ENTNAME: 著作权人姓名/名称
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_copyright_production_query/', request_body)

    def company_tminfo_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        商标基本信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 商标基本信息数据，包含以下字段：
                - data: 返回列表
                    - TMINFO: 商标信息数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - mid: 商标ID
                            - tnum: 注册号/申请号
                            - tmcat: 商标分类
                            - tname: 商标名称
                            - application_date	: 申请日期
                            - ENTNAME: 注册人中文名称
                            - ENTNAME_E: 注册人外文名称
                            - agency: 代理机构
                            - tstyle: 商标类型
                            - pstatus: 商标状态
                            - pic_path: 图片oss地址
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_tminfo_query/', request_body)

    def company_ipr_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        知识产权出质查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 知识产权出质数据，包含以下字段：
                - data: 返回列表
                    - KNOWPOWER: 知识产权出质数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - IPRTYPE: 种类
                            - IPRNAME: 名称
                            - IPRNO: 知识产权登记证号
                            - PUBDATE: 公示日期
                            - IMPORG: 质权人名称
                            - PLEDGOR: 出质人名称
                            - REGFROM: 质权登记自
                            - REGTO: 质权登记至
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_ipr_query/', request_body)

    def company_icp_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        ICP网站备案查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: ICP网站备案数据，包含以下字段：
                - data: 返回列表
                    - ICPREG: ICP网站备案数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - mid: 唯一键
                            - ENTNAME: 开办者名称
                            - shdate: 审核/备案日期
                            - lable: 备案类型
                            - hostname: 网站域名
                            - icpnum: 公安备案号
                            - webname: 网站名称
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_icp_query/', request_body)

    def fuzzy_query(self, key: str):
        """
        企业模糊搜索
        :param key: 关键词
        :return: 企业模糊搜索，包含以下字段：
                - data: 返回列表
                    - ENTNAME: 企业名称
                    - entid: 企业id
                    - historyname: 历史名
                    - regcap: 注册资本
        """
        request_body = {
            "key": key
        }
        return self.__post__('/fuzzy_query/', request_body)

    def company_punish_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        行政处罚查询
        :param key: 关键词(企业id/ 企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 行政处罚数据，包含以下字段：
                - data: 返回的数据对象
                    - PunishInfo: 大数据行政处罚数据
                        - total: 返回总数
                        - datalist:数据列表
                            - PENEXEST: 处罚执行情况
                            - PENAM: 处罚金额
                            - PENBASIS: 处罚依据
                            - CASETIME: 案发时间
                            - CASERESULT: 案件结果
                            - ILLEGFACT: 主要违法事实
                            - PENRESULT: 处罚结果
                            - PENDECNO: 处罚决定文书
                            - CASEVAL: 案值
                            - PENDECISSDATE: 处罚决定书签发日期
                            - CASETYPE: 文书类型（案件类型）
                            - CASEREASON: 案由
                            - EXESORT: 执行类别
                            - PENAUTH: 处罚机关
                            - UNEXECMONEY: 未执行金额
                            - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_punish_query/', request_body)

    def company_justice_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        司法协助查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 司法协助数据，包含以下字段：
                - data: 返回的数据对象
                    - CASESFXZ: 司法协助数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - sfxz_jcdj: 司法协助-解除冻结相关数据
                                - EXE_ITEM: 执行事项
                                - EXE_NAME: 被执行股东
                                - court: 执行法院
                                - updated: 更新时间
                            - sfxz_gqdj: 司法协助-主表相关数据
                                - EXE_ITEM: 执行事项
                                - entid_exe: 案号
                                - EXE_NO: 执行法院
                                - EXE_CONAM: 立案时间
                                - ENTNAME: 被执行人姓名/名称
                                - EXE_NAME: 被执行股东
                                - court: 执行法院
                                - updated: 更新时间
                            - sfxz_gdbg: 司法协助-股东变更相关数据
                                - EXE_ITEM: 执行事项
                                - EXE_NAME: 被执行股东
                                - court: 执行法院
                                - updated: 更新时间
                            - sfxz_cl: 司法协助-续行信息相关数据
                                - EXE_ITEM: 执行事项
                                - ENTNAME:被执行人姓名/名称
                                - EXE_NAME: 被执行股东
                                - court: 执行法院
                                - updated: 更新时间
                            - sfxz_djsx: 司法协助-冻结失效相关数据
                                - INVREA: 失效原因
                                - IDATE: 失效日期
                                - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_justice_query/', request_body)

    def company_court_cpws_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        裁判文书查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 每页大小，默认20
        :return: 裁判文书数据，包含以下字段：
                - data: 返回的数据对象
                    - COURT_CPWS: 裁判文书数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - mid: 文书id
                            - title: 标题
                            - causename: 案由名称
                            - CASENO: 案号
                            - pname: 主体名称
                            - ptname: 主体类型名称
                            - judgeresult: 判决结果
                            - pdate: 裁定日期
                            - sdate: 发布日期
                            - casetype: 案件类型
                            - wenshutype: 文书类型
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_court_cpws_query/', request_body)

    def company_court_ktgg_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        开庭公告查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 开庭公告数据，包含以下字段：
                - data: 返回的数据对象
                    - COURT_KTGG: 开庭公告数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - CASENO: 案号
                            - causename: 案由
                            - sdate: 开庭日期
                            - courtname: 法院名称
                            - plaintiffName: 原告
                            - defendantName: 被告
                            - otherName: 其他当事人
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_court_ktgg_query/', request_body)

    def company_court_endcase_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        终本案件查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 终本案件数据，包含以下字段：
                - data: 返回的数据对象
                    - COURT_ENDCASE: 终本案件数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - CASENO: 案号
                            - courtname: 执行法院名称
                            - sdate: 终本时间
                            - bdate: 立案时间
                            - execMoney: 执行标的
                            - unnexeMoney: 未履行金额
                            - pname: 当事人
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_court_endcase_query/', request_body)

    def company_court_lian_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        立案信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 立案信息数据，包含以下字段：
                - data: 返回的数据对象
                    - COURT_LIAN: 立案信息数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - causename: 案由
                            - CASENO: 案号
                            - adate: 立案日期
                            - courtname: 法院
                            - casestatus: 案件状态
                            - defendant: 被告人/被告/被上诉人/被申请人
                            - prosecutor: 公诉人/原告/上诉人/申请人
                            - otherName: 其他
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_court_lian_query/', request_body)

    def company_court_xgl_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        限制高消费查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 限制高消费数据，包含以下字段：
                - data: 返回的数据对象
                    - XGL: 限制高消数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - CASENO: 案号
                            - causename: 案由
                            - pname: 关联对象
                            - sdate: 立案时间
                            - pdate: 发布时间
                            - limitobj: 限消令对象
                            - petitioner: 申请人
                            - entid_exe: 申请人（企业）id
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_court_xgl_query/', request_body)

    def company_bankruptcy_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        破产重整查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 破产重整数据，包含以下字段：
                - data: 返回的数据对象
                    - BANKRUPTCY: 企业破产重整数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - CASENO: 案号
                            - pdate: 公开日期
                            - Applicant: 申请人/上诉人
                            - Respondent: 被申请人/被上诉人
                            - pname: 企业名称
                            - title: 公告标题
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_bankruptcy_query/', request_body)

    def entadvquery(self, filterFlag: str, keyEntName: str, keyEntNameFlag:str,
                    keyOpscope: str, keyOpscopeFlag: str, keyAddr: str, keyAddrFlag: str,
                    region_id: str, industry: str, esdate: str, regcap: str, ent_status:str,
                    list_status:str, list_count:str, company_tech:str, pageNum: int = 1, pageSize: int = 20 ):
        """
        高级筛选
        :param filterFlag: 搜索关系
        :param keyEntName: 企业名称
        :param keyEntNameFlag: 企业名称精度
        :param keyOpscope: 经营范围
        :param keyOpscopeFlag:经营范围精度
        :param keyAddr: 注册地址
        :param keyAddrFlag	: 注册地址精度
        :param region_id: 省份地区
        :param industry: 行业分类
        :param esdate: 成立年限
        :param regcap: 注册资本
        :param ent_status: 企业状态
        :param list_status: 上市状态
        :param list_count: 融资信息
        :param company_tech: 科技企业标签
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 高级筛选数据，包含以下字段：
                - data: 返回列表
                    - ent: 企业列表数据
                        - total: 返回datalist总数
                        - datalist: 数据列表
                            - ent_name: 企业名称
                            - uniscid: 纳税人识别号
                            - faren: 法人
                            - opscope: 经营范围
                            - dom: 注册地址
                            - region_name: 省份地区
                            - nic_name: 行业名称
                            - esdate: 成立日期
                            - regcap: 注册资本
                            - regcapcur: 注册资本单位币种
                            - ent_status_name: 企业状态
                            - listed_state: 上市状态
                            - list_name: 融资轮次
                            - company_tech_tag: 科技企业标签
        """
        request_body = {
            "filterFlag":filterFlag,
            "pageNum":pageNum,
            "pageSize":pageSize,
            "keyEntNameFlag":keyEntNameFlag,
            "keyEntName":keyEntName,
            "keyOpscopeFlag":keyOpscopeFlag,
            "keyOpscope":keyOpscope,
            "keyAddrFlag":keyAddrFlag,
            "keyAddr":keyAddr,
            "region_id":region_id,
            "industry":industry,
            "esdate":esdate,
            "regcap":regcap,
            "ent_status":ent_status,
            "list_status":list_status,
            "list_count":list_count,
            "company_tech":company_tech
        }
        return self.__post__('/entadvquery/', request_body)
    def company_listing_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业主板新三板上市查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业主板新三板上市查询数据，包含以下字段：
        主板数据：    - data: 返回列表
                    - STKCOM: 主板数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - STOCKCODE: 股票代码
                            - STOCKSNAME: 股票简称
                            - LIST_SEC: 上市版块
                            - TRADE_MKT	: 交易市场
                            - STK_TYPE: 股票类别
                            - ISIN: ISIN编码
                            - LIST_DATE: 上市日期
                            - LIST_ENDDATE: 退市日期
                            - STATUS_TYPE: 上市状态
                            - SPECIAL_TYPE	: 特别处理状态
                            - LEG_PERSON: 法定代表人
                            - WEB_SITE: 公司官网
                            - GEN_MANAGER: 总经理
                            - DISTRICT_NO: 区号
                            - BOARD_SECTRY	: 董事会秘书姓名
                            - REPR: 证券事务代表姓名
                            - PRI_BIZ: 主营业务
                            - NON_PRI_BIZ: 兼营业务
                            - PRI_PRD: 主营产品
                            - CSRC_INDU: 证监会行业分类
                            - COM_BRIEF: 公司简介
                            - LOGO: 企业logo
        新三板数据：  - data: 返回列表
                    - STASCOM: 新三板数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - STOCKCODE: 股票代码
                            - STOCKSNAME: 股票简介
                            - LIST_SEC: 上市版块 1-两网及退市公司板块 2-挂牌公司板块
                            - TRADE_MKT	: 交易市场
                            - WEB_SITE: 公司官网
                            - LEG_PERSON: 法定代表人
                            - ORG_STATUS: 机构状态 1-成立, 2-筹建, 3-注销, 4-撤销筹建, 9-其他
                            - CHAIRMAN: 董事长
                            - GEN_MANAGER: 总经理
                            - AREA	: 地区
                            - BOARD_SECTRY: 董事会秘书姓名
                            - REPR: 证券事务代表姓名
                            - OFFICE_ADDR: 办公地址
                            - POSTCODE_OFFICE: 办公地址邮政编码
                            - SCOPE_BUSS	: 经营范围
                            - PRI_BIZ: 主营业务
                            - BRIEF_INTRO: 公司简介
                            - LOGO: 企业logo
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_listing_query/', request_body)

    def company_listed_pub_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        上市公司公告查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 上市公司公告数据，包含以下字段：
                - data: 返回列表
                    - LISTEDPUB: 企业上市公告数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - ENTNAME: 企业名称
                            - secCode: 证券代码
                            - secName: 证券简称
                            - title: 公告标题
                            - sdate: 公告日期
                            - title: 公告标题
                            - gtype_1: 公告分类
                            - pdf_url: pdf原始地址
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_listed_pub_query/', request_body)

    def company_aggre_list_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        港股上市
        :param key: 关键词(港股企业完整名称)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 港股上市数据，包含以下字段：
                - data: 返回列表
                    - AGGRELIST: 港股上市数据
                        - total: 返回datalist总数
                        - datalist: 数据列表
                            - ENTNAME: 公司名称
                            - LIST_DATE: 上市日期
                            - LIST_SECTOR: 上市版块:1 主板;2 创业板;3 中小企业板;4  新三板;5 科创板;6 香港证券交易所;7 美股
                            - MKT_TYPE: 所属交易所:1 上交所;2 深交所;3 股份转让系统;4 香港证券交易所
                            - SEC_CODE: 证券代码
                            - SEC_SNAME: 证券简称
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_aggre_list_query/', request_body)

    def company_listed_tenstk_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        十大流通股东查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 十大流通股东数据，包含以下字段：
                - data: 返回列表
                    - TENSTK: 十大流通股东数据
                        - total: 返回datalist总数
                        - datalist: 数据列表
                            - ENTNAME: 企业名称
                            - ENDDATE: 截止日期
                            - A_STOCKCODE	: 证券代码
                            - A_STOCKSNAME	: 证券简称
                            - holder_list: 股东详情列表
                            - NAME: 股东名称
                            - CHNG_REAS_NAME: 变动原因
                            - HOLD_NUM: 持有总数量
                            - HOLD_PCT	: 持有比例
                            - ADD_NUM	: 报告期内增减股份数量
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_listed_tenstk_query/', request_body)

    def company_stockholder_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        股东信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 股东信息数据，包含以下字段：
                - data: 返回的数据对象
                    - SHAREHOLDER: 股东信息数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - CONPROP: 出资比例
                            - COUNTRY: 国家（地区）
                            - BLICNO: 证照号码
                            - SUBCONAM: 认缴出资金额
                            - CONFORM: 出资方式
                            - CONDATE: 出资日期
                            - SHANAME: 股东名称
                            - updated: 更新时间
                            - BLICTYPE: 证照类型
                            - INVTYPE: 投资人类型
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_stockholder_query/', request_body)

    def company_management_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        高管信息查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 高管信息数据，包含以下字段：
                - data: 返回的数据对象
                    - PERSON: 高管信息数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - ISFRDB: 是否法人
                            - POSITION: 职位
                            - positionTotal: 企业主要人员数量
                            - PERSONPIC: 高管头像
                            - personid: 人物id
                            - invTotal: 企业高管数量
                            - updated: 更新时间
                            - NAME: 姓名
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_management_query/', request_body)

    def company_changeRecord_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        变更记录查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 变更记录数据，包含以下字段：
                - data: 返回的数据对象
                    - ALTER: 变更数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - ALTDATE: 变更日期
                            - ALTAF: 变更后
                            - ALTITEM: 变更事项
                            - ALTBE: 变更前
                            - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_changeRecord_query/', request_body)

    def company_investment_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        企业对外投资查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 企业对外投资数据，包含以下字段：
                - data: 返回的数据对象
                    - ENTINV: 企业对外投资数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - ENTSTATUS	: 企业状态
                            - entid: 企业id
                            - REGCAP: 注册资本
                            - regInstitute: 登记机关名称(GS)
                            - SUBCONAM: 认缴出资金额
                            - REGCAPCUR: 币种
                            - ENTTYPE: 企业类型
                            - CONDATE: 出资日期
                            - faren	: 法人
                            - REGCAPCN: 注册资本名称(GS)
                            - ESDATE: 成立日期
                            - creditCode: 统一社会信用代码
                            - province: 住所所在行政区划
                            - REGNO: 工商注册号
                            - ENTNAME: 企业名称
                            - CONFORM: 出资方式
                            - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_investment_query/', request_body)

    def company_branch_query(self, key: str, page_index: int = 1, page_size: int = 20):
        """
        分支机构查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :param page_index: 页码索引，默认1
        :param page_size: 页面大小，默认20
        :return: 分支机构数据，包含以下字段：
                - data: 返回的数据对象
                    - FILIATION: 分支机构数据
                        - total: 返回总数
                        - datalist: 数据列表
                            - BRADDR: 企业地址
                            - ESDATE: 成立日期
                            - ENTSTATUS: 企业状态
                            - entid: 企业id
                            - CBUITEM: 一般经营项目
                            - BRREGNO: 分支机构企业注册号
                            - BRNAME: 分支机构名称
                            - BRPRINCIPAL: 分支机构负责人
                            - UNISCID	: 统一社会信用代码
                            - updated: 更新时间
        """
        request_body = {
            "key": key,
            "page_index": page_index,
            "page_size": page_size
        }
        return self.__post__('/company_branch_query/', request_body)

    def related_multi_new(self, related_args: str, tab: str, depth: 20):
        """
        企业关系链查询
        :param related_args: 关系参数,"type" 中 "e" 表示企业类型，"p" 表示企业+人名类型；"type" 为 "p" 时,"key" 传参必须是 "eMBp0EeCkKE-林斌" 格式
        :param tab: "A"：关联路径，"B"：关系追踪；默认为"A"
        :param depth: 下钻层次，"tab"传参为"A"时该传参才生效，默认为"4"
        :return: 企业关系链数据，包含以下字段：
                - data: 返回的数据对象
                    - edges: 企业关系链数据
                        - relation: 关系说明
                        - source: 源企业唯一标识
                        - source_name: 源企业/源个人名称
                        - target: 目标企业唯一标识
                        - target_name: 目标企业/目标个人名称
                        - type: "line"表示普通连接线，"best"表示最短路径连接线
                    - nodes: 节点
                        - id: 企业唯一标识
                        - imgtype: "com"表示企业，"person"表示个人
                        - name: 企业/个人名称
                        - type: 企业或个人类型，"e"表示企业，"p"表示个人
                    - table: 企业表单信息
                        - conam: 企业投资出资金额
                        - conprop: 企业投资出资比例
                        - eid: 企业唯一标识
                        - esdate: 企业成立时间
                        - id: 该条数据唯一标识
                        - lerepsign: 是否法人
                        - name: 企业/个人名称
                        - nicid: 行业分类名称
                        - ntype: 企业或个人类型，"e"表示企业，"p"表示个人
                        - position: 职务
                        - regcap: 企业注册资金
                        - regionid: 企业所在地区名称
                        - roadid: 路径号
                        - rowname: 路径详情文字说明；若是"企业"/"个人"文字说明为企业/个人名称；否则直接为文字说明，例："投资"
                        - status: 企业状态
        """
        request_body = {
            "related_args": related_args,
            "tab": tab,
            "depth": depth
        }
        return self.__post__('/related_multi_new/', request_body)

    def company_ar_query(self, key: str):
        """
        企业年报查询
        :param key: 关键词(企业id/企业完整名称/社会统一信用代码)
        :return: 企业年报数据，包含以下字段：
                - data: 返回的数据对象
                    - ARYEAR: 企业年报数据
                        - total: 返回datalist总数 只返回1条
                        - datalist: 基本信息表
                        - ALTER: 股权变更表
                        - WEBSITE: 网站信息表
                        - CAPITAL: 出资信息表
                        - GUAR: 对外担保信息表
                        - ASSET: 资产信息表
                        - SOCIAL: 社保信息表
                        - datalist: 基本信息表
                            - ANCHEDATE: 年报时间
                            - ANCHEYEAR	: 年报年度
                            - BUSST: 经营状态
                            - DOM: 企业通信地址
                            - EMAIL: 电子邮箱
                            - EMPNUM: 从业人数
                            - EMPNUMDIS: 从业人数是否公示（0否 1是）
                            - ENTNAME: 企业名称
                            - has_equity: 是否发生股东股权转让
                            - has_extguarantee: 是否有对外提供担保信息
                            - has_invest: 是否有投资信息或购买其他公司股权（0否 1是）
                            - has_website: 是否有网站或网店
                            - holdingSmsg: 控股类型
                            - holdingSmsgDis: 控股类型是否公示（0否 1是）
                            - MAINBUSIACT: 主营业务
                            - parIns: 党组织类型
                            - parIns_code: 党组织类型编码
                            - POSTALCODE: 邮政编码
                            - REGNO: 注册号
                            - TEL: 企业联系电话
                            - UNISCID: 统一社会信用代码
                            - WOMEMPNUM: 女性从业人数
                            - WOMEMPNUMDIS: 女性从业人数是否公示（0否 1是）
                        - ALTER: 股权变更表
                            - altdate: 股权变更日期
                            - ancheyear: 年报年度
                            - inv: 股东名称
                            - transamaft: 转让后股权比例
                            - transampr: 转让前股权比例
                        - WEBSITE: 网站信息表
                            - ancheyear: 年报年度
                            - domain: 网站（网店）网址
                            - websitname: 网络经营者拥有的网站或网店名称
                            - webtype: 网站网店类型1网站2网店
                        - CAPITAL: 出资信息表
                            - acconam: 累计实缴额
                            - accondate: 实缴出资日期
                            - ancheyear: 年报年度
                            - inv: 股东/发起人名称
                            - subconam: 累计认缴额
                            - subcondate: 认缴出资日期
                        - GUAR: 对外担保信息表
                            - ancheyear: 年报年度
                            - gatype: 保证的方式1一般保证2连带保证3未约定
                            - guaranperiod: 保证的期间1期限2未约定
                            - more: 债权人
                            - mortgagor: 债务人
                            - pefperform: 履行债务的期限自
                            - pefperto: 履行债务的期限至
                            - priclasecam: 主债权数额
                            - priclaseckind: 主债权种类1合同2其他
                        - ASSET: 资产信息表
                            - ancheyear: 年报年度
                            - assgro: 资产总额
                            - assgrodis: 资产总额是否公示（0否 1是）
                            - liagro: 负债总额
                            - liagrodis: 负债总额是否公示（0否 1是）
                            - maibusinc: 主营业务收入
                            - maibusincdis: 主营业务收入是否公示（0否 1是）
                            - netinc: 净利润
                            - netincdis: 净利润是否公示（0否 1是）
                            - progro: 利润总额
                            - progrodis: 利润总额是否公示（0否 1是）
                            - ratgro: 纳税总额
                            - ratgrodis: 纳税总额是否公示（0否 1是）
                            - totequ: 所有者权益合计
                            - totequdis: 所有者权益合计是否公示（0否 1是）
                            - vendinc: 营业总收入
                            - vendincdis: 营业总收入是否公示（0否 1是）
                        - SOCIAL: 社保信息表
                            - anchedate: 发布日期
                            - ancheyear: 年报年份
                            - so1: 城镇职工养老保险人数
                            - so2: 失业保险人数
                            - so3: 职工医疗保险人数
                            - so4: 工伤保险人数
                            - so5: 生育保险人数
                            - totalPaymentSo1: 城镇职工养老保险实缴基数
                            - totalPaymentSo2: 失业保险实缴基数
                            - totalPaymentSo3: 职工医疗保险实缴基数
                            - totalPaymentSo4: 工伤保险实缴基数
                            - totalPaymentSo5: 生育保险实缴基数
                            - totalWagesSo1: 城镇职工养老保险缴费基数
                            - totalWagesSo2: 失业保险缴费基数
                            - totalWagesSo3: 职工医疗保险缴费基数
                            - totalWagesSo4: 工伤保险缴费基数
                            - totalWagesSo5: 生育保险缴费基数
                            - unPaidSocialInsSo1: 城镇职工养老保险累计欠缴
                            - unPaidSocialInsSo2: 失业保险累计欠缴
                            - unPaidSocialInsSo3: 职工医疗保险累计欠缴
                            - unPaidSocialInsSo4: 工伤保险累计欠缴
                            - unPaidSocialInsSo5: 生育保险累计欠缴
        """
        request_body = {
            "key": key
        }
        return self.__post__('/company_ar_query/', request_body)
