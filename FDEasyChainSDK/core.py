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

from FDEasyChainSDK.utils import calculate_sign, generate_timestamp, logger


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
        # 初始化logger
        logger.init("EasyChainCli")

    def __calculate_sign__(self, payload:dict, timestamp):
        return calculate_sign(self.app_id, timestamp, self.app_secret, payload)

    def __post__(self, api_path, payload:dict):
        url = self.api_endpoint + api_path
        if self.debug:
            print("(调试信息) URL:", url)
            print("(调试信息) RequestBody:", payload)
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
            if self.debug:
                print(f"(调试信息) Response(缓存):", cached_result)
            return cached_result

        timestamp = generate_timestamp()
        sign = self.__calculate_sign__(payload, timestamp)
        headers = {
            "APPID": self.app_id,
            "TIMESTAMP": timestamp,
            "SIGN": sign,
            "Content-Type": "application/json"
        }

        if self.debug:
            print("(调试信息) Headers:", headers)
        n = 1
        while True:
            try:
                response = requests.post(url, headers=headers, data=payload)
                break
            except requests.exceptions.ConnectionError as e:
                delay = n * 1
                logging.error(e)
                print(f"等待{delay}s 后再进行请求....")
                time.sleep(delay)
        if self.debug:
            print(f"(调试信息) Response({response.status_code}):", response.text)

        if response.status_code == 200:
            resp_json = response.json()
            service_code = resp_json.get("code")
            if service_code == 200:
                result = resp_json.get("data", None)
                # 存入缓存
                self._cache.set(cache_key, result)
                return result
            else:
                raise Exception("业务异常")
        else:
            raise Exception("请求异常")

    def company_certificate_query(self, key: str,page_index: int = 1, page_size: int = 20):
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
                    - bidwinList: 中标方列表
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
                    - investdate: 投资日期
                    - invse_similar_money_name: 投资的近似金额名称
                    - invse_detail_money: 投资的详细金额
                    - invse_guess_particulars: 估值明细
                    - invse_round_name: 投资的轮次名称
                    - org_name: 机构名称
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

    def company_mirland_transfer_query(self, key: str, page_index: int = 1, page_size: int = 20):
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
        return self.__post__('/company_mirland_transfer_query/', request_body)

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
                    - UNICODE: 统一社会信用代码
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

    def company_case_ywfwt_query(self, key: str, page_index: int = 1, page_size: int = 20):
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
        return self.__post__('/company_case_ywfwt_query/', request_body)
