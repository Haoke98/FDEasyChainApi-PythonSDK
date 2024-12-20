import logging
import os

import mysql.connector
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

from FDEasyChainSDK import EasyChainCli

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

esCli = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv('SLRC_ES_HOST'),
                      basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                      ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)
INDEX = "ent-mdsi-v4"


def get_db_connection():
    # 从环境变量中获取数据库连接信息
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABASE')  # 假设你有一个数据库名称
    }
    return mysql.connector.connect(**db_config)


def fetch_firm_list():
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dc_z_firm_first_docking GROUP BY firm_uncid")
    firms = cursor.fetchall()
    cursor.close()
    connection.close()
    return firms


if __name__ == '__main__':
    ddwCli = EasyChainCli(debug=True)
    firm_list = fetch_firm_list()
    total = len(firm_list)
    for i, firm in enumerate(firm_list, 1):
        progress = i / total * 100
        print(f"{progress:.2f}% ({i}/{total})", firm, end=':\n')
        db_id, chain_id, chain_name, chain_node_id, chain_node_name, firm_uncid, is_local_fir, has_over = firm
        # TODO: 从五度易链API开放平台的接口中调取多维数据
        # TODO: 股权质押
        resp_data = ddwCli.company_impawn_query(firm_uncid)
        if resp_data is None:
            logging.error(f"企业[{firm_uncid}]没有股权质押数据，接口返回没有data字段体")
            continue
        impawn = resp_data["IMPAWN"]
        impawn_total = impawn["total"]
        print("\t\t", "股权押质: ", impawn_total)
        if impawn_total > 0:
            impawn_list = impawn['datalist']
            bulk_actions = []
            for impawn_item in impawn_list:
                pledge_info = {
                    "pledgor": impawn_item["pledgor"],  # 出质人
                    "relatedCompany": impawn_item["RelatedCompany"],  # 出质股权标的企业
                    "pledgee": impawn_item["IMPORG"],  # 质权人
                    "amount": impawn_item["IMPAM"],  # 质押金额
                    "execState": impawn_item["EXESTATE"],  # 执行状态
                    "recDate": impawn_item.get("IMPONRECDATE", None)  # 质押备案日期
                }
                # 假设每个pledge_info是一个字典，包含股权质押信息
                # 构建更新操作的action部分
                action = {
                    "_op_type": "update",
                    "_index": INDEX,
                    "_id": firm_uncid,
                    "script": {
                        "source": '''
                        if (ctx._source.sharePledgeData == null) {
                             ctx._source.sharePledgeData = [:];
                        }
                        if (ctx._source.sharePledgeData.dataList == null) {
                            ctx._source.sharePledgeData.dataList = []; 
                        } 
                        ctx._source.sharePledgeData.dataList.add(params.pledgeInfo);
                        ctx._source.sharePledgeData.totalNum=ctx._source.sharePledgeData.dataList.length;
                        ''',
                        "params": {
                            "pledgeInfo": pledge_info
                        }
                    }

                }
                # 将action添加到bulk请求体中
                bulk_actions.append(action)
                # 使用helpers.bulk来执行bulk更新
            try:
                success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                if errors:
                    for j, err in enumerate(errors):
                        updateBox = err['update']
                        status = updateBox['status']
                        errorBox = updateBox['error']
                        errType = errorBox['type']
                        if errType == 'document_missing_exception':
                            raise Exception(errorBox)
                        else:
                            print(f"Error.{j}: ", errorBox)
                            raise Exception(errorBox)
                else:
                    print(f"Bulk update completed successfully. Updated {success_count} documents.")
            except Exception as e:
                pass

        # 行政许可证同步
        resp_data = ddwCli.company_certificate_query(firm_uncid)
        if resp_data and 'ADMINISTRATIVE_LICENSE' in resp_data:
            license_data = resp_data['ADMINISTRATIVE_LICENSE']
            license_total = license_data.get('total', 0)
            print("\t\t", "行政许可: ", license_total)
            
            if license_total > 0:
                license_list = license_data.get('datalist', [])
                bulk_actions = []
                
                for license_item in license_list:
                    license_info = {
                        "fileName": license_item.get("FILENAME"),     # 许可文件名称
                        "fileNo": license_item.get("FILENO"),        # 文件编号
                        "validFrom": license_item.get("VALFROM"),    # 有效期自
                        "validTo": license_item.get("VALTO"),        # 有效期至
                        "licenseOffice": license_item.get("LICAUTH"), # 许可机关
                        "licenseContent": license_item.get("LICCONTENT"), # 许可内容
                        "entName": license_item.get("ENTNAME")       # 企业名称
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.administrativeLicenseData == null) {
                                ctx._source.administrativeLicenseData = [:];
                            }
                            if (ctx._source.administrativeLicenseData.dataList == null) {
                                ctx._source.administrativeLicenseData.dataList = [];
                            }
                            ctx._source.administrativeLicenseData.dataList.add(params.licenseInfo);
                            ctx._source.administrativeLicenseData.totalNum = ctx._source.administrativeLicenseData.dataList.length;
                            ''',
                            "params": {
                                "licenseInfo": license_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
                            status = updateBox['status']
                            errorBox = updateBox['error']
                            errType = errorBox['type']
                            if errType == 'document_missing_exception':
                                raise Exception(errorBox)
                            else:
                                print(f"Error.{j}: ", errorBox)
                                raise Exception(errorBox)
                    else:
                        print(f"Bulk update completed successfully. Updated {success_count} documents.")
                except Exception as e:
                    print(f"Exception while syncing licenses: {e}")

        # 荣誉资质同步
        resp_data = ddwCli.company_billboard_golory_query(firm_uncid)
        if resp_data and 'HONOR' in resp_data:
            honor_data = resp_data['HONOR']
            honor_total = honor_data.get('total', 0)
            print("\t\t", "荣誉资质: ", honor_total)
            
            if honor_total > 0:
                honor_list = honor_data.get('datalist', [])
                bulk_actions = []
                
                for honor_item in honor_list:
                    honor_info = {
                        "firmName": honor_item.get("ENTNAME"),      # 企业名称
                        "honorName": honor_item.get("golory_name"), # 荣誉名称
                        "issueDate": honor_item.get("pdate"),       # 发布日期
                        "validFrom": honor_item.get("datefrom"),    # 有效期起
                        "validTo": honor_item.get("dateto"),        # 有效期至
                        "honorLevel": honor_item.get("plevel"),     # 荣誉级别
                        "status": honor_item.get("status")          # 状态(1有效3已期未知)
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.honorQualificationData == null) {
                                ctx._source.honorQualificationData = [:];
                            }
                            if (ctx._source.honorQualificationData.dataList == null) {
                                ctx._source.honorQualificationData.dataList = [];
                            }
                            ctx._source.honorQualificationData.dataList.add(params.honorInfo);
                            ctx._source.honorQualificationData.totalNum = ctx._source.honorQualificationData.dataList.length;
                            ''',
                            "params": {
                                "honorInfo": honor_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
                            status = updateBox['status']
                            errorBox = updateBox['error']
                            errType = errorBox['type']
                            if errType == 'document_missing_exception':
                                raise Exception(errorBox)
                            else:
                                print(f"Error.{j}: ", errorBox)
                                raise Exception(errorBox)
                    else:
                        print(f"Bulk update completed successfully. Updated {success_count} documents.")
                except Exception as e:
                    print(f"Exception while syncing honors: {e}")

        # 新闻舆情同步
        resp_data = ddwCli.company_news_query(firm_uncid)
        if resp_data and 'NEWS' in resp_data:
            news_data = resp_data['NEWS']
            news_total = news_data.get('total', 0)
            print("\t\t", "新闻舆情: ", news_total)
            
            if news_total > 0:
                news_list = news_data.get('datalist', [])
                bulk_actions = []
                
                for news_item in news_list:
                    news_info = {
                        "author": news_item.get("author"),          # 作者/来源平台
                        "title": news_item.get("title"),           # 标题
                        "url": news_item.get("url"),               # 来源URL
                        "eventTime": news_item.get("event_time"),  # 事件时间
                        "category": news_item.get("category"),     # 新闻分类
                        "impact": news_item.get("impact"),         # 舆情倾向
                        "keywords": news_item.get("keywords"),     # 文章关键词
                        "content": news_item.get("content"),       # 新闻正文
                        "entName": news_item.get("ENTNAME")        # 主体名称
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.newsFeelingsData == null) {
                                ctx._source.newsFeelingsData = [:];
                            }
                            if (ctx._source.newsFeelingsData.dataList == null) {
                                ctx._source.newsFeelingsData.dataList = [];
                            }
                            ctx._source.newsFeelingsData.dataList.add(params.newsInfo);
                            ctx._source.newsFeelingsData.totalNum = ctx._source.newsFeelingsData.dataList.length;
                            ''',
                            "params": {
                                "newsInfo": news_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
                            status = updateBox['status']
                            errorBox = updateBox['error']
                            errType = errorBox['type']
                            if errType == 'document_missing_exception':
                                raise Exception(errorBox)
                            else:
                                print(f"Error.{j}: ", errorBox)
                                raise Exception(errorBox)
                    else:
                        print(f"Bulk update completed successfully. Updated {success_count} documents.")
                except Exception as e:
                    print(f"Exception while syncing news: {e}")
