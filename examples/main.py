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
        # 股权质押
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

        # 招投标信息同步
        resp_data = ddwCli.company_bid_list_query(firm_uncid)
        if resp_data and 'BIDLIST' in resp_data:
            bid_data = resp_data['BIDLIST']
            bid_total = bid_data.get('total', 0)
            print("\t\t", "招投标信息: ", bid_total)
            
            if bid_total > 0:
                bid_list = bid_data.get('datalist', [])
                bulk_actions = []
                
                for bid_item in bid_list:
                    # 处理中标方列表
                    bid_win_list = bid_item.get("bidwinList", []) or bid_item.get("bidWinList", [])
                    bid_winners = [{
                        "entId": winner.get("entid"),
                        "entName": winner.get("ENTNAME")
                    } for winner in bid_win_list] if bid_win_list else []
                    
                    # 处理代理方列表
                    agent_list = bid_item.get("agentList", [])
                    agents = [{
                        "entId": agent.get("entid"),
                        "entName": agent.get("ENTNAME")
                    } for agent in agent_list] if agent_list else []
                    
                    bid_info = {
                        "title": bid_item.get("title"),               # 公告标题
                        "noticeType": bid_item.get("noticetype"),     # 公告类型(01招标公告、02中标公告等)
                        "regionName": bid_item.get("region_name"),    # 地区名称
                        "bType": bid_item.get("btype"),               # 角色(95项目、01供应方等)
                        "bidWinners": bid_winners,                    # 中标方列表
                        "agents": agents,                             # 代理方列表
                        # 新增字段
                        "publishDate": bid_item.get("pubdate"),       # 发布日期(新增)
                        "projectAmount": bid_item.get("proj_amount"), # 项目金额(新增)
                        "projectType": bid_item.get("proj_type"),     # 项目类型(新增)
                        "sourceUrl": bid_item.get("source_url"),      # 来源链接(新增)
                        "content": bid_item.get("content"),           # 公告内容(新增)
                        "bidCategory": bid_item.get("bid_category")   # 招标类别(新增)
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.bidData == null) {
                                ctx._source.bidData = [:];
                            }
                            if (ctx._source.bidData.dataList == null) {
                                ctx._source.bidData.dataList = [];
                            }
                            ctx._source.bidData.dataList.add(params.bidInfo);
                            ctx._source.bidData.totalNum = ctx._source.bidData.dataList.length;
                            ''',
                            "params": {
                                "bidInfo": bid_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
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
                    print(f"Exception while syncing bids: {e}")

        # 上榜榜单信息同步
        resp_data = ddwCli.company_fc_thirdtop_query(firm_uncid)
        if resp_data and 'THIRDTOP' in resp_data:
            ranking_data = resp_data['THIRDTOP']
            ranking_total = ranking_data.get('total', 0)
            print("\t\t", "上榜榜单信息: ", ranking_total)
            
            if ranking_total > 0:
                ranking_list = ranking_data.get('datalist', [])
                bulk_actions = []
                
                for ranking_item in ranking_list:
                    ranking_info = {
                        "listName": ranking_item.get("bangdan_name"),    # 榜单名称
                        "listType": ranking_item.get("bangdan_type"),    # 榜单类型
                        "url": ranking_item.get("url"),                  # 来源url
                        "entName": ranking_item.get("ENTNAME"),          # 企业名称
                        "ranking": ranking_item.get("ranking"),          # 排名
                        "publishDate": ranking_item.get("pdate")         # 发布日期
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.rankingListData == null) {
                                ctx._source.rankingListData = [:];
                            }
                            if (ctx._source.rankingListData.dataList == null) {
                                ctx._source.rankingListData.dataList = [];
                            }
                            ctx._source.rankingListData.dataList.add(params.rankingInfo);
                            ctx._source.rankingListData.totalNum = ctx._source.rankingListData.dataList.length;
                            ''',
                            "params": {
                                "rankingInfo": ranking_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
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
                    print(f"Exception while syncing rankings: {e}")

        # 企业科技成果信息同步
        resp_data = ddwCli.company_most_scitech_query(firm_uncid)
        if resp_data and 'SCITECH' in resp_data:
            scitech_data = resp_data['SCITECH']
            scitech_total = scitech_data.get('total', 0)
            print("\t\t", "科技成果信息: ", scitech_total)
            
            if scitech_total > 0:
                scitech_list = scitech_data.get('datalist', [])
                bulk_actions = []
                
                for scitech_item in scitech_list:
                    scitech_info = {
                        "queryEntName": scitech_item.get("QRYENTNAME"),  # 企业名称
                        "registrationNo": scitech_item.get("desno"),     # 登记号
                        "entName": scitech_item.get("ENTNAME"),          # 第一完成单位
                        "achievementName": scitech_item.get("pname"),    # 成果名称
                        "completors": scitech_item.get("names"),         # 成果完成人
                        "year": scitech_item.get("year"),               # 年份
                        # 新增字段
                        "achievementType": scitech_item.get("type"),    # 成果类型(新增)
                        "registrationDate": scitech_item.get("regDate") # 登记日期(新增)
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.scientificTechnologicalAchievementData == null) {
                                ctx._source.scientificTechnologicalAchievementData = [:];
                            }
                            if (ctx._source.scientificTechnologicalAchievementData.dataList == null) {
                                ctx._source.scientificTechnologicalAchievementData.dataList = [];
                            }
                            ctx._source.scientificTechnologicalAchievementData.dataList.add(params.scitechInfo);
                            ctx._source.scientificTechnologicalAchievementData.totalNum = ctx._source.scientificTechnologicalAchievementData.dataList.length;
                            ''',
                            "params": {
                                "scitechInfo": scitech_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
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
                    print(f"Exception while syncing scitech: {e}")

        # 企业融资信息同步
        resp_data = ddwCli.company_vc_inv_query(firm_uncid)
        if resp_data and 'INVESTMENT' in resp_data:
            investment_data = resp_data['INVESTMENT']
            investment_total = investment_data.get('total', 0)
            print("\t\t", "融资信息: ", investment_total)
            
            if investment_total > 0:
                investment_list = investment_data.get('datalist', [])
                bulk_actions = []
                
                for investment_item in investment_list:
                    investment_info = {
                        "entName": investment_item.get("ENTNAME"),                    # 融资公司全称
                        "investDate": investment_item.get("investdate"),              # 投资日期
                        "investAmount": investment_item.get("invse_similar_money_name"), # 投资的近似金额名称
                        "investDetailAmount": investment_item.get("invse_detail_money"),  # 投资的详细金额
                        "valuationDetail": investment_item.get("invse_guess_particulars"), # 估值明细
                        "investRound": investment_item.get("invse_round_name"),        # 投资的轮次名称
                        "orgName": investment_item.get("org_name"),                    # 机构名称
                        # 新增字段
                        "investType": investment_item.get("invest_type"),             # 投资类型(新增)
                        "currency": investment_item.get("currency"),                   # 币种(新增)
                        "investorType": investment_item.get("investor_type")          # 投资方类型(新增)
                    }
                    
                    action = {
                        "_op_type": "update",
                        "_index": INDEX,
                        "_id": firm_uncid,
                        "script": {
                            "source": '''
                            if (ctx._source.investmentData == null) {
                                ctx._source.investmentData = [:];
                            }
                            if (ctx._source.investmentData.dataList == null) {
                                ctx._source.investmentData.dataList = [];
                            }
                            ctx._source.investmentData.dataList.add(params.investmentInfo);
                            ctx._source.investmentData.totalNum = ctx._source.investmentData.dataList.length;
                            ''',
                            "params": {
                                "investmentInfo": investment_info
                            }
                        }
                    }
                    bulk_actions.append(action)
                
                try:
                    success_count, errors = helpers.bulk(esCli, bulk_actions, raise_on_error=False)
                    if errors:
                        for j, err in enumerate(errors):
                            updateBox = err['update']
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
                    print(f"Exception while syncing investments: {e}")
