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


def sync_dimension_data(es_client, firm_uncid, api_method, dimension_config):
    """
    通用维度数据同步方法
    :param es_client: ES客户端
    :param firm_uncid: 企业ID
    :param api_method: API调用方法
    :param dimension_config: 维度配置信息，包含：
        - response_key: API响应中的数据键
        - es_field: ES中的字段名
        - field_mapping: 字段映射关系
        - special_handlers: 特殊字段处理器(可选)
    """
    resp_data = api_method(firm_uncid)
    if not resp_data or dimension_config['response_key'] not in resp_data:
        return
        
    data = resp_data[dimension_config['response_key']]
    total = data.get('total', 0)
    print(f"\t\t {dimension_config['display_name']}: {total}")
    
    if total <= 0:
        return
        
    data_list = data.get('datalist', [])
    bulk_actions = []
    
    for item in data_list:
        # 处理基础字段映射
        info = {
            es_key: item.get(api_key)
            for es_key, api_key in dimension_config['field_mapping'].items()
        }
        
        # 处理特殊字段
        if 'special_handlers' in dimension_config:
            for handler in dimension_config['special_handlers']:
                handler(info, item)
        
        action = {
            "_op_type": "update",
            "_index": INDEX,
            "_id": firm_uncid,
            "script": {
                "source": f'''
                if (ctx._source.{dimension_config['es_field']} == null) {{
                    ctx._source.{dimension_config['es_field']} = [:];
                }}
                if (ctx._source.{dimension_config['es_field']}.dataList == null) {{
                    ctx._source.{dimension_config['es_field']}.dataList = [];
                }}
                ctx._source.{dimension_config['es_field']}.dataList.add(params.info);
                ctx._source.{dimension_config['es_field']}.totalNum = ctx._source.{dimension_config['es_field']}.dataList.length;
                ''',
                "params": {
                    "info": info
                }
            }
        }
        bulk_actions.append(action)
    
    try:
        success_count, errors = helpers.bulk(es_client, bulk_actions, raise_on_error=False)
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
        print(f"Exception while syncing {dimension_config['display_name']}: {e}")

# 定义维度配置
DIMENSION_CONFIGS = {
    'administrative_license': {
        'response_key': 'ADMINISTRATIVE_LICENSE',
        'es_field': 'administrativeLicenseData',
        'display_name': '行政许可',
        'field_mapping': {
            'fileName': 'FILENAME',
            'fileNo': 'FILENO',
            'validFrom': 'VALFROM',
            'validTo': 'VALTO',
            'licenseOffice': 'LICAUTH',
            'licenseContent': 'LICCONTENT',
            'entName': 'ENTNAME'
        }
    },
    'honor': {
        'response_key': 'HONOR',
        'es_field': 'honorQualificationData',
        'display_name': '荣誉资质',
        'field_mapping': {
            'firmName': 'ENTNAME',
            'honorName': 'golory_name',
            'issueDate': 'pdate',
            'validFrom': 'datefrom',
            'validTo': 'dateto',
            'honorLevel': 'plevel',
            'status': 'status'
        }
    },
    'bid': {
        'response_key': 'BIDLIST',
        'es_field': 'bidData',
        'display_name': '招投标信息',
        'field_mapping': {
            'title': 'title',
            'noticeType': 'noticetype',
            'regionName': 'region_name',
            'bType': 'btype',
            'publishDate': 'pubdate',
            'projectAmount': 'proj_amount',
            'projectType': 'proj_type',
            'sourceUrl': 'source_url',
            'content': 'content',
            'bidCategory': 'bid_category'
        },
        'special_handlers': [
            # 处理中标方列表
            lambda info, item: info.update({
                'bidWinners': [{
                    'entId': winner.get('entid'),
                    'entName': winner.get('ENTNAME')
                } for winner in (item.get('bidwinList', []) or item.get('bidWinList', []))] if (item.get('bidwinList') or item.get('bidWinList')) else []
            }),
            # 处理代理方列表
            lambda info, item: info.update({
                'agents': [{
                    'entId': agent.get('entid'),
                    'entName': agent.get('ENTNAME')
                } for agent in item.get('agentList', [])] if item.get('agentList') else []
            })
        ]
    },
    'share_pledge': {
        'response_key': 'IMPAWN',
        'es_field': 'sharePledgeData',
        'display_name': '股权质押',
        'field_mapping': {
            'pledgor': 'pledgor',           # 出质人
            'relatedCompany': 'RelatedCompany', # 出质股权标的企业
            'pledgee': 'IMPORG',            # 质权人
            'amount': 'IMPAM',              # 质押金额
            'execState': 'EXESTATE',        # 执行状态
            'recDate': 'IMPONRECDATE'       # 质押备案日期
        }
    },
    'telecom_license': {
        'response_key': 'TELECOM_LICENSE',
        'es_field': 'telecomLicenseData',
        'display_name': '电信许可证',
        'field_mapping': {
            'entName': 'ENTNAME',           # 企业名称
            'licenseScope': 'LICSCOPE',     # 许可范围
            'licenseName': 'LICNAME',       # 许可文件名称
            'licenseNo': 'LICNO',           # ���可文件编号
            'validFrom': 'VALFROM',         # 有效期自
            'validTo': 'VALTO'              # 有效期至
        }
    },
    'land_transfer': {
        'response_key': 'LAND_TRANSFER',
        'es_field': 'landTransferData',
        'display_name': '土地转让信息',
        'field_mapping': {
            'entName': 'ENTNAME',           # 企业名称
            'address': 'address',           # 宗地地址
            'city': 'city',                 # 行政区
            'originalOwner': 'ENTNAME_A',   # 原土地使用权人
            'currentOwner': 'ENTNAME_B',    # 现土地使用权人
            'transDate': 'trans_date'       # 成交时间
        }
    },
    'job_info': {
        'response_key': 'JOB_INFO',
        'es_field': 'jobInfoData',
        'display_name': '招聘信息',
        'field_mapping': {
            'entName': 'ENTNAME',           # 公司名称
            'title': 'title',               # 招聘标题
            'publishDate': 'pdate',         # 发布日期
            'salary': 'salary',             # 薪资
            'province': 'province',         # 工作省份
            'city': 'city',                 # 工作城市
            'experience': 'experience',      # 工作年限
            'education': 'education'         # 学历
        }
    },
    'tax_rating': {
        'response_key': 'TAX_RATING',
        'es_field': 'taxRatingData',
        'display_name': '纳税信用等级',
        'field_mapping': {
            'taxId': 'TAXID',               # 纳税人识别号
            'entName': 'ENTNAME',           # 企业名称
            'year': 'tyear',                # 评定年份
            'rating': 'rating'              # 评级
        }
    },
    'certification': {
        'response_key': 'CNCA5',
        'es_field': 'certificationAccreditationData',
        'display_name': '认证认可信息',
        'field_mapping': {
            'certProject': 'cert_project',   # 认证项目
            'certType': 'cert_type',         # 证书类型
            'awardDate': 'award_date',       # 颁证日期
            'expireDate': 'expire_date',     # 证书到期日期
            'certNum': 'cert_num',           # 证书编号
            'orgNum': 'org_num',             # 机构批准号
            'orgName': 'org_name',           # 机构名称
            'certStatus': 'cert_status'      # 证书状态
        }
    },
    'news': {
        'response_key': 'NEWS',
        'es_field': 'newsFeelingsData',
        'display_name': '新闻舆情',
        'field_mapping': {
            'author': 'author',              # 作者/来源平台
            'title': 'title',                # 标题
            'url': 'url',                    # 来源URL
            'eventTime': 'event_time',       # 事件时间
            'category': 'category',          # 新闻分类
            'impact': 'impact',              # 舆情倾向
            'keywords': 'keywords',          # 文章关键词
            'content': 'content',            # 新闻正文
            'entName': 'ENTNAME'             # 主体名称
        }
    },
    'scitech': {
        'response_key': 'SCITECH',
        'es_field': 'scientificTechnologicalAchievementData',
        'display_name': '科技成果信息',
        'field_mapping': {
            'queryEntName': 'QRYENTNAME',     # 企业名称
            'registrationNo': 'desno',        # 登记号
            'entName': 'ENTNAME',             # 第一完成单位
            'achievementName': 'pname',       # 成果名称
            'completors': 'names',            # 成果完成人
            'year': 'year',                   # 年份
            'achievementType': 'type',        # 成果类型
            'registrationDate': 'regDate'     # 登记日期
        }
    },
    'investment': {
        'response_key': 'INVESTMENT',
        'es_field': 'investmentData',
        'display_name': '融资信息',
        'field_mapping': {
            'entName': 'ENTNAME',                      # 融资公司全称
            'investDate': 'investdate',                # 投资日期
            'investAmount': 'invse_similar_money_name', # 投资的近似金额名称
            'investDetailAmount': 'invse_detail_money', # 投资的详细金额
            'valuationDetail': 'invse_guess_particulars', # 估值明细
            'investRound': 'invse_round_name',         # 投资的轮次名称
            'orgName': 'org_name',                     # 机构名称
            'investType': 'invest_type',               # 投资类型
            'currency': 'currency',                    # 币种
            'investorType': 'investor_type'            # 投资方类型
        }
    },
    'ranking': {
        'response_key': 'THIRDTOP',
        'es_field': 'rankingListData',
        'display_name': '上榜榜单信息',
        'field_mapping': {
            'listName': 'bangdan_name',      # 榜单名称
            'listType': 'bangdan_type',      # 榜单类型
            'url': 'url',                    # 来源url
            'entName': 'ENTNAME',            # 企业名称
            'ranking': 'ranking',            # 排名
            'publishDate': 'pdate'           # 发布日期
        }
    }
}

if __name__ == '__main__':
    ddwCli = EasyChainCli(debug=True)
    firm_list = fetch_firm_list()
    total = len(firm_list)
    
    # 定义API方法映射
    API_METHODS = {
        'administrative_license': ddwCli.company_certificate_query,
        'honor': ddwCli.company_billboard_golory_query,
        'bid': ddwCli.company_bid_list_query,
        'share_pledge': ddwCli.company_impawn_query,
        'telecom_license': ddwCli.company_aggre_cert_query,
        'land_transfer': ddwCli.company_mirland_transfer_query,
        'job_info': ddwCli.company_job_info_query,
        'tax_rating': ddwCli.company_tax_rating_query,
        'certification': ddwCli.company_cnca5_query,
        'news': ddwCli.company_news_query,
        'scitech': ddwCli.company_most_scitech_query,
        'investment': ddwCli.company_vc_inv_query,
        'ranking': ddwCli.company_fc_thirdtop_query
    }

    for i, firm in enumerate(firm_list, 1):
        progress = i / total * 100
        print(f"{progress:.2f}% ({i}/{total})", firm, end=':\n')
        db_id, chain_id, chain_name, chain_node_id, chain_node_name, firm_uncid, is_local_fir, has_over = firm

        # 遍历所有维度进行同步
        for dimension, config in DIMENSION_CONFIGS.items():
            if dimension not in ["share_pledge"]:
                sync_dimension_data(esCli, firm_uncid, API_METHODS[dimension], config)
