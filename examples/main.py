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
    # ... 其他维度的配置
}

if __name__ == '__main__':
    ddwCli = EasyChainCli(debug=True)
    firm_list = fetch_firm_list()
    total = len(firm_list)
    for i, firm in enumerate(firm_list, 1):
        progress = i / total * 100
        print(f"{progress:.2f}% ({i}/{total})", firm, end=':\n')
        db_id, chain_id, chain_name, chain_node_id, chain_node_name, firm_uncid, is_local_fir, has_over = firm

        # 使用通用方法同步各个维度的数据
        sync_dimension_data(esCli, firm_uncid, ddwCli.company_certificate_query, DIMENSION_CONFIGS['administrative_license'])
        sync_dimension_data(esCli, firm_uncid, ddwCli.company_billboard_golory_query, DIMENSION_CONFIGS['honor'])
        sync_dimension_data(esCli, firm_uncid, ddwCli.company_bid_list_query, DIMENSION_CONFIGS['bid'])
        # ... 其他维度的同步调用
