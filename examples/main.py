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
                            pass
                        else:
                            print(f"Error.{j}: ", errorBox)
                            raise Exception(err['error'])
                else:
                    print(f"Bulk update completed successfully. Updated {success_count} documents.")
            except Exception as e:
                pass

        pass
        # TODO: 行政许可
        resp = ddwCli.company_certificate_query(firm_uncid)
        pass

        # TODO: 荣誉资质
        # TODO: 严重违法
        # TODO: 严重违法
        # TODO: 新闻舆情
