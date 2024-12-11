import os
import mysql.connector
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIR_NAME = BASE_DIR.split(os.path.sep)[-1]
USER_HOME_DIR = os.path.expanduser("~")
USER_CONFIG_DIR = os.path.join(USER_HOME_DIR, ".config")
PROJ_CONFIG_DIR = os.path.join(USER_CONFIG_DIR, DIR_NAME)
print("PROJ_CONFIG_DIR:", PROJ_CONFIG_DIR)
if not os.path.exists(PROJ_CONFIG_DIR):
    os.makedirs(PROJ_CONFIG_DIR)
ENV_FILE_PATH = os.path.join(PROJ_CONFIG_DIR, ".env")
print("ENV_FILE_PATH:", ENV_FILE_PATH)
load_dotenv(ENV_FILE_PATH)

es_slrc = Elasticsearch(hosts=os.getenv('SLRC_ES_PROTOCOL') + "://" + os.getenv('SLRC_ES_HOST'),
                        basic_auth=(os.getenv("SLRC_ES_USERNAME"), os.getenv("SLRC_ES_PASSWORD")),
                        ca_certs=os.getenv("SLRC_ES_CA"), request_timeout=3600)


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
    firm_list = fetch_firm_list()
    for i, firm in enumerate(firm_list, 1):
        print(i, firm)
        db_id, chain_id, chain_name, chain_node_id, chain_node_name, firm_uncid, is_local_fir, has_over = firm
        resp = es_slrc.search(index="hzxy_nation_global_enterprise", query={
            "term": {
                "firmUncid": {
                    "value": firm_uncid
                }
            }

        })
        took = resp['took']
        hits = resp["hits"]["hits"]
        total = len(hits)
        print("\t\t", took, total)
        for hit in hits:
            _source = hit["_source"]
            _id = hit["_id"]
            print("\t\t" * 2, _id)
        # TODO: 从五度易链API开放平台的接口中调取多维数据
        # TODO: 实现接口缓存
        # TODO: 行政许可
        # TODO: 荣誉资质
        # TODO: 严重违法
        # TODO: 严重违法
        # TODO: 新闻舆情

