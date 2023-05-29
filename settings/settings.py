import json, redis

from playhouse.pool import PooledMySQLDatabase
from playhouse.shortcuts import ReconnectMixin
import nacos
from loguru import logger


class ReconnectMysqlDatabase(ReconnectMixin, PooledMySQLDatabase):
    pass


# MYSQL_DB = "mxshop_user_srv"
# MYSQL_HOST = "110.40.153.208"
# MYSQL_PORT = 3306
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "123456"
# DB = ReconnectMysqlDatabase(MYSQL_DB, host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD)

# consul的配置
# CONSUL_HOST = "192.168.50.236"
# CONSUL_PORT = "8500"

# 服务相关的配置
# SERVICE_NAME = "user_srv"
# SERVICE_TAGS = ["user_srv", "user_info"]


NACOS = {
    "Host": "110.40.153.208",
    "Port": 8848,
    "NameSpace": "2364d554-1737-4fd8-8b6e-a67c49277430",
    "User": "nacos",
    "Password": "nacos",
    "DataId": "order_srv.json",
    "Group": "dev"
}

client = nacos.NacosClient(f'{NACOS["Host"]}:{NACOS["Port"]}', namespace=NACOS["NameSpace"],
                           username=NACOS["User"],
                           password=NACOS["Password"])

# get config
data = client.get_config(NACOS["DataId"], NACOS["Group"])
data = json.loads(data)


def update_cfg(args):
    print("配置产生变化")
    print(args)


# 服务相关的配置
SERVICE_NAME = data["name"]
SERVICE_TAGS = data["tags"]

# consul的配置
CONSUL_HOST = data["consul"]["host"]
CONSUL_PORT = data["consul"]["port"]

ROCKETMQ_HOST = data["rocketmq"]["host"]
ROCKETMQ_PORT = data["rocketmq"]["port"]
# redis
# REDIS_HOST = data["redis"]["host"]
# REDIS_PORT = data["redis"]["port"]
# REDIS_DB = data["redis"]["db"]


GOODS_SRV_NAME = data["goods_srv"]["name"]
INVENTORY_SRV_NAME = data["inventory_srv"]["name"]

# pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# REDIS_CLIENT = redis.StrictRedis(connection_pool=pool)

DB = ReconnectMysqlDatabase(data["mysql"]["db"], host=data["mysql"]["host"], port=data["mysql"]["port"],
                            user=data["mysql"]["user"], password=data["mysql"]["password"])
