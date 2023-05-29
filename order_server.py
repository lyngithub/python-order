import logging
import signal
import sys
import os
from concurrent import futures
from functools import partial

from loguru import logger
import argparse
import grpc
import socket
from rocketmq.client import PushConsumer

BASE_DIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, BASE_DIR)

from order_srv.proto import order_pb2, order_pb2_grpc

from order_srv.handler.order import OrderServicer, order_timeout
from order_srv.settings import settings
from common.grpc_health.v1 import health_pb2_grpc, health_pb2
from common.grpc_health.v1 import health
from common.register import consul


def on_exit(signo, frame, service_id):
    register = consul.ConsulRegister(settings.CONSUL_HOST, settings.CONSUL_PORT)
    logger.info(f"注销 {service_id} 服务")
    register.deregister(service_id=service_id)
    logger.info("注销成功")
    sys.exit(0)


def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(("", 0))
    _, port = tcp.getsockname()
    tcp.close()
    return port


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ip', nargs="?", type=str, default="0.0.0.0", help="biding ip")
    parser.add_argument('--port', nargs="?", type=int, default=0, help="listening port")
    args = parser.parse_args()

    if args.port == 0:
        port = get_free_tcp_port()
    else:
        port = args.port

    logger.add("logs/order_srv_{time}.log")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # 注册订单服务
    order_pb2_grpc.add_OrderServicer_to_server(OrderServicer(), server)

    # 注册健康检查
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)

    server.add_insecure_port(f'{args.ip}:{port}')

    import uuid
    service_id = str(uuid.uuid1())
    # 主进程退出信号监听
    """
        windows 支持的信号有限
        SIGINT ctrl + c终端
        SIGTERM kill
    """
    signal.signal(signal.SIGINT, partial(on_exit, service_id=service_id))
    signal.signal(signal.SIGTERM, partial(on_exit, service_id=service_id))

    print(f"启动服务:", {args.ip}, {port})

    server.start()
    logger.info(f"服务注册开始")
    register = consul.ConsulRegister(settings.CONSUL_HOST, settings.CONSUL_PORT)
    if not register.register(name=settings.SERVICE_NAME, id=service_id, address="110.40.153.208", port=port,
                             tags=settings.SERVICE_TAGS):
        logger.info(f"服务注册失败")
        sys.exit(0)

    logger.info(f"服务注册成功")

    # 监听超时订单消息
    consumer = PushConsumer("mxshop_order")
    consumer.set_name_server_address(f"{settings.ROCKETMQ_HOST}:{settings.ROCKETMQ_PORT}")
    consumer.subscribe("order_timeout", order_timeout)
    consumer.start()

    server.wait_for_termination()
    consumer.shutdown()


@logger.catch
def my_function(x, y, z):
    return 1 / (x + y + z)


if __name__ == '__main__':
    logging.basicConfig()
    settings.client.add_config_watcher(settings.NACOS["DataId"], settings.NACOS["Group"], settings.update_cfg)
    serve()
    # my_function(0, 0, 0)
