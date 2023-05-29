#!/bin/sh
srv_name="order_srv"
order_server="order_server"
chmod +x /code/mxshop_srvs/order_srv/order_server.py
PIDS=`ps -ef | grep ${order_server} | grep -v grep | awk '{print $2}'`
if [ "$PIDS" != "" ];
then
  echo "${srv_name} is running"
  echo "shutting down ${srv_name}"
  if ps -ef | grep ${order_server} | awk '{print $2}' | xargs kill $1
    then
      echo "starting ${srv_name}"
      cd /code/mxshop_srvs/order_srv
      /root/.virtualenvs/mxshop_srvs/bin/python order_server.py --ip=0.0.0.0 --port=56003 > /dev/null 2>&1 &
      echo "start ${srv_name} success"
  fi
else
  echo "starting ${srv_name}"
    cd /code/mxshop_srvs/order_srv
    /root/.virtualenvs/mxshop_srvs/bin/python order_server.py --ip=0.0.0.0 --port=56003 > /dev/null 2>&1 &
  echo "start ${srv_name} success xxx"

fi