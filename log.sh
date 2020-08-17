#!/bin/bash

# 在102启动nginx
# 在102,103,104分别启动日志服务器

gmall_home=/opt/gmall-0317

case $1 in
"start")
    echo '在 hadoop102 启动nginx'
    sudo /usr/local/webserver/nginx/sbin/nginx

    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在 $host 启动日志服务器"
        ssh $host "source /etc/profile; nohup java -jar ${gmall_home}/gmall-logger-0.0.1-SNAPSHOT.jar 1>$gmall_home/a.log 2>&1  &"
    done
   ;;
"stop")
    echo '在 hadoop102 启动nginx'
    sudo /usr/local/webserver/nginx/sbin/nginx -s stop

    for host in hadoop102 hadoop103 hadoop104 ; do
        echo "在 $host 启动日志服务器"
        ssh $host "ps -ef | awk '/gmall/ && !/awk/ {print \$2}' | xargs kill -9"
    done
   ;;
 *)
    echo "你的启动姿势不对"
    echo "  start 启动采集"
    echo "  stop  停止采集"
 ;;
esac

