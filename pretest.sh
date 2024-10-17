#!/bin/bash

# 目标端口
PORT=9801

echo "查找占用端口 $PORT 的进程..."

# 使用 ss 命令查找占用指定端口的进程
PROCESS_INFO=$(ss -tulnp | grep ":$PORT ")

if [ -z "$PROCESS_INFO" ]; then
    echo "没有进程占用端口 $PORT。"
else
    echo "找到占用端口 $PORT 的以下进程："
    echo "$PROCESS_INFO"

    # 提取 PID（假设只处理第一个匹配的进程）
    PID=$(echo "$PROCESS_INFO" | awk '{print $NF}' | awk -F',' '{for (i=1;i<=NF;i++) if ($i ~ /^pid=/){split($i, a, "="); print a[2]}}')

    if [ -z "$PID" ]; then
        echo "未能提取到 PID。请检查是否有足够的权限或进程信息格式是否正确。"
        exit 1
    fi

    echo "将终止 PID 为 $PID 的进程。"

    # 终止进程
    kill -9 "$PID"

    if [ $? -eq 0 ]; then
        echo "成功终止 PID 为 $PID 的进程。"
    else
        echo "终止 PID 为 $PID 的进程失败。"
        exit 1
    fi
fi

echo "执行命令：sudo ./regtest.sh stop"

# 执行停止脚本
sudo ./regtest.sh stop

if [ $? -eq 0 ]; then
    echo "成功执行 ./regtest.sh stop。"
else
    echo "执行 ./regtest.sh stop 失败。"
    exit 1
fi

exit 0
