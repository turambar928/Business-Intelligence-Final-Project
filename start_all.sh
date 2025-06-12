#!/bin/bash
echo "=== 启动新闻实时分析系统 ==="
echo ""

# 设置环境变量
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/home/$USER/.local/lib/python3.10/site-packages/pyspark

# 检查MySQL
echo "检查MySQL状态..."
systemctl is-active mysql > /dev/null || (echo "❌ MySQL未运行，请先启动MySQL: sudo systemctl start mysql" && exit 1)
echo "✅ MySQL运行正常"
echo ""

# 检查和停止现有进程
echo "检查并清理现有进程..."
pkill -f "zookeeper-server-start.sh" 2>/dev/null
pkill -f "kafka-server-start.sh" 2>/dev/null
pkill -f "python3 main.py" 2>/dev/null
sleep 2
echo "✅ 进程清理完成"
echo ""

echo "1. 启动Zookeeper..."
cd /opt/kafka
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > /tmp/zookeeper.log 2>&1 &
sleep 8
echo "✅ Zookeeper启动完成"

echo "2. 启动Kafka..."
nohup bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &
sleep 15
echo "✅ Kafka启动完成"

echo "3. 创建/验证Topic..."
bin/kafka-topics.sh --create --topic news_impression_logs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic已存在"
echo "✅ Topic准备完成"

echo "4. 启动分析系统..."
cd ~/Desktop/Business-Intelligence-Final-Project
nohup python3 main.py > /tmp/main.log 2>&1 &
sleep 8
echo "✅ 主分析系统启动完成"


echo ""
echo "🎉 系统启动完成!"
echo ""
echo "📊 查看状态: ./check_system.sh"
echo "📋 查看日志: tail -f logs/news_analytics.log"
echo "💾 查看数据: mysql -u analytics_user -p'pass123' news_analytics"
echo "🔍 实时监控: watch -n 5 './check_system.sh'"
echo ""
echo "⚠️  系统需要约30秒完全启动，请耐心等待第一批数据处理" 