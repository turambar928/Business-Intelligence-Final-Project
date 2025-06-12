#!/bin/bash

# Kafka启动脚本
# 用于启动Zookeeper和Kafka服务

echo "🚀 启动Kafka服务..."

# 设置Kafka路径（根据您的安装位置调整）
KAFKA_HOME="/opt/kafka"
if [ ! -d "$KAFKA_HOME" ]; then
    # 尝试其他常见路径
    if [ -d "/usr/local/kafka" ]; then
        KAFKA_HOME="/usr/local/kafka"
    elif [ -d "$HOME/kafka" ]; then
        KAFKA_HOME="$HOME/kafka"
    else
        echo "❌ 未找到Kafka安装目录，请设置KAFKA_HOME环境变量"
        exit 1
    fi
fi

echo "📁 Kafka目录: $KAFKA_HOME"

# 函数：检查端口是否被占用
check_port() {
    local port=$1
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        return 0  # 端口被占用
    else
        return 1  # 端口空闲
    fi
}

# 函数：停止服务
stop_services() {
    echo "🛑 停止现有服务..."
    
    # 停止Kafka
    if check_port 9092; then
        echo "  停止Kafka..."
        pkill -f kafka.Kafka 2>/dev/null || true
        sleep 2
    fi
    
    # 停止Zookeeper
    if check_port 2181; then
        echo "  停止Zookeeper..."
        pkill -f zookeeper 2>/dev/null || true
        sleep 2
    fi
}

# 函数：清理临时文件
cleanup_temp() {
    echo "🧹 清理临时文件..."
    rm -rf /tmp/kafka-logs* 2>/dev/null || true
    rm -rf /tmp/zookeeper* 2>/dev/null || true
}

# 函数：启动Zookeeper
start_zookeeper() {
    echo "📡 启动Zookeeper..."
    
    if check_port 2181; then
        echo "  ✅ Zookeeper已在运行 (端口2181)"
        return 0
    fi
    
    # 启动Zookeeper
    nohup $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties > logs/zookeeper.log 2>&1 &
    
    # 等待Zookeeper启动
    echo "  ⏳ 等待Zookeeper启动..."
    for i in {1..30}; do
        if check_port 2181; then
            echo "  ✅ Zookeeper启动成功"
            return 0
        fi
        sleep 1
    done
    
    echo "  ❌ Zookeeper启动失败"
    return 1
}

# 函数：启动Kafka
start_kafka() {
    echo "📨 启动Kafka..."
    
    if check_port 9092; then
        echo "  ✅ Kafka已在运行 (端口9092)"
        return 0
    fi
    
    # 启动Kafka
    nohup $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties > logs/kafka.log 2>&1 &
    
    # 等待Kafka启动
    echo "  ⏳ 等待Kafka启动..."
    for i in {1..30}; do
        if check_port 9092; then
            echo "  ✅ Kafka启动成功"
            return 0
        fi
        sleep 1
    done
    
    echo "  ❌ Kafka启动失败"
    return 1
}

# 函数：创建Topic
create_topic() {
    echo "📝 创建新闻事件Topic..."
    
    # 检查Topic是否已存在
    if $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic news-events >/dev/null 2>&1; then
        echo "  ✅ Topic 'news-events' 已存在"
    else
        # 创建Topic
        $KAFKA_HOME/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 \
            --create \
            --topic news-events \
            --partitions 3 \
            --replication-factor 1
        
        if [ $? -eq 0 ]; then
            echo "  ✅ Topic 'news-events' 创建成功"
        else
            echo "  ❌ Topic创建失败"
            return 1
        fi
    fi
}

# 函数：验证安装
verify_kafka() {
    echo "🔍 验证Kafka安装..."
    
    # 列出所有Topic
    echo "📋 当前Topic列表:"
    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
    
    echo "✅ Kafka验证完成"
}

# 主函数
main() {
    # 创建日志目录
    mkdir -p logs
    
    echo "🎯 增强新闻分析系统 - Kafka启动"
    echo "=================================="
    
    # 停止现有服务
    stop_services
    
    # 清理临时文件
    cleanup_temp
    
    # 启动Zookeeper
    if ! start_zookeeper; then
        echo "❌ Zookeeper启动失败，退出"
        exit 1
    fi
    
    # 启动Kafka
    if ! start_kafka; then
        echo "❌ Kafka启动失败，退出"
        exit 1
    fi
    
    # 创建Topic
    if ! create_topic; then
        echo "❌ Topic创建失败，退出"
        exit 1
    fi
    
    # 验证安装
    verify_kafka
    
    echo ""
    echo "🎉 Kafka启动完成!"
    echo "📊 服务状态:"
    echo "  - Zookeeper: localhost:2181"
    echo "  - Kafka: localhost:9092"
    echo "  - Topic: news-events"
    echo ""
    echo "💡 接下来可以:"
    echo "  1. 初始化数据库: python setup/init_enhanced_database.py"
    echo "  2. 启动系统: python scripts/run_enhanced_system.py"
    echo ""
    echo "📝 查看日志:"
    echo "  - Zookeeper: tail -f logs/zookeeper.log"
    echo "  - Kafka: tail -f logs/kafka.log"
}

# 执行主函数
main 