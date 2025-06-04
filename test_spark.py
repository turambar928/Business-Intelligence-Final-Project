#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark连接测试程序
"""

import os
import sys
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_spark_basic():
    """测试基础Spark功能"""
    try:
        from pyspark.sql import SparkSession
        
        print("正在初始化Spark...")
        
        spark = SparkSession.builder \
            .appName("SparkTest") \
            .master("local[*]") \
            .config("spark.ui.port", "4040") \
            .getOrCreate()
        
        print("✓ Spark初始化成功!")
        print(f"✓ Spark UI: http://localhost:4040")
        print(f"✓ Spark版本: {spark.version}")
        
        # 简单测试
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        
        print("✓ 测试DataFrame:")
        df.show()
        
        spark.stop()
        print("✓ Spark测试完成")
        return True
        
    except Exception as e:
        print(f"✗ Spark测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_kafka_connection():
    """测试Kafka连接"""
    try:
        from kafka import KafkaConsumer
        
        print("正在测试Kafka连接...")
        
        consumer = KafkaConsumer(
            'news_impression_logs',
            bootstrap_servers=['localhost:9092'],
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: m.decode('utf-8')
        )
        
        print("✓ Kafka连接成功")
        
        # 尝试获取一条消息
        message_count = 0
        for message in consumer:
            print(f"✓ 收到Kafka消息: {message.value[:100]}...")
            message_count += 1
            if message_count >= 3:  # 只获取前3条
                break
        
        consumer.close()
        
        if message_count > 0:
            print(f"✓ 成功接收到 {message_count} 条消息")
        else:
            print("⚠ 没有收到消息，请检查数据模拟器是否运行")
        
        return True
        
    except Exception as e:
        print(f"✗ Kafka连接失败: {e}")
        return False

def test_mysql_connection():
    """测试MySQL连接"""
    try:
        import pymysql
        
        print("正在测试MySQL连接...")
        
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='analytics_user',
            password='your_password',  # 请替换为实际密码
            database='news_analytics',
            charset='utf8mb4'
        )
        
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        
        print("✓ MySQL连接成功")
        print(f"✓ 找到 {len(tables)} 个表")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"✗ MySQL连接失败: {e}")
        return False

def main():
    """主测试函数"""
    print("="*60)
    print("系统组件测试")
    print("="*60)
    
    results = {
        'spark': test_spark_basic(),
        'kafka': test_kafka_connection(), 
        'mysql': test_mysql_connection()
    }
    
    print("\n" + "="*60)
    print("测试结果汇总:")
    print("="*60)
    
    for component, result in results.items():
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{component.upper()}: {status}")
    
    if all(results.values()):
        print("\n🎉 所有组件测试通过! 可以启动完整系统")
    else:
        print("\n⚠ 部分组件测试失败，请先修复问题")

if __name__ == "__main__":
    main()