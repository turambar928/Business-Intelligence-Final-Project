#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻实时分析系统主启动文件
整合Kafka消费、Spark Streaming处理、AI分析和MySQL存储
"""

import sys
import os
import logging
import signal
import time
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def setup_logging():
    """设置日志"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('logs/news_analytics.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def create_directories():
    """创建必要的目录"""
    directories = ['logs', 'checkpoints', 'data', 'models']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"创建目录: {directory}")

def main():
    """主函数"""
    print("="*60)
    print("新闻实时分析系统启动")
    print("="*60)
    
    # 创建目录
    create_directories()
    
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # 配置信息
    config = {
        'kafka': {
            'bootstrap_servers': ['localhost:9092'],
            'topic_name': 'news_impression_logs',
            'consumer_group': 'news_analytics_group'
        },
        'mysql': {
            'host': 'localhost',
            'port': 3306,
            'database': 'news_analytics',
            'username': 'root',
            'password': 'password',
            'charset': 'utf8mb4'
        },
        'spark': {
            'app_name': 'NewsRealTimeAnalytics',
            'batch_interval': 10,
            'checkpoint_dir': './checkpoints',
            'master': 'local[*]'
        }
    }
    
    logger.info("系统配置加载完成")
    
    try:
        #这里可以添加实际的分析器启动代码
        analyzer = NewsStreamingAnalyzer(config['kafka'], config['mysql'], config['spark'])
        analyzer.start_streaming()
        
        print("系统组件:")
        print("1. Kafka消费者 - 接收新闻曝光日志")
        print("2. Spark Streaming - 实时数据处理")
        print("3. AI分析引擎 - 智能分析和推荐")
        print("4. MySQL存储 - 分析结果持久化")
        print("\n系统正在运行中...")
        
        # 保持运行
        while True:
            time.sleep(10)
            logger.info(f"系统运行正常 - {datetime.now()}")
            
    except KeyboardInterrupt:
        logger.info("收到停止信号，正在关闭系统...")
        print("\n系统正在安全关闭...")
    except Exception as e:
        logger.error(f"系统运行错误: {str(e)}")
        print(f"系统错误: {str(e)}")
    finally:
        print("系统已停止")

if __name__ == "__main__":
    main() 