#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻实时分析系统主启动文件 - 增强版
整合Kafka消费、Spark Streaming处理、AI分析和MySQL存储
使用增强的分析组件，支持新的日志格式和七大分析功能
"""

import sys
import os
import logging
import signal
import time
from datetime import datetime

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入增强的自定义模块
from src.enhanced_spark_streaming_analyzer import EnhancedNewsStreamingAnalyzer
from src.enhanced_ai_analysis_engine import EnhancedAIAnalysisEngine
from src.enhanced_data_generator import EnhancedDataGenerator

def setup_logging():
    """设置日志"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('logs/enhanced_news_analytics.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def create_directories():
    """创建必要的目录"""
    directories = ['logs', 'checkpoints', 'data', 'models', 'web_logs']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"创建目录: {directory}")

def main():
    """主函数"""
    print("="*60)
    print("新闻实时分析系统启动 - 增强版")
    print("="*60)
    
    # 创建目录
    create_directories()
    
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # 增强版配置信息
    config = {
        'kafka': {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'news_impression_logs',
            'consumer_group': 'enhanced_news_analytics_group'
        },
        'mysql': {
            'host': 'localhost',
            'port': 3306,
            'database': 'news_analytics',
            'user': 'analytics_user',
            'password': 'pass123',
            'charset': 'utf8mb4'
        },
        'spark': {
            'app_name': 'EnhancedNewsRealTimeAnalytics',
            'batch_interval': 10,
            'checkpoint_dir': './checkpoints',
            'master': 'local[*]'
        },
        'analysis': {
            'enable_ai_analysis': True,
            'enable_viral_prediction': True,
            'enable_real_time_recommendations': True,
            'enable_sentiment_analysis': True,
            'save_analysis_results': True
        }
    }
    
    logger.info("增强版系统配置加载完成")
    
    try:
        # 初始化增强的分析器
        logger.info("初始化增强的Spark流处理分析器...")
        analyzer = EnhancedNewsStreamingAnalyzer(config['kafka'], config['mysql'])
        
        # 初始化AI分析引擎（可选独立测试）
        logger.info("初始化增强的AI分析引擎...")
        ai_engine = EnhancedAIAnalysisEngine(config['mysql'])
        
        # 初始化数据生成器（可选，用于测试）
        logger.info("初始化增强的数据生成器...")
        data_generator = EnhancedDataGenerator("data/")
        
        print("\n🚀 增强版系统组件:")
        print("1. ✅ Kafka消费者 - 接收增强格式新闻日志")
        print("2. ✅ Spark Streaming - 实时数据处理和分析")
        print("3. ✅ AI分析引擎 - 七大智能分析功能")
        print("   - 新闻生命周期分析")
        print("   - 分类趋势分析")
        print("   - 用户兴趣跟踪")
        print("   - 爆款新闻预测")
        print("   - 实时推荐系统")
        print("   - 情感分析")
        print("   - 多维度查询统计")
        print("4. ✅ MySQL存储 - 增强的分析结果持久化")
        print("5. ✅ 数据生成器 - 支持新日志格式的测试数据")
        
        print(f"\n📊 日志格式: 增强版JSON格式 (符合docs/new_log_format.md规范)")
        print(f"📁 日志目录: web_logs/")
        print(f"🔄 处理间隔: {config['spark']['batch_interval']}秒")
        print(f"🎯 Kafka主题: {config['kafka']['topic']}")
        
        # 启动流处理分析
        logger.info("启动增强版流处理分析...")
        print("\n🎯 系统正在运行中...")
        print("   - 实时接收Kafka日志流")
        print("   - 执行七大AI分析功能")
        print("   - 存储分析结果到MySQL")
        print("   - 按Ctrl+C安全停止")
        
        analyzer.start_streaming()
        
        # 保持运行
        while True:
            time.sleep(30)
            logger.info(f"增强版系统运行正常 - {datetime.now()}")
            
    except KeyboardInterrupt:
        logger.info("收到停止信号，正在关闭增强版系统...")
        print("\n🛑 系统正在安全关闭...")
    except Exception as e:
        logger.error(f"增强版系统运行错误: {str(e)}")
        print(f"❌ 系统错误: {str(e)}")
        # 打印详细错误信息用于调试
        import traceback
        traceback.print_exc()
    finally:
        print("✅ 增强版系统已停止")

if __name__ == "__main__":
    main()
