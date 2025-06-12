# -*- coding: utf-8 -*-
"""
新闻实时分析系统配置文件
"""

import os

# Kafka 配置
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic_name': 'news_impression_logs',
    'consumer_group': 'news_analytics_group',
    'auto_offset_reset': 'latest'
}

# MySQL 数据库配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'database': 'news_analytics',
    'username': 'root',
    'password': 'password',
    'charset': 'utf8mb4'
}

# Spark 配置
SPARK_CONFIG = {
    'app_name': 'NewsRealTimeAnalytics',
    'batch_interval': 10,  # 批处理间隔（秒）
    'checkpoint_dir': './checkpoints',
    'master': 'local[*]'
}

# AI分析配置
AI_CONFIG = {
    'sentiment_model': 'textblob',  # 情感分析模型
    'topic_model': 'lda',  # 主题模型
    'recommendation_algo': 'collaborative_filtering',  # 推荐算法
    'trending_threshold': 100,  # 热门新闻阈值
    'time_window': 3600  # 时间窗口（秒）
}

# 日志配置
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_file': './logs/analytics.log'
}

# 新闻类别映射
NEWS_CATEGORIES = {
    'sports': '体育',
    'entertainment': '娱乐',
    'autos': '汽车',
    'tv': '电视',
    'news': '新闻',
    'finance': '财经',
    'lifestyle': '生活',
    'movies': '电影',
    'weather': '天气',
    'travel': '旅游',
    'health': '健康',
    'foodanddrink': '美食',
    'music': '音乐',
    'kids': '儿童',
    'video': '视频'
}