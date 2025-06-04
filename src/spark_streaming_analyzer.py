# -*- coding: utf-8 -*-
"""
新闻实时分析系统 - Spark Streaming 分析器
实现从Kafka消费数据，进行AI+BI分析，并存储到MySQL
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from kafka import KafkaConsumer
import pymysql
import pandas as pd
import numpy as np
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import LatentDirichletAllocation
import warnings
warnings.filterwarnings('ignore')

class NewsStreamingAnalyzer:
    """新闻流数据实时分析器"""
    
    def __init__(self, kafka_config: Dict, mysql_config: Dict, spark_config: Dict):
        self.kafka_config = kafka_config
        self.mysql_config = mysql_config
        self.spark_config = spark_config
        
        # 初始化Spark
        self.spark = self._init_spark()
        
        # 初始化数据库连接
        self.mysql_conn = self._init_mysql()
        
        # 分析模型
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.topic_model = LatentDirichletAllocation(n_components=10, random_state=42)
        
        # 缓存数据
        self.news_cache = {}
        self.user_profile_cache = {}
        
        logging.info("新闻流分析器初始化完成")
    
    def _init_spark(self) -> SparkSession:
        """初始化Spark Session"""
        spark = SparkSession.builder \
            .appName(self.spark_config['app_name']) \
            .master(self.spark_config['master']) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def _init_mysql(self):
        """初始化MySQL连接"""
        return pymysql.connect(
            host=self.mysql_config['host'],
            port=self.mysql_config['port'],
            user=self.mysql_config['username'],
            password=self.mysql_config['password'],
            database=self.mysql_config['database'],
            charset=self.mysql_config['charset']
        )
    
    def start_streaming(self):
        """启动流数据处理"""
        # 从Kafka读取流数据
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ",".join(self.kafka_config['bootstrap_servers'])) \
            .option("subscribe", self.kafka_config['topic_name']) \
            .option("startingOffsets", "latest") \
            .load()
        
        # 定义数据schema
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("clicked_news", ArrayType(StringType()), True),
            StructField("unclicked_news", ArrayType(StringType()), True),
            StructField("user_history", ArrayType(StringType()), True)
        ])
        
        # 解析JSON数据
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # 启动流处理
        query = parsed_df.writeStream \
            .foreachBatch(self._process_batch) \
            .outputMode("append") \
            .trigger(processingTime=f"{self.spark_config['batch_interval']} seconds") \
            .option("checkpointLocation", self.spark_config['checkpoint_dir']) \
            .start()
        
        logging.info("流数据处理已启动...")
        query.awaitTermination()
    
    def _process_batch(self, batch_df, batch_id):
        """处理每个批次的数据"""
        if batch_df.count() == 0:
            return
        
        logging.info(f"处理批次 {batch_id}, 数据量: {batch_df.count()}")
        
        # 转换为Pandas DataFrame以便处理
        batch_pandas = batch_df.toPandas()
        
        # 1. 用户行为分析
        user_behaviors = self._analyze_user_behavior(batch_pandas)
        
        # 2. 新闻热度分析
        news_popularity = self._analyze_news_popularity(batch_pandas)
        
        # 3. 实时趋势分析
        trending_topics = self._analyze_trending_topics(batch_pandas)
        
        # 4. 情感分析
        sentiment_analysis = self._analyze_sentiment(batch_pandas)
        
        # 5. 推荐分析
        recommendations = self._generate_recommendations(batch_pandas)
        
        # 6. 存储分析结果
        self._save_analysis_results({
            'user_behaviors': user_behaviors,
            'news_popularity': news_popularity,
            'trending_topics': trending_topics,
            'sentiment_analysis': sentiment_analysis,
            'recommendations': recommendations,
            'batch_id': batch_id,
            'timestamp': datetime.now()
        })
    
    def _analyze_user_behavior(self, df: pd.DataFrame) -> Dict:
        """分析用户行为模式"""
        results = {}
        
        # 计算点击率
        df['click_rate'] = df.apply(
            lambda row: len(row['clicked_news']) / (len(row['clicked_news']) + len(row['unclicked_news'])) 
            if (len(row['clicked_news']) + len(row['unclicked_news'])) > 0 else 0, axis=1
        )
        
        # 用户活跃度统计
        results['avg_click_rate'] = df['click_rate'].mean()
        results['active_users'] = df['user_id'].nunique()
        results['total_impressions'] = len(df)
        results['total_clicks'] = df['clicked_news'].apply(len).sum()
        
        # 用户兴趣分析
        all_clicked_news = []
        for clicked_list in df['clicked_news']:
            all_clicked_news.extend(clicked_list)
        
        results['popular_news'] = pd.Series(all_clicked_news).value_counts().head(10).to_dict()
        
        return results
    
    def _analyze_news_popularity(self, df: pd.DataFrame) -> Dict:
        """分析新闻热度"""
        # 统计每个新闻的点击次数
        click_counts = {}
        impression_counts = {}
        
        for _, row in df.iterrows():
            # 点击统计
            for news_id in row['clicked_news']:
                click_counts[news_id] = click_counts.get(news_id, 0) + 1
            
            # 曝光统计
            all_news = row['clicked_news'] + row['unclicked_news']
            for news_id in all_news:
                impression_counts[news_id] = impression_counts.get(news_id, 0) + 1
        
        # 计算CTR
        ctr_data = {}
        for news_id in impression_counts:
            clicks = click_counts.get(news_id, 0)
            impressions = impression_counts[news_id]
            ctr_data[news_id] = {
                'clicks': clicks,
                'impressions': impressions,
                'ctr': clicks / impressions if impressions > 0 else 0
            }
        
        # 排序获取热门新闻
        popular_by_clicks = sorted(click_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        popular_by_ctr = sorted(ctr_data.items(), key=lambda x: x[1]['ctr'], reverse=True)[:20]
        
        return {
            'popular_by_clicks': popular_by_clicks,
            'popular_by_ctr': popular_by_ctr,
            'total_news_count': len(impression_counts)
        }
    
    def _analyze_trending_topics(self, df: pd.DataFrame) -> Dict:
        """分析热门话题趋势"""
        # 获取所有点击的新闻
        all_clicked_news = []
        for clicked_list in df['clicked_news']:
            all_clicked_news.extend(clicked_list)
        
        if not all_clicked_news:
            return {'trending_news': [], 'trending_score': {}}
        
        # 统计新闻点击频次
        news_counts = pd.Series(all_clicked_news).value_counts()
        
        # 计算趋势得分（考虑时间衰减）
        current_time = datetime.now()
        trending_scores = {}
        
        for news_id, count in news_counts.items():
            # 简单的趋势得分计算
            base_score = count * 10
            trending_scores[news_id] = base_score
        
        trending_news = sorted(trending_scores.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'trending_news': trending_news,
            'trending_score': trending_scores
        }
    
    def _analyze_sentiment(self, df: pd.DataFrame) -> Dict:
        """情感分析（基于新闻标题和内容）"""
        # 这里简化处理，实际应该分析新闻内容
        sentiment_stats = {
            'positive_ratio': 0.6,  # 模拟数据
            'negative_ratio': 0.2,
            'neutral_ratio': 0.2,
            'average_sentiment': 0.3
        }
        
        return sentiment_stats
    
    def _generate_recommendations(self, df: pd.DataFrame) -> Dict:
        """生成推荐结果"""
        recommendations = {}
        
        for _, row in df.iterrows():
            user_id = row['user_id']
            user_history = row['user_history']
            
            # 基于协同过滤的简单推荐
            if len(user_history) > 0:
                # 获取用户历史新闻的相似用户
                similar_users = self._find_similar_users(user_id, user_history, df)
                
                # 为用户推荐新闻
                recommended_news = self._recommend_news_for_user(user_id, similar_users, df)
                recommendations[user_id] = recommended_news[:10]  # 推荐前10个
        
        return recommendations
    
    def _find_similar_users(self, target_user: str, target_history: List[str], df: pd.DataFrame) -> List[str]:
        """找到相似用户"""
        similar_users = []
        target_set = set(target_history)
        
        for _, row in df.iterrows():
            if row['user_id'] != target_user:
                user_history_set = set(row['user_history'])
                # 计算Jaccard相似度
                intersection = len(target_set.intersection(user_history_set))
                union = len(target_set.union(user_history_set))
                
                if union > 0:
                    similarity = intersection / union
                    if similarity > 0.1:  # 相似度阈值
                        similar_users.append((row['user_id'], similarity))
        
        # 按相似度排序
        similar_users.sort(key=lambda x: x[1], reverse=True)
        return [user[0] for user in similar_users[:5]]  # 返回前5个相似用户
    
    def _recommend_news_for_user(self, user_id: str, similar_users: List[str], df: pd.DataFrame) -> List[str]:
        """为用户推荐新闻"""
        recommended_news = []
        user_history = set()
        
        # 获取用户历史
        user_row = df[df['user_id'] == user_id]
        if not user_row.empty:
            user_history = set(user_row.iloc[0]['user_history'])
        
        # 收集相似用户的点击新闻
        for similar_user in similar_users:
            similar_user_rows = df[df['user_id'] == similar_user]
            for _, row in similar_user_rows.iterrows():
                for news_id in row['clicked_news']:
                    if news_id not in user_history and news_id not in recommended_news:
                        recommended_news.append(news_id)
        
        return recommended_news
    
    def _save_analysis_results(self, results: Dict):
        """保存分析结果到MySQL"""
        try:
            cursor = self.mysql_conn.cursor()
            
            # 保存用户行为分析结果
            user_behavior_sql = """
                INSERT INTO user_behavior_analytics 
                (batch_id, timestamp, avg_click_rate, active_users, total_impressions, total_clicks)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            user_data = results['user_behaviors']
            cursor.execute(user_behavior_sql, (
                results['batch_id'],
                results['timestamp'],
                user_data['avg_click_rate'],
                user_data['active_users'],
                user_data['total_impressions'],
                user_data['total_clicks']
            ))
            
            # 保存新闻热度分析结果
            for news_id, clicks in results['news_popularity']['popular_by_clicks']:
                news_popularity_sql = """
                    INSERT INTO news_popularity_analytics 
                    (batch_id, timestamp, news_id, clicks, popularity_score)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                    clicks = clicks + VALUES(clicks),
                    popularity_score = VALUES(popularity_score)
                """
                cursor.execute(news_popularity_sql, (
                    results['batch_id'],
                    results['timestamp'],
                    news_id,
                    clicks,
                    clicks * 10  # 简单的热度得分
                ))
            
            # 保存趋势分析结果
            for news_id, trend_score in results['trending_topics']['trending_news']:
                trending_sql = """
                    INSERT INTO trending_analytics 
                    (batch_id, timestamp, news_id, trend_score)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(trending_sql, (
                    results['batch_id'],
                    results['timestamp'],
                    news_id,
                    trend_score
                ))
            
            # 保存推荐结果
            for user_id, recommended_news in results['recommendations'].items():
                for rank, news_id in enumerate(recommended_news, 1):
                    recommendation_sql = """
                        INSERT INTO recommendation_results 
                        (batch_id, timestamp, user_id, news_id, rank_score)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(recommendation_sql, (
                        results['batch_id'],
                        results['timestamp'],
                        user_id,
                        news_id,
                        1.0 / rank  # 排名得分
                    ))
            
            self.mysql_conn.commit()
            logging.info(f"批次 {results['batch_id']} 分析结果已保存到数据库")
            
        except Exception as e:
            logging.error(f"保存分析结果时出错: {str(e)}")
            self.mysql_conn.rollback()
    
    def stop(self):
        """停止分析器"""
        if self.spark:
            self.spark.stop()
        if self.mysql_conn:
            self.mysql_conn.close()
        logging.info("新闻流分析器已停止")

# 使用示例
if __name__ == "__main__":
    # 配置信息
    kafka_config = {
        'bootstrap_servers': ['localhost:9092'],
        'topic_name': 'news_impression_logs',
        'consumer_group': 'news_analytics_group'
    }
    
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'database': 'news_analytics',
        'username': 'root',
        'password': 'password',
        'charset': 'utf8mb4'
    }
    
    spark_config = {
        'app_name': 'NewsRealTimeAnalytics',
        'batch_interval': 10,
        'checkpoint_dir': './checkpoints',
        'master': 'local[*]'
    }
    
    # 初始化并启动分析器
    analyzer = NewsStreamingAnalyzer(kafka_config, mysql_config, spark_config)
    
    try:
        analyzer.start_streaming()
    except KeyboardInterrupt:
        print("停止分析器...")
        analyzer.stop() 