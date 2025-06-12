#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的Spark流处理分析器
支持新日志格式和七大分析功能
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, max as spark_max, 
    min as spark_min, sum as spark_sum, collect_list, 
    when, regexp_extract, length, split, explode,
    current_timestamp, date_format, hour, dayofweek, countDistinct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType, ArrayType
)
from src.enhanced_ai_analysis_engine import EnhancedAIAnalysisEngine

class EnhancedNewsStreamingAnalyzer:
    """增强的新闻流处理分析器"""
    
    def __init__(self, kafka_config: Dict[str, Any], mysql_config: Dict[str, Any]):
        self.kafka_config = kafka_config
        self.mysql_config = mysql_config
        self.logger = logging.getLogger(__name__)
        
        # 初始化Spark
        self.spark = self._create_spark_session()
        
        # 初始化AI分析引擎
        self.ai_engine = EnhancedAIAnalysisEngine(mysql_config)
        
        # 定义增强的数据模式
        self.schema = self._define_enhanced_schema()
        
        # 批次计数器
        self.batch_count = 0
    
    def _create_spark_session(self) -> SparkSession:
        """创建Spark会话"""
        return SparkSession.builder \
            .appName("EnhancedNewsStreamingAnalyzer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
    
    def _define_enhanced_schema(self) -> StructType:
        """定义增强的数据模式"""
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("news_id", StringType(), True),
            StructField("action_type", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("dwell_time", IntegerType(), True),
            StructField("device_info", StructType([
                StructField("device_type", StringType(), True),
                StructField("os", StringType(), True),
                StructField("browser", StringType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("country", StringType(), True),
                StructField("province", StringType(), True),
                StructField("city", StringType(), True)
            ]), True),
            StructField("news_details", StructType([
                StructField("category", StringType(), True),
                StructField("topic", StringType(), True),
                StructField("headline", StringType(), True),
                StructField("content", StringType(), True),
                StructField("word_count", IntegerType(), True),
                StructField("publish_time", StringType(), True),
                StructField("source", StringType(), True),
                StructField("tags", ArrayType(StringType()), True),
                StructField("entities", StructType([
                    StructField("persons", ArrayType(StringType()), True),
                    StructField("organizations", ArrayType(StringType()), True),
                    StructField("locations", ArrayType(StringType()), True)
                ]), True)
            ]), True),
            StructField("user_context", StructType([
                StructField("previous_articles", ArrayType(StringType()), True),
                StructField("reading_time_of_day", StringType(), True),
                StructField("is_weekend", BooleanType(), True),
                StructField("user_interests", ArrayType(StringType()), True),
                StructField("engagement_score", DoubleType(), True)
            ]), True),
            StructField("interaction_data", StructType([
                StructField("scroll_depth", DoubleType(), True),
                StructField("clicks_count", IntegerType(), True),
                StructField("shares_count", IntegerType(), True),
                StructField("comments_count", IntegerType(), True),
                StructField("likes_count", IntegerType(), True)
            ]), True)
        ])
    
    def start_streaming(self):
        """启动流处理"""
        self.logger.info("启动增强的新闻流处理分析器...")
        
        # 读取Kafka流
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
            .option("subscribe", self.kafka_config['topic']) \
            .option("startingOffsets", "latest") \
            .load()
        
        # 解析JSON数据
        parsed_df = df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")
        
        # 启动多个分析查询
        queries = []
        
        # 1. 实时新闻热度分析
        news_popularity_query = self._create_news_popularity_query(parsed_df)
        queries.append(news_popularity_query)
        
        # 2. 用户行为分析
        user_behavior_query = self._create_user_behavior_query(parsed_df)
        queries.append(user_behavior_query)
        
        # 3. 分类趋势分析
        category_trend_query = self._create_category_trend_query(parsed_df)
        queries.append(category_trend_query)
        
        # 4. 主要AI分析处理
        ai_analysis_query = self._create_ai_analysis_query(parsed_df)
        queries.append(ai_analysis_query)
        
        # 等待所有查询完成
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:
            self.logger.info("停止流处理...")
            for query in queries:
                query.stop()
        finally:
            self.spark.stop()
    
    def _create_news_popularity_query(self, df):
        """创建新闻热度分析查询"""
        news_popularity = df \
            .filter(col("action_type").isin(["read", "share", "like"])) \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "10 minutes"),
                col("news_id"),
                col("news_details.category"),
                col("news_details.topic")
            ) \
            .agg(
                count("*").alias("total_interactions"),
                countDistinct("user_id").alias("unique_users"),
                avg("dwell_time").alias("avg_dwell_time"),
                spark_sum(when(col("action_type") == "share", 1).otherwise(0)).alias("shares"),
                spark_sum(when(col("action_type") == "like", 1).otherwise(0)).alias("likes"),
                avg("user_context.engagement_score").alias("avg_engagement")
            )
        
        return news_popularity.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="30 seconds") \
            .start()
    
    def _create_user_behavior_query(self, df):
        """创建用户行为分析查询"""
        user_behavior = df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "15 minutes"),
                col("user_id"),
                col("device_info.device_type")
            ) \
            .agg(
                count("*").alias("total_actions"),
                countDistinct("news_id").alias("articles_read"),
                avg("dwell_time").alias("avg_reading_time"),
                avg("interaction_data.scroll_depth").alias("avg_scroll_depth"),
                spark_sum("interaction_data.clicks_count").alias("total_clicks"),
                avg("user_context.engagement_score").alias("engagement_score")
            )
        
        return user_behavior.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="45 seconds") \
            .start()
    
    def _create_category_trend_query(self, df):
        """创建分类趋势分析查询"""
        category_trends = df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "20 minutes"),
                col("news_details.category"),
                col("news_details.topic")
            ) \
            .agg(
                countDistinct("news_id").alias("unique_articles"),
                count("*").alias("total_interactions"),
                countDistinct("user_id").alias("unique_readers"),
                avg("user_context.engagement_score").alias("avg_engagement"),
                collect_list("news_details.tags").alias("all_tags")
            )
        
        return category_trends.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="60 seconds") \
            .start()
    
    def _create_ai_analysis_query(self, df):
        """创建AI分析查询"""
        def process_batch(batch_df, batch_id):
            """处理每个批次进行AI分析"""
            self.batch_count += 1
            start_time = datetime.now()
            
            self.logger.info(f"处理批次 #{self.batch_count} (batch_id: {batch_id})")
            
            try:
                # 收集批次数据
                batch_data = batch_df.collect()
                
                if batch_data:
                    # 转换为字典格式
                    events = []
                    for row in batch_data:
                        event = self._row_to_dict(row)
                        events.append(event)
                    
                    self.logger.info(f"批次 #{self.batch_count} 包含 {len(events)} 条记录")
                    
                    # 执行AI分析
                    analysis_results = self.ai_engine.analyze_batch(events)
                    
                    # 输出分析结果摘要
                    self._print_analysis_summary(analysis_results)
                    
                    # 保存到MySQL
                    self._save_batch_to_mysql(events, analysis_results)
                    
                    processing_time = (datetime.now() - start_time).total_seconds()
                    self.logger.info(f"批次 #{self.batch_count} 处理完成，耗时: {processing_time:.2f}秒")
                
            except Exception as e:
                self.logger.error(f"批次 #{self.batch_count} 处理失败: {e}")
        
        return df.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime="10 seconds") \
            .start()
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """将Spark Row转换为字典"""
        try:
            return {
                'timestamp': row.timestamp,
                'user_id': row.user_id,
                'news_id': row.news_id,
                'action_type': row.action_type,
                'session_id': row.session_id,
                'dwell_time': row.dwell_time or 0,
                'device_info': {
                    'device_type': row.device_info.device_type if row.device_info else None,
                    'os': row.device_info.os if row.device_info else None,
                    'browser': row.device_info.browser if row.device_info else None
                } if row.device_info else {},
                'location': {
                    'country': row.location.country if row.location else None,
                    'province': row.location.province if row.location else None,
                    'city': row.location.city if row.location else None
                } if row.location else {},
                'news_details': {
                    'category': row.news_details.category if row.news_details else None,
                    'topic': row.news_details.topic if row.news_details else None,
                    'headline': row.news_details.headline if row.news_details else None,
                    'content': row.news_details.content if row.news_details else None,
                    'word_count': row.news_details.word_count if row.news_details else 0,
                    'publish_time': row.news_details.publish_time if row.news_details else None,
                    'source': row.news_details.source if row.news_details else None,
                    'tags': row.news_details.tags if row.news_details else [],
                    'entities': {
                        'persons': row.news_details.entities.persons if (row.news_details and row.news_details.entities) else [],
                        'organizations': row.news_details.entities.organizations if (row.news_details and row.news_details.entities) else [],
                        'locations': row.news_details.entities.locations if (row.news_details and row.news_details.entities) else []
                    } if (row.news_details and row.news_details.entities) else {}
                } if row.news_details else {},
                'user_context': {
                    'previous_articles': row.user_context.previous_articles if row.user_context else [],
                    'reading_time_of_day': row.user_context.reading_time_of_day if row.user_context else None,
                    'is_weekend': row.user_context.is_weekend if row.user_context else False,
                    'user_interests': row.user_context.user_interests if row.user_context else [],
                    'engagement_score': row.user_context.engagement_score if row.user_context else 0.0
                } if row.user_context else {},
                'interaction_data': {
                    'scroll_depth': row.interaction_data.scroll_depth if row.interaction_data else 0.0,
                    'clicks_count': row.interaction_data.clicks_count if row.interaction_data else 0,
                    'shares_count': row.interaction_data.shares_count if row.interaction_data else 0,
                    'comments_count': row.interaction_data.comments_count if row.interaction_data else 0,
                    'likes_count': row.interaction_data.likes_count if row.interaction_data else 0
                } if row.interaction_data else {}
            }
        except Exception as e:
            self.logger.error(f"转换行数据失败: {e}")
            return {}
    
    def _print_analysis_summary(self, results: Dict):
        """打印分析结果摘要"""
        print(f"\n{'='*50}")
        print(f"📊 AI分析结果摘要 - 批次 #{self.batch_count}")
        print(f"{'='*50}")
        print(f"🕒 处理时间: {results.get('timestamp', 'N/A')}")
        print(f"📝 处理记录数: {results.get('processed_count', 0)}")
        
        analysis = results.get('analysis_results', {})
        
        # 新闻生命周期
        lifecycle = analysis.get('news_lifecycle', [])
        print(f"📈 新闻生命周期分析: {len(lifecycle)} 篇新闻")
        
        # 分类趋势
        trends = analysis.get('category_trends', [])
        print(f"📊 分类趋势分析: {len(trends)} 个分类")
        
        # 用户兴趣
        interests = analysis.get('user_interests', [])
        print(f"👥 用户兴趣分析: {len(interests)} 个用户")
        
        # 病毒性预测
        viral = analysis.get('viral_predictions', [])
        high_viral = [v for v in viral if v.get('viral_score', 0) > 0.7]
        print(f"🔥 爆款预测: {len(high_viral)}/{len(viral)} 篇高病毒性新闻")
        
        # 推荐
        recommendations = analysis.get('recommendations', [])
        print(f"💡 生成推荐: {len(recommendations)} 个用户")
        
        # 情感分析
        sentiments = analysis.get('sentiment_analysis', [])
        if sentiments:
            avg_sentiment = sum([s.get('sentiment_score', 0) for s in sentiments]) / len(sentiments)
            print(f"😊 情感分析: 平均情感分数 {avg_sentiment:.3f}")
        
        print(f"{'='*50}\n")
    
    def _save_batch_to_mysql(self, events: List[Dict], analysis_results: Dict):
        """保存批次数据到MySQL"""
        try:
            # 这里可以实现具体的MySQL保存逻辑
            # 保存原始事件和分析结果到相应的表
            pass
        except Exception as e:
            self.logger.error(f"保存批次数据到MySQL失败: {e}")

def main():
    """主函数"""
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Kafka配置
    kafka_config = {
        'bootstrap_servers': 'localhost:9092',
        'topic': 'news-events'
    }
    
    # MySQL配置  
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'your_password',
        'database': 'news_analytics'
    }
    
    # 创建并启动分析器
    analyzer = EnhancedNewsStreamingAnalyzer(kafka_config, mysql_config)
    analyzer.start_streaming()

if __name__ == "__main__":
    main() 