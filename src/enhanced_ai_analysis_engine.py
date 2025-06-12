#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的AI分析引擎
支持七大分析功能的完整AI分析系统
"""

import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics.pairwise import cosine_similarity
import mysql.connector
import uuid

class EnhancedAIAnalysisEngine:
    """增强的AI分析引擎"""
    
    def __init__(self, mysql_config: Dict[str, Any]):
        self.mysql_config = mysql_config
        self.logger = logging.getLogger(__name__)
        
        # 初始化ML模型
        self.tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')
        self.lda_model = LatentDirichletAllocation(n_components=10, random_state=42)
        self.viral_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        
        # 缓存和状态
        self.user_profiles = {}
        self.news_embeddings = {}
        self.category_trends = {}
        
    def connect_mysql(self):
        """连接MySQL数据库"""
        try:
            return mysql.connector.connect(**self.mysql_config)
        except Exception as e:
            self.logger.error(f"MySQL连接失败: {e}")
            return None
    
    def analyze_batch(self, batch_data: List[Dict]) -> Dict[str, Any]:
        """分析一个批次的数据"""
        results = {
            'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'processed_count': len(batch_data),
            'analysis_results': {
                'news_lifecycle': [],
                'category_trends': [],
                'user_interests': [],
                'viral_predictions': [],
                'recommendations': [],
                'sentiment_analysis': [],
                'performance_metrics': {}
            },
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            # 1. 新闻生命周期分析
            lifecycle_results = self._analyze_news_lifecycle(batch_data)
            results['analysis_results']['news_lifecycle'] = lifecycle_results
            
            # 2. 分类趋势分析
            trend_results = self._analyze_category_trends(batch_data)
            results['analysis_results']['category_trends'] = trend_results
            
            # 3. 用户兴趣分析
            interest_results = self._analyze_user_interests(batch_data)
            results['analysis_results']['user_interests'] = interest_results
            
            # 4. 爆款新闻预测
            viral_results = self._predict_viral_news(batch_data)
            results['analysis_results']['viral_predictions'] = viral_results
            
            # 5. 实时推荐
            recommendation_results = self._generate_recommendations(batch_data)
            results['analysis_results']['recommendations'] = recommendation_results
            
            # 6. 情感分析
            sentiment_results = self._analyze_sentiment(batch_data)
            results['analysis_results']['sentiment_analysis'] = sentiment_results
            
            # 7. 性能指标计算
            performance_metrics = self._calculate_performance_metrics(batch_data)
            results['analysis_results']['performance_metrics'] = performance_metrics
            
            # 保存结果到数据库
            self._save_analysis_results(results)
            
            return results
            
        except Exception as e:
            self.logger.error(f"批次分析失败: {e}")
            return {'error': str(e), 'batch_id': results['batch_id']}
    
    def _analyze_news_lifecycle(self, batch_data: List[Dict]) -> List[Dict]:
        """分析1：单个新闻的生命周期"""
        lifecycle_results = []
        
        # 按新闻ID分组数据
        news_groups = {}
        for event in batch_data:
            news_id = event.get('news_id')
            if news_id:
                if news_id not in news_groups:
                    news_groups[news_id] = []
                news_groups[news_id].append(event)
        
        for news_id, events in news_groups.items():
            # 计算不同时间段的流行度
            lifecycle_data = self._calculate_lifecycle_metrics(news_id, events)
            lifecycle_results.append(lifecycle_data)
        
        return lifecycle_results
    
    def _calculate_lifecycle_metrics(self, news_id: str, events: List[Dict]) -> Dict:
        """计算新闻生命周期指标"""
        now = datetime.now()
        time_periods = {
            '1h': timedelta(hours=1),
            '6h': timedelta(hours=6), 
            '12h': timedelta(hours=12),
            '1d': timedelta(days=1),
            '3d': timedelta(days=3),
            '7d': timedelta(days=7)
        }
        
        lifecycle_metrics = {
            'news_id': news_id,
            'periods': {}
        }
        
        for period_name, period_duration in time_periods.items():
            period_start = now - period_duration
            period_events = [e for e in events 
                           if datetime.fromisoformat(e.get('timestamp', '')) >= period_start]
            
            metrics = {
                'total_reads': len([e for e in period_events if e.get('action_type') == 'read']),
                'total_shares': len([e for e in period_events if e.get('action_type') == 'share']),
                'total_likes': len([e for e in period_events if e.get('action_type') == 'like']),
                'unique_readers': len(set([e.get('user_id') for e in period_events])),
                'avg_dwell_time': np.mean([e.get('dwell_time', 0) for e in period_events]) if period_events else 0,
                'engagement_score': np.mean([e.get('interaction_data', {}).get('engagement_score', 0) for e in period_events]) if period_events else 0
            }
            
            # 计算流行度分数
            popularity_score = self._calculate_popularity_score(metrics)
            metrics['popularity_score'] = popularity_score
            
            lifecycle_metrics['periods'][period_name] = metrics
        
        return lifecycle_metrics
    
    def _analyze_category_trends(self, batch_data: List[Dict]) -> List[Dict]:
        """分析2：不同类别新闻变化情况"""
        category_data = {}
        
        for event in batch_data:
            news_details = event.get('news_details', {})
            category = news_details.get('category', 'unknown')
            topic = news_details.get('topic', 'general')
            
            key = f"{category}_{topic}"
            if key not in category_data:
                category_data[key] = {
                    'category': category,
                    'topic': topic,
                    'events': []
                }
            category_data[key]['events'].append(event)
        
        trend_results = []
        for key, data in category_data.items():
            trend_metrics = self._calculate_category_metrics(data)
            trend_results.append(trend_metrics)
        
        return trend_results
    
    def _analyze_user_interests(self, batch_data: List[Dict]) -> List[Dict]:
        """分析3：用户兴趣变化统计"""
        user_data = {}
        
        for event in batch_data:
            user_id = event.get('user_id')
            if user_id:
                if user_id not in user_data:
                    user_data[user_id] = []
                user_data[user_id].append(event)
        
        interest_results = []
        for user_id, events in user_data.items():
            interest_analysis = self._analyze_user_interest_evolution(user_id, events)
            interest_results.append(interest_analysis)
        
        return interest_results
    
    def _predict_viral_news(self, batch_data: List[Dict]) -> List[Dict]:
        """分析5：预测爆款新闻"""
        viral_predictions = []
        
        # 提取新闻特征
        news_features = self._extract_news_features(batch_data)
        
        for news_feature in news_features:
            viral_score = self._calculate_viral_potential(news_feature)
            
            prediction = {
                'news_id': news_feature['news_id'],
                'viral_score': viral_score,
                'key_factors': self._identify_viral_factors(news_feature),
                'predicted_peak_time': self._predict_peak_time(news_feature),
                'confidence': self._calculate_prediction_confidence(news_feature)
            }
            viral_predictions.append(prediction)
        
        return viral_predictions
    
    def _generate_recommendations(self, batch_data: List[Dict]) -> List[Dict]:
        """分析6：实时个性化推荐"""
        recommendation_results = []
        
        # 获取活跃用户
        active_users = set([event.get('user_id') for event in batch_data])
        
        for user_id in active_users:
            if user_id:
                recommendations = self._generate_user_recommendations(user_id, batch_data)
                recommendation_results.append(recommendations)
        
        return recommendation_results
    
    def _generate_user_recommendations(self, user_id: str, batch_data: List[Dict]) -> Dict:
        """为用户生成个性化推荐"""
        # 获取用户历史行为
        user_events = [e for e in batch_data if e.get('user_id') == user_id]
        
        # 分析用户偏好
        user_preferences = self._analyze_user_preferences(user_events)
        
        # 获取候选新闻
        candidate_news = self._get_candidate_news(batch_data)
        
        # 计算推荐分数
        recommendations = []
        for news in candidate_news:
            score = self._calculate_recommendation_score(user_preferences, news)
            if score > 0.5:  # 阈值过滤
                recommendations.append({
                    'news_id': news['news_id'],
                    'score': score,
                    'reason': self._generate_recommendation_reason(user_preferences, news)
                })
        
        # 排序和筛选top推荐
        recommendations.sort(key=lambda x: x['score'], reverse=True)
        
        return {
            'user_id': user_id,
            'recommendations': recommendations[:10],  # 返回top10
            'generated_at': datetime.now().isoformat(),
            'context': user_preferences
        }
    
    def _analyze_sentiment(self, batch_data: List[Dict]) -> List[Dict]:
        """情感分析"""
        sentiment_results = []
        
        for event in batch_data:
            news_details = event.get('news_details', {})
            content = news_details.get('content', '') or news_details.get('headline', '')
            
            if content:
                sentiment_data = self._perform_sentiment_analysis(content)
                sentiment_results.append({
                    'news_id': news_details.get('news_id'),
                    'sentiment_score': sentiment_data['score'],
                    'sentiment_label': sentiment_data['label'],
                    'confidence': sentiment_data['confidence'],
                    'keywords': sentiment_data.get('keywords', [])
                })
        
        return sentiment_results
    
    def _perform_sentiment_analysis(self, text: str) -> Dict:
        """执行情感分析"""
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity
        
        # 分类情感
        if polarity > 0.1:
            label = 'positive'
        elif polarity < -0.1:
            label = 'negative'
        else:
            label = 'neutral'
        
        return {
            'score': polarity,
            'label': label,
            'confidence': abs(polarity),
            'subjectivity': subjectivity,
            'keywords': [str(phrase) for phrase in blob.noun_phrases[:5]]
        }
    
    def _calculate_performance_metrics(self, batch_data: List[Dict]) -> Dict:
        """计算性能指标"""
        return {
            'processing_time': datetime.now().isoformat(),
            'batch_size': len(batch_data),
            'unique_users': len(set([e.get('user_id') for e in batch_data])),
            'unique_news': len(set([e.get('news_id') for e in batch_data])),
            'action_distribution': self._calculate_action_distribution(batch_data),
            'avg_engagement': np.mean([e.get('interaction_data', {}).get('engagement_score', 0) for e in batch_data])
        }
    
    def _save_analysis_results(self, results: Dict):
        """保存分析结果到数据库"""
        conn = self.connect_mysql()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            
            # 记录查询日志
            query_id = str(uuid.uuid4())
            log_query = """
            INSERT INTO query_logs (query_id, query_type, sql_query, execution_time_ms, result_count, query_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(log_query, (
                query_id, 'ai_analysis', 'AI batch analysis', 0, results['processed_count'], datetime.now()
            ))
            
            # 保存其他分析结果到相应表格
            # 这里可以根据需要保存到不同的分析表
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"保存分析结果失败: {e}")
        finally:
            conn.close()
    
    # 辅助方法
    def _calculate_popularity_score(self, metrics: Dict) -> float:
        """计算流行度分数"""
        reads = metrics.get('total_reads', 0)
        shares = metrics.get('total_shares', 0)
        likes = metrics.get('total_likes', 0)
        engagement = metrics.get('engagement_score', 0)
        
        # 加权计算流行度
        score = (reads * 1.0 + shares * 3.0 + likes * 2.0) * engagement
        return min(score / 100.0, 1.0)  # 归一化到0-1
    
    def _calculate_viral_potential(self, news_feature: Dict) -> float:
        """计算病毒传播潜力"""
        # 简化的病毒性预测算法
        factors = {
            'engagement_rate': news_feature.get('engagement_rate', 0),
            'sharing_rate': news_feature.get('sharing_rate', 0),
            'word_count': min(news_feature.get('word_count', 0) / 1000, 1.0),
            'sentiment_intensity': abs(news_feature.get('sentiment_score', 0)),
            'topic_popularity': news_feature.get('topic_popularity', 0)
        }
        
        weights = [0.3, 0.3, 0.1, 0.2, 0.1]
        return sum(f * w for f, w in zip(factors.values(), weights))
    
    def _extract_news_features(self, batch_data: List[Dict]) -> List[Dict]:
        """提取新闻特征"""
        features = []
        news_groups = {}
        
        # 按新闻分组
        for event in batch_data:
            news_id = event.get('news_id')
            if news_id:
                if news_id not in news_groups:
                    news_groups[news_id] = []
                news_groups[news_id].append(event)
        
        # 为每个新闻计算特征
        for news_id, events in news_groups.items():
            feature = {
                'news_id': news_id,
                'total_interactions': len(events),
                'unique_users': len(set([e.get('user_id') for e in events])),
                'engagement_rate': np.mean([e.get('interaction_data', {}).get('engagement_score', 0) for e in events]),
                'sharing_rate': len([e for e in events if e.get('action_type') == 'share']) / max(len(events), 1),
                'avg_dwell_time': np.mean([e.get('dwell_time', 0) for e in events])
            }
            
            # 添加新闻内容特征
            if events:
                news_details = events[0].get('news_details', {})
                feature.update({
                    'word_count': news_details.get('word_count', 0),
                    'category': news_details.get('category', ''),
                    'topic': news_details.get('topic', '')
                })
            
            features.append(feature)
        
        return features
    
    def log_query(self, query_type: str, sql_query: str, execution_time_ms: int, result_count: int):
        """记录查询日志（分析7：查询日志记录）"""
        conn = self.connect_mysql()
        if not conn:
            return
        
        try:
            cursor = conn.cursor()
            query_id = str(uuid.uuid4())
            
            log_query = """
            INSERT INTO query_logs (query_id, query_type, sql_query, execution_time_ms, result_count, query_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(log_query, (
                query_id, query_type, sql_query, execution_time_ms, result_count, datetime.now()
            ))
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"记录查询日志失败: {e}")
        finally:
            conn.close() 