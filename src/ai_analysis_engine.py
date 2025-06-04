# -*- coding: utf-8 -*-
"""
AI分析引擎 - 实现基于AI+BI的新闻分析功能
包含情感分析、主题建模、推荐系统、趋势预测等功能
"""

import json
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Any
from datetime import datetime, timedelta
import logging

# NLP和机器学习库
from textblob import TextBlob
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation, NMF
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.ensemble import RandomForestRegressor
import warnings
warnings.filterwarnings('ignore')

class AIAnalysisEngine:
    """AI分析引擎"""
    
    def __init__(self):
        """初始化AI分析引擎"""
        # 文本向量化器
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )
        
        # 主题模型
        self.lda_model = LatentDirichletAllocation(
            n_components=10,
            random_state=42,
            max_iter=100
        )
        
        # 聚类模型
        self.kmeans_model = KMeans(n_clusters=8, random_state=42)
        
        # 趋势预测模型
        self.trend_predictor = RandomForestRegressor(n_estimators=100, random_state=42)
        
        # 缓存
        self.news_embeddings = {}
        self.user_profiles = {}
        self.topic_keywords = {}
        
        logging.info("AI分析引擎初始化完成")
    
    def analyze_sentiment(self, texts: List[str]) -> List[Dict]:
        """
        情感分析
        Args:
            texts: 文本列表
        Returns:
            情感分析结果列表
        """
        results = []
        
        for text in texts:
            try:
                # 使用TextBlob进行情感分析
                blob = TextBlob(text)
                sentiment_score = blob.sentiment.polarity  # -1到1之间
                
                # 分类情感
                if sentiment_score > 0.1:
                    sentiment_label = 'positive'
                elif sentiment_score < -0.1:
                    sentiment_label = 'negative'
                else:
                    sentiment_label = 'neutral'
                
                results.append({
                    'sentiment_score': round(sentiment_score, 3),
                    'sentiment_label': sentiment_label,
                    'confidence': abs(sentiment_score)
                })
                
            except Exception as e:
                logging.error(f"情感分析错误: {str(e)}")
                results.append({
                    'sentiment_score': 0.0,
                    'sentiment_label': 'neutral',
                    'confidence': 0.0
                })
        
        return results
    
    def extract_topics(self, documents: List[str], n_topics: int = 10) -> Dict:
        """
        主题提取和建模
        Args:
            documents: 文档列表
            n_topics: 主题数量
        Returns:
            主题分析结果
        """
        if not documents:
            return {'topics': [], 'doc_topic_distribution': []}
        
        try:
            # 向量化文档
            doc_term_matrix = self.tfidf_vectorizer.fit_transform(documents)
            
            # 训练LDA主题模型
            self.lda_model.n_components = n_topics
            self.lda_model.fit(doc_term_matrix)
            
            # 获取主题关键词
            feature_names = self.tfidf_vectorizer.get_feature_names_out()
            topics = []
            
            for topic_idx, topic in enumerate(self.lda_model.components_):
                top_keywords_idx = topic.argsort()[-10:][::-1]  # 前10个关键词
                top_keywords = [feature_names[i] for i in top_keywords_idx]
                top_weights = [topic[i] for i in top_keywords_idx]
                
                topics.append({
                    'topic_id': topic_idx,
                    'keywords': top_keywords,
                    'weights': [round(w, 4) for w in top_weights]
                })
            
            # 获取文档-主题分布
            doc_topic_dist = self.lda_model.transform(doc_term_matrix)
            
            return {
                'topics': topics,
                'doc_topic_distribution': doc_topic_dist.tolist()
            }
            
        except Exception as e:
            logging.error(f"主题提取错误: {str(e)}")
            return {'topics': [], 'doc_topic_distribution': []}
    
    def analyze_news_popularity_trend(self, historical_data: pd.DataFrame) -> Dict:
        """
        分析新闻热度趋势
        Args:
            historical_data: 历史数据DataFrame
        Returns:
            趋势分析结果
        """
        results = {
            'trending_news': [],
            'popularity_predictions': {},
            'trend_indicators': {}
        }
        
        try:
            if historical_data.empty:
                return results
            
            # 按新闻ID分组分析趋势
            for news_id in historical_data['news_id'].unique():
                news_data = historical_data[historical_data['news_id'] == news_id].copy()
                news_data = news_data.sort_values('timestamp')
                
                if len(news_data) < 2:
                    continue
                
                # 计算趋势指标
                clicks = news_data['clicks'].values
                time_points = np.arange(len(clicks))
                
                # 计算趋势斜率
                if len(clicks) > 1:
                    slope = np.polyfit(time_points, clicks, 1)[0]
                    
                    # 计算增长率
                    growth_rate = (clicks[-1] - clicks[0]) / clicks[0] if clicks[0] > 0 else 0
                    
                    # 计算波动性
                    volatility = np.std(clicks) / np.mean(clicks) if np.mean(clicks) > 0 else 0
                    
                    trend_score = slope * 0.4 + growth_rate * 0.4 + (1/volatility if volatility > 0 else 1) * 0.2
                    
                    results['trend_indicators'][news_id] = {
                        'slope': round(slope, 4),
                        'growth_rate': round(growth_rate, 4),
                        'volatility': round(volatility, 4),
                        'trend_score': round(trend_score, 4)
                    }
            
            # 排序获取趋势新闻
            sorted_trends = sorted(
                results['trend_indicators'].items(),
                key=lambda x: x[1]['trend_score'],
                reverse=True
            )
            
            results['trending_news'] = sorted_trends[:20]
            
        except Exception as e:
            logging.error(f"趋势分析错误: {str(e)}")
        
        return results
    
    def generate_news_recommendations(self, user_id: str, user_history: List[str], 
                                    all_news: List[str], news_features: Dict) -> List[Dict]:
        """
        生成新闻推荐
        Args:
            user_id: 用户ID
            user_history: 用户历史点击新闻
            all_news: 所有新闻列表
            news_features: 新闻特征数据
        Returns:
            推荐结果列表
        """
        recommendations = []
        
        try:
            if not user_history or not all_news:
                return recommendations
            
            # 构建用户兴趣画像
            user_profile = self._build_user_profile(user_id, user_history, news_features)
            
            # 计算候选新闻与用户兴趣的相似度
            candidate_news = [news for news in all_news if news not in user_history]
            
            for news_id in candidate_news:
                if news_id in news_features:
                    # 计算相似度得分
                    similarity_score = self._calculate_news_similarity(
                        user_profile, news_features[news_id]
                    )
                    
                    # 考虑新闻热度
                    popularity_boost = news_features[news_id].get('popularity_score', 0) * 0.1
                    
                    # 最终推荐得分
                    final_score = similarity_score * 0.8 + popularity_boost * 0.2
                    
                    recommendations.append({
                        'news_id': news_id,
                        'recommendation_score': round(final_score, 4),
                        'similarity_score': round(similarity_score, 4),
                        'popularity_boost': round(popularity_boost, 4)
                    })
            
            # 按得分排序
            recommendations.sort(key=lambda x: x['recommendation_score'], reverse=True)
            
        except Exception as e:
            logging.error(f"推荐生成错误: {str(e)}")
        
        return recommendations[:50]  # 返回前50个推荐
    
    def detect_anomalies(self, metrics_data: pd.DataFrame) -> Dict:
        """
        异常检测
        Args:
            metrics_data: 指标数据
        Returns:
            异常检测结果
        """
        anomalies = {
            'anomaly_points': [],
            'anomaly_scores': [],
            'threshold': 0
        }
        
        try:
            if metrics_data.empty:
                return anomalies
            
            # 使用统计方法检测异常
            for column in ['clicks', 'impressions', 'ctr']:
                if column in metrics_data.columns:
                    values = metrics_data[column].values
                    
                    # 计算Z-score
                    mean_val = np.mean(values)
                    std_val = np.std(values)
                    z_scores = np.abs((values - mean_val) / std_val) if std_val > 0 else np.zeros_like(values)
                    
                    # 设置阈值
                    threshold = 3.0
                    anomaly_mask = z_scores > threshold
                    
                    # 记录异常点
                    anomaly_indices = np.where(anomaly_mask)[0]
                    for idx in anomaly_indices:
                        anomalies['anomaly_points'].append({
                            'index': int(idx),
                            'column': column,
                            'value': float(values[idx]),
                            'z_score': float(z_scores[idx]),
                            'timestamp': metrics_data.iloc[idx].get('timestamp', '').strftime('%Y-%m-%d %H:%M:%S') if 'timestamp' in metrics_data.columns else ''
                        })
            
            anomalies['threshold'] = threshold
            
        except Exception as e:
            logging.error(f"异常检测错误: {str(e)}")
        
        return anomalies
    
    def predict_viral_potential(self, news_features: Dict) -> float:
        """
        预测新闻病毒传播潜力
        Args:
            news_features: 新闻特征
        Returns:
            病毒传播潜力得分 (0-1)
        """
        try:
            # 特征权重
            feature_weights = {
                'sentiment_score': 0.2,
                'topic_relevance': 0.15,
                'title_length': 0.1,
                'early_engagement': 0.25,
                'time_factor': 0.1,
                'category_popularity': 0.2
            }
            
            viral_score = 0.0
            
            # 情感得分 (极端情感更容易传播)
            sentiment = abs(news_features.get('sentiment_score', 0))
            viral_score += sentiment * feature_weights['sentiment_score']
            
            # 主题相关性
            topic_score = news_features.get('topic_relevance', 0.5)
            viral_score += topic_score * feature_weights['topic_relevance']
            
            # 标题长度 (适中长度更佳)
            title_len = news_features.get('title_length', 50)
            optimal_length = 60
            length_score = 1 - abs(title_len - optimal_length) / optimal_length
            viral_score += max(0, length_score) * feature_weights['title_length']
            
            # 早期参与度
            early_clicks = news_features.get('early_clicks', 0)
            early_impressions = news_features.get('early_impressions', 1)
            early_ctr = early_clicks / early_impressions
            viral_score += min(1.0, early_ctr * 10) * feature_weights['early_engagement']
            
            # 时间因素 (新鲜度)
            time_score = news_features.get('freshness_score', 0.5)
            viral_score += time_score * feature_weights['time_factor']
            
            # 类别热度
            category_pop = news_features.get('category_popularity', 0.5)
            viral_score += category_pop * feature_weights['category_popularity']
            
            return min(1.0, viral_score)
            
        except Exception as e:
            logging.error(f"病毒传播潜力预测错误: {str(e)}")
            return 0.0
    
    def _build_user_profile(self, user_id: str, user_history: List[str], news_features: Dict) -> Dict:
        """构建用户兴趣画像"""
        profile = {
            'preferred_categories': {},
            'sentiment_preference': 0.0,
            'topic_interests': {},
            'avg_title_length_pref': 0
        }
        
        try:
            categories = []
            sentiments = []
            title_lengths = []
            
            for news_id in user_history:
                if news_id in news_features:
                    features = news_features[news_id]
                    
                    # 收集类别偏好
                    category = features.get('category', 'unknown')
                    categories.append(category)
                    
                    # 收集情感偏好
                    sentiment = features.get('sentiment_score', 0)
                    sentiments.append(sentiment)
                    
                    # 收集标题长度偏好
                    title_len = features.get('title_length', 50)
                    title_lengths.append(title_len)
            
            # 统计类别偏好
            if categories:
                category_counts = pd.Series(categories).value_counts()
                total = len(categories)
                profile['preferred_categories'] = {
                    cat: count/total for cat, count in category_counts.items()
                }
            
            # 计算情感偏好
            if sentiments:
                profile['sentiment_preference'] = np.mean(sentiments)
            
            # 计算标题长度偏好
            if title_lengths:
                profile['avg_title_length_pref'] = np.mean(title_lengths)
            
        except Exception as e:
            logging.error(f"用户画像构建错误: {str(e)}")
        
        return profile
    
    def _calculate_news_similarity(self, user_profile: Dict, news_features: Dict) -> float:
        """计算新闻与用户兴趣的相似度"""
        similarity = 0.0
        
        try:
            # 类别相似度
            news_category = news_features.get('category', 'unknown')
            category_pref = user_profile['preferred_categories'].get(news_category, 0)
            similarity += category_pref * 0.4
            
            # 情感相似度
            news_sentiment = news_features.get('sentiment_score', 0)
            user_sentiment_pref = user_profile.get('sentiment_preference', 0)
            sentiment_sim = 1 - abs(news_sentiment - user_sentiment_pref) / 2
            similarity += sentiment_sim * 0.3
            
            # 标题长度相似度
            news_title_len = news_features.get('title_length', 50)
            user_title_pref = user_profile.get('avg_title_length_pref', 50)
            length_sim = 1 - abs(news_title_len - user_title_pref) / max(news_title_len, user_title_pref)
            similarity += length_sim * 0.3
            
        except Exception as e:
            logging.error(f"相似度计算错误: {str(e)}")
        
        return min(1.0, similarity)

# 使用示例
if __name__ == "__main__":
    # 初始化AI分析引擎
    ai_engine = AIAnalysisEngine()
    
    # 示例：情感分析
    sample_texts = [
        "This is an amazing breakthrough in technology!",
        "The economic situation is getting worse.",
        "Today's weather is quite normal."
    ]
    
    sentiment_results = ai_engine.analyze_sentiment(sample_texts)
    print("情感分析结果:", sentiment_results)
    
    # 示例：主题提取
    sample_docs = [
        "Technology innovation artificial intelligence machine learning",
        "Sports football basketball championship games",
        "Finance economy market stock investment"
    ]
    
    topic_results = ai_engine.extract_topics(sample_docs)
    print("主题提取结果:", topic_results['topics']) 