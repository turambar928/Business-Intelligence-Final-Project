#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
七大分析功能示例脚本
演示如何使用增强的新闻分析系统
"""

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import json

plt.rcParams['font.sans-serif'] = ['SimHei']  # 中文字体
plt.rcParams['axes.unicode_minus'] = False

class SevenAnalysisExamples:
    """七大分析功能示例"""
    
    def __init__(self, mysql_config):
        self.mysql_config = mysql_config
        
    def connect_db(self):
        """连接数据库"""
        return mysql.connector.connect(**self.mysql_config)
    
    def analysis_1_news_lifecycle(self, news_id: str = None):
        """分析1：单个新闻的生命周期查询"""
        print("🔍 分析1：新闻生命周期分析")
        print("="*50)
        
        conn = self.connect_db()
        cursor = conn.cursor()
        
        if not news_id:
            # 获取最近的热门新闻
            cursor.execute("""
                SELECT news_id FROM news_lifecycle 
                WHERE calculated_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                ORDER BY popularity_score DESC LIMIT 1
            """)
            result = cursor.fetchone()
            news_id = result[0] if result else "N12345"
        
        # 查询新闻在不同时间段的流行变化
        query = """
        SELECT 
            time_period,
            total_reads,
            total_shares,
            total_likes,
            unique_readers,
            avg_dwell_time,
            popularity_score,
            growth_rate,
            calculated_at
        FROM news_lifecycle 
        WHERE news_id = %s
        ORDER BY 
            FIELD(time_period, '1h', '6h', '12h', '1d', '3d', '7d')
        """
        
        df = pd.read_sql(query, conn, params=[news_id])
        
        if not df.empty:
            print(f"📰 新闻ID: {news_id}")
            print(f"📊 生命周期数据:")
            print(df.to_string(index=False))
            
            # 可视化
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'新闻 {news_id} 生命周期分析', fontsize=16)
            
            # 阅读量变化
            axes[0,0].plot(df['time_period'], df['total_reads'], marker='o')
            axes[0,0].set_title('阅读量变化')
            axes[0,0].set_xlabel('时间段')
            axes[0,0].set_ylabel('阅读量')
            
            # 流行度分数
            axes[0,1].bar(df['time_period'], df['popularity_score'])
            axes[0,1].set_title('流行度分数')
            axes[0,1].set_xlabel('时间段')
            axes[0,1].set_ylabel('流行度分数')
            
            # 互动数据
            axes[1,0].plot(df['time_period'], df['total_shares'], marker='s', label='分享')
            axes[1,0].plot(df['time_period'], df['total_likes'], marker='^', label='点赞')
            axes[1,0].set_title('互动数据')
            axes[1,0].set_xlabel('时间段')
            axes[1,0].set_ylabel('数量')
            axes[1,0].legend()
            
            # 平均停留时间
            axes[1,1].plot(df['time_period'], df['avg_dwell_time'], marker='d', color='red')
            axes[1,1].set_title('平均停留时间')
            axes[1,1].set_xlabel('时间段')
            axes[1,1].set_ylabel('停留时间(秒)')
            
            plt.tight_layout()
            plt.show()
        else:
            print(f"❌ 未找到新闻 {news_id} 的生命周期数据")
        
        conn.close()
    
    def analysis_2_category_trends(self, category: str = None, hours: int = 24):
        """分析2：不同类别新闻变化情况统计"""
        print("🔍 分析2：新闻分类趋势分析")
        print("="*50)
        
        conn = self.connect_db()
        
        # 查询分类趋势数据
        query = """
        SELECT 
            category,
            topic,
            DATE_FORMAT(date_hour, '%%Y-%%m-%%d %%H:00') as hour_period,
            total_articles,
            total_reads,
            total_interactions,
            avg_engagement,
            trending_keywords,
            hot_topics
        FROM category_trends 
        WHERE date_hour >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        """ + (f" AND category = '{category}'" if category else "") + """
        ORDER BY date_hour DESC, avg_engagement DESC
        """
        
        df = pd.read_sql(query, conn, params=[hours])
        
        if not df.empty:
            print(f"📊 过去{hours}小时的分类趋势数据:")
            
            # 按分类汇总
            category_summary = df.groupby('category').agg({
                'total_articles': 'sum',
                'total_reads': 'sum',
                'total_interactions': 'sum',
                'avg_engagement': 'mean'
            }).round(2)
            
            print("\n📈 分类汇总:")
            print(category_summary)
            
            # 找出最热门的话题
            top_topics = df.nlargest(10, 'avg_engagement')[['category', 'topic', 'avg_engagement', 'hour_period']]
            print("\n🔥 最热门话题 (TOP 10):")
            print(top_topics.to_string(index=False))
            
            # 可视化
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'过去{hours}小时新闻分类趋势分析', fontsize=16)
            
            # 各分类文章数量
            category_counts = df.groupby('category')['total_articles'].sum()
            axes[0,0].pie(category_counts.values, labels=category_counts.index, autopct='%1.1f%%')
            axes[0,0].set_title('各分类文章占比')
            
            # 各分类平均参与度
            category_engagement = df.groupby('category')['avg_engagement'].mean()
            axes[0,1].bar(category_engagement.index, category_engagement.values)
            axes[0,1].set_title('各分类平均参与度')
            axes[0,1].tick_params(axis='x', rotation=45)
            
            # 时间趋势（如果有足够数据）
            if len(df['hour_period'].unique()) > 1:
                hourly_data = df.groupby('hour_period')['total_interactions'].sum()
                axes[1,0].plot(range(len(hourly_data)), hourly_data.values, marker='o')
                axes[1,0].set_title('小时互动趋势')
                axes[1,0].set_xlabel('时间点')
                axes[1,0].set_ylabel('总互动数')
            
            # 阅读量 vs 参与度散点图
            axes[1,1].scatter(df['total_reads'], df['avg_engagement'], alpha=0.6)
            axes[1,1].set_title('阅读量 vs 参与度')
            axes[1,1].set_xlabel('总阅读量')
            axes[1,1].set_ylabel('平均参与度')
            
            plt.tight_layout()
            plt.show()
        else:
            print(f"❌ 未找到过去{hours}小时的分类趋势数据")
        
        conn.close()
    
    def analysis_3_user_interests(self, user_id: str = None, days: int = 30):
        """分析3：用户兴趣变化统计查询"""
        print("🔍 分析3：用户兴趣变化分析")
        print("="*50)
        
        conn = self.connect_db()
        cursor = conn.cursor()
        
        if not user_id:
            # 获取一个活跃用户
            cursor.execute("""
                SELECT user_id FROM user_interest_evolution 
                ORDER BY calculated_at DESC LIMIT 1
            """)
            result = cursor.fetchone()
            user_id = result[0] if result else "U335175"
        
        # 查询用户兴趣变化
        query = """
        SELECT 
            date_period,
            interest_categories,
            interest_scores,
            reading_patterns,
            behavior_changes,
            engagement_trend,
            diversity_score
        FROM user_interest_evolution 
        WHERE user_id = %s 
        AND date_period >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        ORDER BY date_period
        """
        
        df = pd.read_sql(query, conn, params=[user_id, days])
        
        if not df.empty:
            print(f"👤 用户ID: {user_id}")
            print(f"📊 过去{days}天的兴趣变化数据:")
            
            # 解析JSON数据
            df['interest_categories'] = df['interest_categories'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else x
            )
            df['interest_scores'] = df['interest_scores'].apply(
                lambda x: json.loads(x) if isinstance(x, str) else x
            )
            
            print(f"\n📈 兴趣演化趋势:")
            print(df[['date_period', 'engagement_trend', 'diversity_score']].to_string(index=False))
            
            # 分析兴趣变化
            if len(df) > 1:
                latest_interests = df.iloc[-1]['interest_categories']
                earliest_interests = df.iloc[0]['interest_categories']
                
                print(f"\n🔄 兴趣变化对比:")
                print(f"早期兴趣: {earliest_interests}")
                print(f"最新兴趣: {latest_interests}")
                
                # 计算兴趣稳定性
                common_interests = set(latest_interests) & set(earliest_interests)
                stability_score = len(common_interests) / len(set(latest_interests) | set(earliest_interests))
                print(f"兴趣稳定性: {stability_score:.2f}")
            
            # 可视化
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'用户 {user_id} 兴趣变化分析', fontsize=16)
            
            # 参与度趋势
            axes[0,0].plot(df['date_period'], df['engagement_trend'], marker='o')
            axes[0,0].set_title('参与度趋势')
            axes[0,0].set_xlabel('日期')
            axes[0,0].set_ylabel('参与度')
            axes[0,0].tick_params(axis='x', rotation=45)
            
            # 兴趣多样性
            axes[0,1].plot(df['date_period'], df['diversity_score'], marker='s', color='green')
            axes[0,1].set_title('兴趣多样性变化')
            axes[0,1].set_xlabel('日期')
            axes[0,1].set_ylabel('多样性分数')
            axes[0,1].tick_params(axis='x', rotation=45)
            
            # 兴趣分布（最近一期）
            if df.iloc[-1]['interest_scores']:
                latest_scores = df.iloc[-1]['interest_scores']
                if isinstance(latest_scores, dict):
                    axes[1,0].bar(latest_scores.keys(), latest_scores.values())
                    axes[1,0].set_title('当前兴趣分布')
                    axes[1,0].set_xlabel('兴趣类别')
                    axes[1,0].set_ylabel('兴趣分数')
                    axes[1,0].tick_params(axis='x', rotation=45)
            
            # 参与度 vs 多样性
            axes[1,1].scatter(df['engagement_trend'], df['diversity_score'], alpha=0.7)
            axes[1,1].set_title('参与度 vs 兴趣多样性')
            axes[1,1].set_xlabel('参与度趋势')
            axes[1,1].set_ylabel('多样性分数')
            
            plt.tight_layout()
            plt.show()
        else:
            print(f"❌ 未找到用户 {user_id} 的兴趣变化数据")
        
        conn.close()
    
    def analysis_4_multidim_statistics(self, conditions: dict = None):
        """分析4：多维度统计查询"""
        print("🔍 分析4：多维度统计查询")
        print("="*50)
        
        conn = self.connect_db()
        
        # 默认查询条件
        if not conditions:
            conditions = {
                'time_range': '24h',
                'category': None,
                'min_word_count': 100,
                'device_type': None
            }
        
        # 构建动态查询
        base_query = """
        SELECT 
            ne.category,
            ne.topic,
            ne.word_count,
            ue.device_type,
            ue.reading_time_of_day,
            ue.is_weekend,
            COUNT(*) as interaction_count,
            COUNT(DISTINCT ue.user_id) as unique_users,
            AVG(ue.dwell_time) as avg_dwell_time,
            AVG(ue.engagement_score) as avg_engagement,
            SUM(CASE WHEN ue.action_type = 'share' THEN 1 ELSE 0 END) as total_shares,
            SUM(CASE WHEN ue.action_type = 'like' THEN 1 ELSE 0 END) as total_likes
        FROM user_events ue
        JOIN news_articles ne ON ue.news_id = ne.news_id
        WHERE ue.timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """
        
        params = []
        
        # 添加查询条件
        if conditions.get('category'):
            base_query += " AND ne.category = %s"
            params.append(conditions['category'])
        
        if conditions.get('min_word_count'):
            base_query += " AND ne.word_count >= %s"
            params.append(conditions['min_word_count'])
        
        if conditions.get('device_type'):
            base_query += " AND ue.device_type = %s"
            params.append(conditions['device_type'])
        
        base_query += """
        GROUP BY ne.category, ne.topic, ne.word_count, ue.device_type, 
                 ue.reading_time_of_day, ue.is_weekend
        ORDER BY avg_engagement DESC
        LIMIT 50
        """
        
        df = pd.read_sql(base_query, conn, params=params)
        
        if not df.empty:
            print(f"📊 多维度统计结果 (查询条件: {conditions}):")
            print(f"📝 共找到 {len(df)} 条记录")
            
            # 基础统计
            print(f"\n📈 基础统计:")
            print(f"总互动数: {df['interaction_count'].sum():,}")
            print(f"独立用户数: {df['unique_users'].sum():,}")
            print(f"平均停留时间: {df['avg_dwell_time'].mean():.1f}秒")
            print(f"平均参与度: {df['avg_engagement'].mean():.3f}")
            
            # 按不同维度统计
            print(f"\n📊 按类别统计:")
            category_stats = df.groupby('category').agg({
                'interaction_count': 'sum',
                'unique_users': 'sum',
                'avg_engagement': 'mean'
            }).round(2)
            print(category_stats)
            
            print(f"\n📱 按设备类型统计:")
            device_stats = df.groupby('device_type').agg({
                'interaction_count': 'sum',
                'avg_dwell_time': 'mean',
                'avg_engagement': 'mean'
            }).round(2)
            print(device_stats)
            
            print(f"\n⏰ 按阅读时间段统计:")
            time_stats = df.groupby('reading_time_of_day').agg({
                'interaction_count': 'sum',
                'avg_engagement': 'mean'
            }).round(2)
            print(time_stats)
            
            # 可视化
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('多维度统计分析', fontsize=16)
            
            # 类别参与度
            category_engagement = df.groupby('category')['avg_engagement'].mean()
            axes[0,0].bar(category_engagement.index, category_engagement.values)
            axes[0,0].set_title('各类别平均参与度')
            axes[0,0].tick_params(axis='x', rotation=45)
            
            # 设备类型分布
            device_counts = df.groupby('device_type')['interaction_count'].sum()
            axes[0,1].pie(device_counts.values, labels=device_counts.index, autopct='%1.1f%%')
            axes[0,1].set_title('设备类型互动分布')
            
            # 阅读时间段分析
            time_engagement = df.groupby('reading_time_of_day')['avg_engagement'].mean()
            axes[1,0].bar(time_engagement.index, time_engagement.values, color='orange')
            axes[1,0].set_title('不同时间段参与度')
            
            # 字数 vs 参与度
            axes[1,1].scatter(df['word_count'], df['avg_engagement'], alpha=0.6)
            axes[1,1].set_title('文章字数 vs 参与度')
            axes[1,1].set_xlabel('字数')
            axes[1,1].set_ylabel('平均参与度')
            
            plt.tight_layout()
            plt.show()
        else:
            print(f"❌ 根据条件 {conditions} 未找到匹配数据")
        
        conn.close()
    
    def analysis_5_viral_prediction(self, threshold: float = 0.7):
        """分析5：爆款新闻预测分析"""
        print("🔍 分析5：爆款新闻预测分析")
        print("="*50)
        
        conn = self.connect_db()
        
        # 查询病毒性预测数据
        query = """
        SELECT 
            vp.news_id,
            na.headline,
            na.category,
            na.topic,
            vp.viral_score,
            vp.predicted_peak_time,
            vp.key_factors,
            vp.social_signals,
            vp.prediction_accuracy,
            vp.created_at
        FROM viral_news_prediction vp
        JOIN news_articles na ON vp.news_id = na.news_id
        WHERE vp.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        ORDER BY vp.viral_score DESC
        LIMIT 20
        """
        
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            print(f"🔥 最新爆款预测结果:")
            print(f"📝 共分析 {len(df)} 篇新闻")
            
            # 高病毒性新闻
            high_viral = df[df['viral_score'] >= threshold]
            print(f"\n⭐ 高病毒性新闻 (病毒性分数 >= {threshold}):")
            print(f"📊 共 {len(high_viral)} 篇")
            
            if not high_viral.empty:
                for _, row in high_viral.head(5).iterrows():
                    print(f"\n🏆 新闻ID: {row['news_id']}")
                    print(f"📰 标题: {row['headline'][:50]}...")
                    print(f"📊 病毒性分数: {row['viral_score']:.3f}")
                    print(f"📈 预测峰值时间: {row['predicted_peak_time']}")
                    
                    # 解析关键因素
                    if row['key_factors']:
                        factors = json.loads(row['key_factors']) if isinstance(row['key_factors'], str) else row['key_factors']
                        print(f"🔑 关键因素: {factors}")
            
            # 统计分析
            print(f"\n📈 病毒性统计:")
            print(f"平均病毒性分数: {df['viral_score'].mean():.3f}")
            print(f"最高病毒性分数: {df['viral_score'].max():.3f}")
            print(f"病毒性分数标准差: {df['viral_score'].std():.3f}")
            
            # 按类别分析
            category_viral = df.groupby('category')['viral_score'].agg(['mean', 'max', 'count']).round(3)
            print(f"\n📊 按类别病毒性分析:")
            print(category_viral)
            
            # 可视化
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('爆款新闻预测分析', fontsize=16)
            
            # 病毒性分数分布
            axes[0,0].hist(df['viral_score'], bins=20, alpha=0.7, color='red')
            axes[0,0].axvline(threshold, color='black', linestyle='--', label=f'阈值 {threshold}')
            axes[0,0].set_title('病毒性分数分布')
            axes[0,0].set_xlabel('病毒性分数')
            axes[0,0].set_ylabel('频数')
            axes[0,0].legend()
            
            # 各类别病毒性对比
            category_means = df.groupby('category')['viral_score'].mean()
            axes[0,1].bar(category_means.index, category_means.values, color='orange')
            axes[0,1].set_title('各类别平均病毒性')
            axes[0,1].tick_params(axis='x', rotation=45)
            
            # 时间趋势（如果有创建时间）
            df['hour'] = pd.to_datetime(df['created_at']).dt.hour
            hourly_viral = df.groupby('hour')['viral_score'].mean()
            axes[1,0].plot(hourly_viral.index, hourly_viral.values, marker='o')
            axes[1,0].set_title('不同时间病毒性趋势')
            axes[1,0].set_xlabel('小时')
            axes[1,0].set_ylabel('平均病毒性分数')
            
            # 病毒性 vs 预测准确度
            if df['prediction_accuracy'].notna().any():
                axes[1,1].scatter(df['viral_score'], df['prediction_accuracy'], alpha=0.6)
                axes[1,1].set_title('病毒性分数 vs 预测准确度')
                axes[1,1].set_xlabel('病毒性分数')
                axes[1,1].set_ylabel('预测准确度')
            
            plt.tight_layout()
            plt.show()
        else:
            print("❌ 未找到爆款预测数据")
        
        conn.close()
    
    def analysis_6_real_time_recommendations(self, user_id: str = None):
        """分析6：实时个性化推荐"""
        print("🔍 分析6：实时个性化推荐分析")
        print("="*50)
        
        conn = self.connect_db()
        cursor = conn.cursor()
        
        if not user_id:
            # 获取最近有推荐的用户
            cursor.execute("""
                SELECT user_id FROM real_time_recommendations 
                ORDER BY generated_at DESC LIMIT 1
            """)
            result = cursor.fetchone()
            user_id = result[0] if result else "U335175"
        
        # 查询用户推荐数据
        query = """
        SELECT 
            user_id,
            recommended_news,
            recommendation_scores,
            recommendation_reasons,
            context_factors,
            generated_at,
            clicked_news,
            recommendation_effectiveness
        FROM real_time_recommendations 
        WHERE user_id = %s
        ORDER BY generated_at DESC
        LIMIT 5
        """
        
        df = pd.read_sql(query, conn, params=[user_id])
        
        if not df.empty:
            print(f"👤 用户ID: {user_id}")
            print(f"📊 最近推荐记录:")
            
            for idx, row in df.iterrows():
                print(f"\n🕒 推荐时间: {row['generated_at']}")
                
                # 解析推荐新闻
                recommended_news = json.loads(row['recommended_news']) if isinstance(row['recommended_news'], str) else row['recommended_news']
                scores = json.loads(row['recommendation_scores']) if isinstance(row['recommendation_scores'], str) else row['recommendation_scores']
                
                print(f"📰 推荐新闻: {recommended_news[:5]}...")  # 显示前5条
                print(f"📊 推荐分数: {scores[:5] if isinstance(scores, list) else list(scores.values())[:5]}...")
                
                # 推荐效果
                if row['recommendation_effectiveness']:
                    print(f"✅ 推荐效果: {row['recommendation_effectiveness']:.3f}")
                
                # 上下文因素
                if row['context_factors']:
                    context = json.loads(row['context_factors']) if isinstance(row['context_factors'], str) else row['context_factors']
                    print(f"📋 上下文: {context}")
            
            # 查询推荐效果统计
            effectiveness_query = """
            SELECT 
                AVG(recommendation_effectiveness) as avg_effectiveness,
                COUNT(*) as total_recommendations,
                SUM(CASE WHEN recommendation_effectiveness > 0.5 THEN 1 ELSE 0 END) as good_recommendations
            FROM real_time_recommendations 
            WHERE user_id = %s AND recommendation_effectiveness IS NOT NULL
            """
            
            cursor.execute(effectiveness_query, [user_id])
            stats = cursor.fetchone()
            
            if stats[0]:
                print(f"\n📈 推荐效果统计:")
                print(f"平均效果: {stats[0]:.3f}")
                print(f"总推荐次数: {stats[1]}")
                print(f"良好推荐率: {stats[2]/stats[1]*100:.1f}%")
        else:
            print(f"❌ 未找到用户 {user_id} 的推荐数据")
        
        conn.close()
    
    def analysis_7_query_logs(self, hours: int = 24):
        """分析7：查询日志记录和性能分析"""
        print("🔍 分析7：查询日志和性能分析")
        print("="*50)
        
        conn = self.connect_db()
        
        # 查询日志统计
        query = """
        SELECT 
            query_type,
            COUNT(*) as query_count,
            AVG(execution_time_ms) as avg_execution_time,
            MAX(execution_time_ms) as max_execution_time,
            MIN(execution_time_ms) as min_execution_time,
            AVG(result_count) as avg_result_count,
            SUM(CASE WHEN error_message IS NOT NULL THEN 1 ELSE 0 END) as error_count
        FROM query_logs 
        WHERE query_timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        GROUP BY query_type
        ORDER BY query_count DESC
        """
        
        df = pd.read_sql(query, conn, params=[hours])
        
        if not df.empty:
            print(f"📊 过去{hours}小时查询统计:")
            print(df.to_string(index=False))
            
            # 性能分析
            total_queries = df['query_count'].sum()
            total_errors = df['error_count'].sum()
            error_rate = total_errors / total_queries * 100 if total_queries > 0 else 0
            
            print(f"\n📈 性能指标:")
            print(f"总查询数: {total_queries:,}")
            print(f"总错误数: {total_errors}")
            print(f"错误率: {error_rate:.2f}%")
            print(f"平均执行时间: {df['avg_execution_time'].mean():.1f}ms")
            
            # 慢查询分析
            slow_query_threshold = 1000  # 1秒
            slow_queries = df[df['max_execution_time'] > slow_query_threshold]
            
            if not slow_queries.empty:
                print(f"\n⚠️ 慢查询分析 (>{slow_query_threshold}ms):")
                print(slow_queries[['query_type', 'max_execution_time', 'avg_execution_time']].to_string(index=False))
            
            # 可视化
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(f'过去{hours}小时查询日志分析', fontsize=16)
            
            # 查询类型分布
            axes[0,0].pie(df['query_count'], labels=df['query_type'], autopct='%1.1f%%')
            axes[0,0].set_title('查询类型分布')
            
            # 平均执行时间对比
            axes[0,1].bar(df['query_type'], df['avg_execution_time'])
            axes[0,1].set_title('平均执行时间')
            axes[0,1].set_ylabel('执行时间(ms)')
            axes[0,1].tick_params(axis='x', rotation=45)
            
            # 错误率分析
            df['error_rate'] = df['error_count'] / df['query_count'] * 100
            axes[1,0].bar(df['query_type'], df['error_rate'], color='red', alpha=0.7)
            axes[1,0].set_title('各类型查询错误率')
            axes[1,0].set_ylabel('错误率(%)')
            axes[1,0].tick_params(axis='x', rotation=45)
            
            # 查询量 vs 执行时间
            axes[1,1].scatter(df['query_count'], df['avg_execution_time'], s=100, alpha=0.7)
            axes[1,1].set_title('查询量 vs 执行时间')
            axes[1,1].set_xlabel('查询数量')
            axes[1,1].set_ylabel('平均执行时间(ms)')
            
            # 添加标签
            for i, row in df.iterrows():
                axes[1,1].annotate(row['query_type'], 
                                 (row['query_count'], row['avg_execution_time']),
                                 xytext=(5, 5), textcoords='offset points', fontsize=8)
            
            plt.tight_layout()
            plt.show()
            
            # 详细的时间序列分析
            time_query = """
            SELECT 
                DATE_FORMAT(query_timestamp, '%%H:00') as hour,
                COUNT(*) as hourly_queries,
                AVG(execution_time_ms) as hourly_avg_time
            FROM query_logs 
            WHERE query_timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            GROUP BY DATE_FORMAT(query_timestamp, '%%H:00')
            ORDER BY hour
            """
            
            time_df = pd.read_sql(time_query, conn, params=[hours])
            
            if not time_df.empty and len(time_df) > 1:
                print(f"\n⏰ 小时级查询趋势:")
                print(time_df.to_string(index=False))
        else:
            print(f"❌ 过去{hours}小时内无查询日志")
        
        conn.close()

def main():
    """主函数 - 演示所有七大分析功能"""
    # MySQL配置
    mysql_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'your_password',
        'database': 'news_analytics'
    }
    
    # 创建分析实例
    analyzer = SevenAnalysisExamples(mysql_config)
    
    print("🎯 新闻实时分析系统 - 七大分析功能演示")
    print("="*60)
    
    try:
        # 1. 新闻生命周期分析
        analyzer.analysis_1_news_lifecycle()
        
        # 2. 分类趋势分析
        analyzer.analysis_2_category_trends()
        
        # 3. 用户兴趣变化分析
        analyzer.analysis_3_user_interests()
        
        # 4. 多维度统计查询
        analyzer.analysis_4_multidim_statistics()
        
        # 5. 爆款新闻预测
        analyzer.analysis_5_viral_prediction()
        
        # 6. 实时推荐
        analyzer.analysis_6_real_time_recommendations()
        
        # 7. 查询日志分析
        analyzer.analysis_7_query_logs()
        
        print("\n✅ 所有分析功能演示完成!")
        
    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")

if __name__ == "__main__":
    main() 