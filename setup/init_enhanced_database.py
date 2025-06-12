#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强系统数据库初始化脚本
重新创建所有表结构和初始数据
"""

import mysql.connector
import logging
from datetime import datetime

class DatabaseInitializer:
    """数据库初始化器"""
    
    def __init__(self, host='localhost', user='analytics_user', password='pass123', database='news_analytics'):
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.logger = logging.getLogger(__name__)
    
    def init_database(self):
        """初始化完整数据库"""
        print("🔧 开始初始化增强系统数据库...")
        
        try:
            # 1. 确认数据库存在（不创建新的，因为analytics_user可能没有权限）
            self._verify_database_exists()
            
            # 2. 删除旧表
            self._drop_old_tables()
            
            # 3. 创建新表结构
            self._create_new_tables()
            
            # 4. 创建索引
            self._create_indexes()
            
            # 5. 创建视图
            self._create_views()
            
            # 6. 插入初始数据
            self._insert_initial_data()
            
            print("✅ 数据库初始化完成！")
            
        except Exception as e:
            print(f"❌ 数据库初始化失败: {e}")
            raise
    
    def _verify_database_exists(self):
        """验证数据库是否存在"""
        print("📁 验证数据库连接...")
        
        conn = mysql.connector.connect(**self.config)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            print(f"✅ 连接到数据库: {db_name}")
        finally:
            conn.close()
    
    def _drop_old_tables(self):
        """删除旧表"""
        print("🗑️ 清理旧表结构...")
        
        conn = mysql.connector.connect(**self.config)
        cursor = conn.cursor()
        
        old_tables = [
            'news_analysis_results',
            'user_behavior_analysis', 
            'trending_topics',
            'recommendation_results',
            'sentiment_analysis_results',
            'performance_metrics',
            'processing_batches'
        ]
        
        try:
            # 禁用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            for table in old_tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"  - 删除表: {table}")
            
            # 重新启用外键检查
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            
            conn.commit()
            print("✅ 旧表清理完成")
            
        finally:
            conn.close()
    
    def _create_new_tables(self):
        """创建新表结构"""
        print("🏗️ 创建新表结构...")
        
        conn = mysql.connector.connect(**self.config)
        cursor = conn.cursor()
        
        try:
            # 读取SQL文件
            with open('database/create_enhanced_tables.sql', 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 分割SQL语句，去除注释和空行
            sql_statements = []
            current_statement = ""
            
            for line in sql_content.split('\n'):
                line = line.strip()
                # 跳过注释行和空行
                if line.startswith('--') or not line:
                    continue
                
                current_statement += line + " "
                
                # 如果行以分号结尾，说明是一个完整的SQL语句
                if line.endswith(';'):
                    sql_statements.append(current_statement.strip())
                    current_statement = ""
            
            # 执行每个SQL语句
            for i, stmt in enumerate(sql_statements):
                if stmt:
                    try:
                        cursor.execute(stmt)
                        
                        # 提取表名或视图名
                        if stmt.upper().startswith('CREATE TABLE'):
                            if 'IF NOT EXISTS' in stmt.upper():
                                table_name = stmt.split('IF NOT EXISTS')[1].split('(')[0].strip()
                            else:
                                table_name = stmt.split('TABLE')[1].split('(')[0].strip()
                            print(f"  ✅ 创建表: {table_name}")
                        elif stmt.upper().startswith('CREATE OR REPLACE VIEW'):
                            view_name = stmt.split('VIEW')[1].split('AS')[0].strip()
                            print(f"  ✅ 创建视图: {view_name}")
                        else:
                            print(f"  ✅ 执行SQL语句 {i+1}")
                            
                    except mysql.connector.Error as e:
                        print(f"  ❌ 执行SQL失败: {e}")
                        print(f"     SQL: {stmt[:100]}...")
                        # 继续执行其他语句，不中断整个过程
            
            conn.commit()
            print("✅ 新表结构创建完成")
            
        finally:
            conn.close()
    
    def _create_indexes(self):
        """创建额外索引"""
        print("📊 创建性能索引...")
        
        conn = mysql.connector.connect(**self.config)
        cursor = conn.cursor()
        
        additional_indexes = [
            "CREATE INDEX idx_user_events_composite ON user_events(user_id, news_id, timestamp)",
            "CREATE INDEX idx_news_articles_category_topic ON news_articles(category, topic)",
            "CREATE INDEX idx_ai_analysis_news_time ON ai_analysis_results(news_id, analysis_timestamp)",
            "CREATE INDEX idx_lifecycle_news_period ON news_lifecycle(news_id, time_period, calculated_at)",
            "CREATE INDEX idx_category_trends_time ON category_trends(category, date_hour)",
            "CREATE INDEX idx_user_interest_date ON user_interest_evolution(user_id, date_period)",
            "CREATE INDEX idx_viral_prediction_score ON viral_news_prediction(viral_score, created_at)",
            "CREATE INDEX idx_recommendations_user_time ON real_time_recommendations(user_id, generated_at)",
            "CREATE INDEX idx_query_logs_time_type ON query_logs(query_timestamp, query_type)"
        ]
        
        try:
            for index_sql in additional_indexes:
                try:
                    cursor.execute(index_sql)
                    index_name = index_sql.split('INDEX')[1].split('ON')[0].strip()
                    print(f"  ✅ 创建索引: {index_name}")
                except mysql.connector.Error as e:
                    if e.errno != 1061:  # 忽略重复索引错误
                        print(f"  ⚠️ 索引创建警告: {e}")
            
            conn.commit()
            print("✅ 索引创建完成")
            
        finally:
            conn.close()
    
    def _create_views(self):
        """创建视图（已在SQL文件中包含）"""
        print("👁️ 视图已在表结构中创建")
    
    def _insert_initial_data(self):
        """插入初始数据"""
        print("📝 插入初始数据...")
        
        conn = mysql.connector.connect(**self.config)
        cursor = conn.cursor()
        
        try:
            # 插入示例用户
            users_data = [
                ('U335175', '2023-01-01 00:00:00', 'China', 'Beijing', 'Beijing', '["体育", "科技", "新闻"]', '{"age": 25, "gender": "M"}'),
                ('U146053', '2023-01-15 00:00:00', 'China', 'Shanghai', 'Shanghai', '["娱乐", "生活", "科技"]', '{"age": 30, "gender": "F"}'),
                ('U123456', '2023-02-01 00:00:00', 'China', 'Guangdong', 'Shenzhen', '["商业", "科技", "新闻"]', '{"age": 28, "gender": "M"}')
            ]
            
            cursor.executemany("""
                INSERT IGNORE INTO users (user_id, registration_date, location_country, location_province, location_city, interests, demographics)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, users_data)
            
            # 插入示例新闻
            news_data = [
                ('N41340', '体育新闻：足球比赛精彩回顾', '昨晚的足球比赛异常精彩，双方球员表现出色...', 'sports', 'soccer', 150, '2024-01-15 08:00:00', '体育日报', '["足球", "比赛", "体育"]', '{"persons": ["梅西"], "organizations": ["巴塞罗那"], "locations": ["西班牙"]}'),
                ('N41341', '科技突破：AI技术新进展', '人工智能领域迎来重大突破，新算法效率提升...', 'technology', 'ai', 200, '2024-01-15 09:00:00', '科技日报', '["AI", "技术", "突破"]', '{"persons": ["李飞飞"], "organizations": ["谷歌"], "locations": ["硅谷"]}'),
                ('N41342', '经济动态：股市分析报告', '今日股市表现良好，科技股领涨...', 'business', 'finance', 180, '2024-01-15 10:00:00', '财经网', '["股市", "经济", "投资"]', '{"organizations": ["纳斯达克"], "locations": ["纽约"]}')
            ]
            
            cursor.executemany("""
                INSERT IGNORE INTO news_articles (news_id, headline, content, category, topic, word_count, publish_time, source, tags, entities)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, news_data)
            
            conn.commit()
            print("✅ 初始数据插入完成")
            
        finally:
            conn.close()
    
    def verify_setup(self):
        """验证设置"""
        print("🔍 验证数据库设置...")
        
        conn = mysql.connector.connect(**self.config)
        cursor = conn.cursor()
        
        try:
            # 检查表是否存在
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            
            expected_tables = [
                'news_articles', 'users', 'user_events', 'ai_analysis_results',
                'news_lifecycle', 'category_trends', 'user_interest_evolution',
                'multidim_statistics', 'viral_news_prediction', 'real_time_recommendations',
                'query_logs', 'performance_metrics', 'processing_batches'
            ]
            
            print(f"📋 数据库表列表 ({len(tables)} 个表):")
            for table in tables:
                status = "✅" if table in expected_tables else "❓"
                print(f"  {status} {table}")
            
            # 检查数据
            cursor.execute("SELECT COUNT(*) FROM users")
            user_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM news_articles")
            news_count = cursor.fetchone()[0]
            
            print(f"\n📊 初始数据统计:")
            print(f"  用户数量: {user_count}")
            print(f"  新闻数量: {news_count}")
            
            print("✅ 数据库验证完成")
            
        finally:
            conn.close()

def main():
    """主函数"""
    print("🎯 增强新闻分析系统 - 数据库初始化")
    print("="*50)
    
    # 获取数据库配置
    import getpass
    
    print("请输入数据库连接信息:")
    host = input("MySQL主机 (默认: localhost): ").strip() or 'localhost'
    user = input("MySQL用户名 (默认: analytics_user): ").strip() or 'analytics_user'
    password = getpass.getpass("MySQL密码 (默认: pass123): ") or 'pass123'
    database = input("数据库名 (默认: news_analytics): ").strip() or 'news_analytics'
    
    # 初始化数据库
    initializer = DatabaseInitializer(host, user, password, database)
    
    try:
        initializer.init_database()
        initializer.verify_setup()
        
        print("\n🎉 数据库初始化成功!")
        print("📝 接下来可以:")
        print("  1. 启动Kafka: bash scripts/start_kafka.sh")
        print("  2. 运行数据生成器: python scripts/run_enhanced_system.py")
        print("  3. 启动分析系统: python src/enhanced_spark_streaming_analyzer.py")
        
    except Exception as e:
        print(f"\n❌ 初始化失败: {e}")
        print("请检查MySQL连接配置和权限")

if __name__ == "__main__":
    main() 