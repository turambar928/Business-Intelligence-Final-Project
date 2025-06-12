#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PENS新闻数据导入脚本
将data/PENS/news.tsv中的所有新闻信息导入到news_articles表中
"""

import json
import re
import mysql.connector
from datetime import datetime, timedelta
import random
from typing import Dict, Any
import logging

class PENSNewsImporter:
    """PENS新闻数据导入器"""
    
    def __init__(self, mysql_config: Dict[str, Any]):
        self.mysql_config = mysql_config
        
        # 设置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # 新闻来源列表
        self.sources = [
            "CNN", "BBC", "Reuters", "Associated Press", "New York Times", 
            "Washington Post", "The Guardian", "Fox News", "ESPN", "TechCrunch",
            "Forbes", "Bloomberg", "Wall Street Journal", "USA Today", "NBC News"
        ]
    
    def connect_mysql(self):
        """连接MySQL数据库"""
        try:
            return mysql.connector.connect(**self.mysql_config)
        except Exception as e:
            self.logger.error(f"MySQL连接失败: {e}")
            return None
    
    def clean_text(self, text: str) -> str:
        """清理文本内容"""
        if not text or text.strip() == '':
            return ''
        
        # 移除特殊字符和多余空格
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        
        # 截断过长的文本
        if len(text) > 10000:
            text = text[:10000] + "..."
        
        return text
    
    def clean_headline(self, headline: str) -> str:
        """清理标题"""
        if not headline or headline.strip() == '':
            return 'Untitled News'
        
        headline = self.clean_text(headline)
        
        # 截断过长的标题
        if len(headline) > 500:
            headline = headline[:500] + "..."
            
        return headline
    
    def parse_entities(self, title_entity: str, entity_content: str) -> str:
        """解析实体信息并转换为JSON格式"""
        entities = {
            "persons": [],
            "organizations": [],
            "locations": []
        }
        
        try:
            if title_entity and title_entity.strip() and title_entity.strip() != '{}':
                # 简单的字符串解析，提取引号内的内容
                matches = re.findall(r'"([^"]*)"', title_entity)
                for match in matches:
                    if match and match.strip():
                        # 简单判断实体类型
                        if any(word in match.lower() for word in ['corp', 'inc', 'company', 'fc', 'united']):
                            entities["organizations"].append(match)
                        elif any(word in match.lower() for word in ['city', 'state', 'country', 'washington', 'atlanta']):
                            entities["locations"].append(match)
                        else:
                            entities["persons"].append(match)
            
        except Exception as e:
            self.logger.warning(f"实体解析失败: {e}")
        
        return json.dumps(entities, ensure_ascii=False)
    
    def generate_tags(self, headline: str, content: str, category: str, topic: str) -> str:
        """生成标签"""
        tags = set()
        
        # 添加分类和主题作为标签
        if category:
            tags.add(category)
        if topic:
            tags.add(topic)
            
        # 从标题中提取关键词作为标签
        headline_lower = headline.lower() if headline else ''
        content_lower = content.lower() if content else ''
        
        # 常见的新闻关键词
        keywords = [
            'breaking', 'news', 'update', 'report', 'analysis', 'exclusive',
            'football', 'soccer', 'basketball', 'baseball', 'tennis', 'olympics',
            'politics', 'election', 'government', 'policy', 'congress', 'senate',
            'technology', 'ai', 'tech', 'innovation', 'startup', 'digital',
            'business', 'economy', 'market', 'finance', 'stock', 'investment',
            'health', 'medical', 'healthcare', 'research', 'study', 'treatment',
            'entertainment', 'movie', 'music', 'celebrity', 'film', 'tv'
        ]
        
        for keyword in keywords:
            if keyword in headline_lower or keyword in content_lower:
                tags.add(keyword)
                if len(tags) >= 8:
                    break
        
        return json.dumps(list(tags)[:8], ensure_ascii=False)
    
    def generate_publish_time(self, index: int) -> str:
        """生成发布时间"""
        days_ago = random.randint(0, 30)
        hours_ago = random.randint(0, 23)
        minutes_ago = random.randint(0, 59)
        
        publish_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
        return publish_time.strftime('%Y-%m-%d %H:%M:%S')
    
    def count_words(self, text: str) -> int:
        """计算单词数"""
        if not text:
            return 0
        return len(text.split())
    
    def process_news_file(self, file_path: str, batch_size: int = 1000):
        """处理新闻文件并批量导入数据库"""
        conn = self.connect_mysql()
        if not conn:
            self.logger.error("无法连接数据库")
            return False
        
        cursor = conn.cursor()
        
        try:
            processed_count = 0
            batch_data = []
            
            self.logger.info(f"开始处理文件: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as file:
                # 跳过标题行
                header = file.readline()
                self.logger.info(f"文件头: {header.strip()}")
                
                for line_num, line in enumerate(file, 1):
                    try:
                        # 解析TSV行
                        parts = line.strip().split('\t')
                        
                        if len(parts) < 5:
                            continue
                        
                        news_id = parts[0].strip()
                        category = parts[1].strip()
                        topic = parts[2].strip()
                        headline = parts[3].strip()
                        content = parts[4].strip() if len(parts) > 4 else ''
                        title_entity = parts[5].strip() if len(parts) > 5 else '{}'
                        entity_content = parts[6].strip() if len(parts) > 6 else '{}'
                        
                        # 数据清理和处理
                        headline = self.clean_headline(headline)
                        content = self.clean_text(content)
                        
                        # 生成其他字段
                        word_count = self.count_words(content)
                        publish_time = self.generate_publish_time(line_num)
                        source = random.choice(self.sources)
                        tags = self.generate_tags(headline, content, category, topic)
                        entities = self.parse_entities(title_entity, entity_content)
                        
                        # 添加到批次数据
                        batch_data.append((
                            news_id, headline, content, category, topic,
                            word_count, publish_time, source, tags, entities
                        ))
                        
                        # 当批次数据达到指定大小时执行插入
                        if len(batch_data) >= batch_size:
                            self._insert_batch(cursor, batch_data)
                            conn.commit()
                            processed_count += len(batch_data)
                            self.logger.info(f"已处理 {processed_count} 条新闻记录")
                            batch_data = []
                        
                    except Exception as e:
                        self.logger.error(f"处理行 {line_num} 时出错: {e}")
                        continue
            
            # 处理剩余的数据
            if batch_data:
                self._insert_batch(cursor, batch_data)
                conn.commit()
                processed_count += len(batch_data)
            
            self.logger.info(f"数据导入完成！总共处理了 {processed_count} 条新闻记录")
            return True
            
        except Exception as e:
            self.logger.error(f"文件处理失败: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
    
    def _insert_batch(self, cursor, batch_data):
        """批量插入数据"""
        insert_sql = """
        INSERT IGNORE INTO news_articles 
        (news_id, headline, content, category, topic, word_count, publish_time, source, tags, entities)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_sql, batch_data)
    
    def verify_import(self):
        """验证导入结果"""
        conn = self.connect_mysql()
        if not conn:
            return
        
        cursor = conn.cursor()
        
        try:
            # 统计总数
            cursor.execute("SELECT COUNT(*) FROM news_articles")
            total_count = cursor.fetchone()[0]
            
            # 按分类统计
            cursor.execute("""
                SELECT category, COUNT(*) as count 
                FROM news_articles 
                GROUP BY category 
                ORDER BY count DESC
            """)
            category_stats = cursor.fetchall()
            
            # 按主题统计（前10个）
            cursor.execute("""
                SELECT topic, COUNT(*) as count 
                FROM news_articles 
                GROUP BY topic 
                ORDER BY count DESC 
                LIMIT 10
            """)
            topic_stats = cursor.fetchall()
            
            print(f"\n📊 导入验证结果:")
            print(f"总新闻数量: {total_count}")
            
            print(f"\n📈 分类统计:")
            for category, count in category_stats:
                print(f"  {category}: {count}")
            
            print(f"\n🔥 热门主题 (前10):")
            for topic, count in topic_stats:
                print(f"  {topic}: {count}")
                
        except Exception as e:
            self.logger.error(f"验证失败: {e}")
        finally:
            cursor.close()
            conn.close()

def main():
    """主函数"""
    print("🎯 PENS新闻数据导入工具")
    print("="*50)
    
    # 数据库配置
    mysql_config = {
        'host': 'localhost',
        'user': 'analytics_user',
        'password': 'pass123',
        'database': 'news_analytics',
        'charset': 'utf8mb4'
    }
    
    # 创建导入器
    importer = PENSNewsImporter(mysql_config)
    
    # 导入数据
    file_path = "data/PENS/news.tsv"
    
    print(f"📁 准备导入文件: {file_path}")
    print("⚠️  这可能需要几分钟时间，请耐心等待...")
    
    success = importer.process_news_file(file_path, batch_size=500)
    
    if success:
        print("\n✅ 数据导入成功！")
        print("🔍 验证导入结果...")
        importer.verify_import()
    else:
        print("\n❌ 数据导入失败！")

if __name__ == "__main__":
    main() 