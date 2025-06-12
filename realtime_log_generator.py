#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实时新闻日志生成器
从PENS数据集读取用户行为数据，生成符合新日志格式的实时日志文件
"""

import json
import time
import random
import mysql.connector
from datetime import datetime, timedelta
import threading
import os
import logging
from typing import Dict, List, Any
import uuid

class RealTimeLogGenerator:
    """实时日志生成器"""
    
    def __init__(self, mysql_config: Dict[str, Any], output_dir: str = "web_logs"):
        self.mysql_config = mysql_config
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        
        # 设置日志
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 用户会话缓存
        self.user_sessions = {}
        self.user_interests = {}
        
        # PENS数据缓存
        self.pens_data = []
        self.current_index = 0
        
        # 模拟数据生成参数
        self.action_types = ["read", "skip", "share", "like", "comment", "click"]
        self.device_types = ["mobile", "desktop", "tablet"]
        self.os_types = ["iOS", "Android", "Windows", "Mac"]
        self.browsers = ["Chrome", "Safari", "Firefox", "Edge"]
        self.sources = ["news_app", "web_portal", "mobile_app"]
        
        # 中国省市数据
        self.locations = [
            {"country": "China", "province": "Beijing", "city": "Beijing"},
            {"country": "China", "province": "Shanghai", "city": "Shanghai"},
            {"country": "China", "province": "Guangdong", "city": "Guangzhou"},
            {"country": "China", "province": "Guangdong", "city": "Shenzhen"},
            {"country": "China", "province": "Zhejiang", "city": "Hangzhou"},
            {"country": "China", "province": "Jiangsu", "city": "Nanjing"},
            {"country": "China", "province": "Sichuan", "city": "Chengdu"},
            {"country": "China", "province": "Hubei", "city": "Wuhan"},
            {"country": "China", "province": "Shaanxi", "city": "Xi'an"},
            {"country": "China", "province": "Tianjin", "city": "Tianjin"}
        ]
        
        # 用户兴趣标签
        self.interest_categories = {
            "sports": ["体育", "足球", "篮球", "网球", "奥运会"],
            "news": ["新闻", "政治", "社会", "国际"],
            "technology": ["科技", "AI", "互联网", "手机", "电脑"],
            "business": ["商业", "经济", "金融", "股票", "投资"],
            "entertainment": ["娱乐", "电影", "音乐", "明星", "电视"],
            "lifestyle": ["生活", "健康", "美食", "旅行", "时尚"],
            "finance": ["财经", "理财", "银行", "保险"]
        }
        
    def connect_mysql(self):
        """连接MySQL数据库"""
        try:
            return mysql.connector.connect(**self.mysql_config)
        except Exception as e:
            self.logger.error(f"MySQL连接失败: {e}")
            return None
    
    def load_pens_data(self, file_path: str, max_records: int = 10000):
        """加载PENS数据集"""
        self.logger.info(f"开始加载PENS数据: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                # 跳过标题行
                header = file.readline().strip().split('\t')
                self.logger.info(f"数据列: {header}")
                
                count = 0
                for line in file:
                    if count >= max_records:
                        break
                        
                    parts = line.strip().split('\t')
                    if len(parts) >= 4:
                        user_id = parts[0]
                        clicked_news = parts[1].split() if len(parts) > 1 else []
                        dwell_times = list(map(int, parts[2].split())) if len(parts) > 2 and parts[2] else []
                        exposure_times = parts[3] if len(parts) > 3 else ""
                        
                        # 解析曝光时间
                        time_stamps = []
                        if exposure_times:
                            time_parts = exposure_times.split('#TAB#')
                            for time_part in time_parts:
                                try:
                                    time_stamps.append(datetime.strptime(time_part.strip(), '%m/%d/%Y %I:%M:%S %p'))
                                except:
                                    continue
                        
                        self.pens_data.append({
                            'user_id': user_id,
                            'clicked_news': clicked_news,
                            'dwell_times': dwell_times,
                            'time_stamps': time_stamps
                        })
                        count += 1
                        
            self.logger.info(f"成功加载 {len(self.pens_data)} 条PENS记录")
            
        except Exception as e:
            self.logger.error(f"加载PENS数据失败: {e}")
    
    def get_news_details(self, news_id: str) -> Dict[str, Any]:
        """从数据库获取新闻详情"""
        conn = self.connect_mysql()
        if not conn:
            return self._generate_mock_news_details(news_id)
        
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT * FROM news_articles WHERE news_id = %s LIMIT 1
            """, (news_id,))
            
            result = cursor.fetchone()
            if result:
                return {
                    "category": result.get('category', 'news'),
                    "topic": result.get('topic', 'general'),
                    "headline": result.get('headline', 'Sample News Headline'),
                    "content": result.get('content', 'Sample news content...'),
                    "word_count": result.get('word_count', 100),
                    "publish_time": result.get('publish_time', datetime.now()).isoformat() + 'Z',
                    "source": result.get('source', 'news_source'),
                    "tags": json.loads(result.get('tags', '[]')),
                    "entities": json.loads(result.get('entities', '{}'))
                }
            else:
                return self._generate_mock_news_details(news_id)
                
        except Exception as e:
            self.logger.warning(f"获取新闻详情失败: {e}")
            return self._generate_mock_news_details(news_id)
        finally:
            if conn:
                cursor.close()
                conn.close()
    
    def _generate_mock_news_details(self, news_id: str) -> Dict[str, Any]:
        """生成模拟新闻详情"""
        categories = list(self.interest_categories.keys())
        category = random.choice(categories)
        
        return {
            "category": category,
            "topic": random.choice(["breaking", "analysis", "report", "update"]),
            "headline": f"Sample News Headline for {news_id}",
            "content": "This is a sample news content for simulation purposes.",
            "word_count": random.randint(100, 1000),
            "publish_time": (datetime.now() - timedelta(hours=random.randint(1, 24))).isoformat() + 'Z',
            "source": random.choice(["CNN", "BBC", "Reuters", "AP"]),
            "tags": random.sample(self.interest_categories[category], k=min(3, len(self.interest_categories[category]))),
            "entities": {
                "persons": [],
                "organizations": [],
                "locations": []
            }
        }
    
    def get_user_session(self, user_id: str) -> str:
        """获取或生成用户会话ID"""
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = f"session_{uuid.uuid4().hex[:12]}"
        return self.user_sessions[user_id]
    
    def get_user_interests(self, user_id: str) -> List[str]:
        """获取或生成用户兴趣"""
        if user_id not in self.user_interests:
            # 随机选择2-4个兴趣类别
            categories = random.sample(list(self.interest_categories.keys()), k=random.randint(2, 4))
            interests = []
            for category in categories:
                interests.extend(random.sample(self.interest_categories[category], k=random.randint(1, 2)))
            self.user_interests[user_id] = interests
        return self.user_interests[user_id]
    
    def get_time_of_day(self, timestamp: datetime) -> str:
        """根据时间判断时段"""
        hour = timestamp.hour
        if 5 <= hour < 12:
            return "morning"
        elif 12 <= hour < 18:
            return "afternoon"
        elif 18 <= hour < 22:
            return "evening"
        else:
            return "night"
    
    def generate_ai_analysis(self, news_details: Dict[str, Any], action_type: str, dwell_time: int) -> Dict[str, Any]:
        """生成AI分析数据"""
        return {
            "sentiment_score": round(random.uniform(-1.0, 1.0), 2),
            "topic_keywords": news_details.get("tags", [])[:3],
            "viral_potential": round(random.uniform(0.0, 1.0), 2),
            "trend_score": round(random.uniform(0.0, 1.0), 2),
            "recommendation_relevance": round(random.uniform(0.0, 1.0), 2),
            "content_quality": round(random.uniform(0.0, 1.0), 2),
            "user_profile_match": round(random.uniform(0.0, 1.0), 2)
        }
    
    def generate_log_entry(self, user_data: Dict[str, Any], news_index: int) -> Dict[str, Any]:
        """生成单条日志记录"""
        user_id = user_data['user_id']
        clicked_news = user_data['clicked_news']
        dwell_times = user_data['dwell_times']
        time_stamps = user_data['time_stamps']
        
        # 如果没有点击新闻，跳过
        if not clicked_news or news_index >= len(clicked_news):
            return None
        
        news_id = clicked_news[news_index]
        dwell_time = dwell_times[news_index] if news_index < len(dwell_times) else random.randint(10, 300)
        
        # 使用原始时间戳或生成新的
        if news_index < len(time_stamps):
            timestamp = time_stamps[news_index]
        else:
            timestamp = datetime.now()
        
        # 获取新闻详情
        news_details = self.get_news_details(news_id)
        
        # 生成日志记录
        action_type = random.choice(self.action_types)
        device_info = {
            "device_type": random.choice(self.device_types),
            "os": random.choice(self.os_types),
            "browser": random.choice(self.browsers)
        }
        
        log_entry = {
            "timestamp": timestamp.isoformat() + 'Z',
            "user_id": user_id,
            "news_id": news_id,
            "action_type": action_type,
            "session_id": self.get_user_session(user_id),
            "dwell_time": dwell_time,
            "device_info": device_info,
            "location": random.choice(self.locations),
            "news_details": news_details,
            "user_context": {
                "previous_articles": clicked_news[max(0, news_index-2):news_index],
                "reading_time_of_day": self.get_time_of_day(timestamp),
                "is_weekend": timestamp.weekday() >= 5,
                "user_interests": self.get_user_interests(user_id),
                "engagement_score": round(random.uniform(0.0, 1.0), 2)
            },
            "interaction_data": {
                "scroll_depth": round(random.uniform(0.1, 1.0), 2),
                "clicks_count": random.randint(0, 10),
                "shares_count": random.randint(0, 3),
                "comments_count": random.randint(0, 5),
                "likes_count": random.randint(0, 8)
            },
            "ai_analysis": self.generate_ai_analysis(news_details, action_type, dwell_time)
        }
        
        return log_entry
    
    def generate_batch_log(self, batch_size: int = 10) -> Dict[str, Any]:
        """生成批量日志"""
        events = []
        
        for _ in range(batch_size):
            if not self.pens_data:
                break
                
            # 循环使用PENS数据
            user_data = self.pens_data[self.current_index % len(self.pens_data)]
            
            # 随机选择该用户的一个新闻
            if user_data['clicked_news']:
                news_index = random.randint(0, len(user_data['clicked_news']) - 1)
                log_entry = self.generate_log_entry(user_data, news_index)
                if log_entry:
                    events.append(log_entry)
            
            self.current_index += 1
        
        batch_log = {
            "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}",
            "events": events,
            "metadata": {
                "source": random.choice(self.sources),
                "version": "1.0",
                "processed_time": datetime.now().isoformat() + 'Z'
            }
        }
        
        return batch_log
    
    def save_log_to_file(self, log_data: Dict[str, Any], filename: str = None):
        """保存日志到文件"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"news_logs_{timestamp}.json"
        
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"日志已保存到: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"保存日志文件失败: {e}")
            return None
    
    def start_realtime_generation(self, interval_seconds: int = 5, batch_size: int = 10):
        """开始实时生成日志"""
        self.logger.info(f"开始实时日志生成，间隔: {interval_seconds}秒，批次大小: {batch_size}")
        
        try:
            while True:
                # 生成批量日志
                batch_log = self.generate_batch_log(batch_size)
                
                if batch_log['events']:
                    # 保存到文件
                    self.save_log_to_file(batch_log)
                    self.logger.info(f"生成了 {len(batch_log['events'])} 条日志记录")
                else:
                    self.logger.warning("没有生成任何日志记录")
                
                # 等待指定间隔
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            self.logger.info("日志生成已停止")
        except Exception as e:
            self.logger.error(f"日志生成出错: {e}")

def main():
    """主函数"""
    print("🎯 实时新闻日志生成器")
    print("="*50)
    
    # 数据库配置
    mysql_config = {
        'host': 'localhost',
        'user': 'analytics_user',
        'password': 'pass123',
        'database': 'news_analytics',
        'charset': 'utf8mb4'
    }
    
    # 创建日志生成器
    generator = RealTimeLogGenerator(mysql_config, "web_logs")
    
    # 加载PENS数据
    print("📁 加载PENS数据集...")
    generator.load_pens_data("data/PENS/train.tsv", max_records=5000)  # 限制记录数以便测试
    
    if not generator.pens_data:
        print("❌ 没有加载到PENS数据，退出程序")
        return
    
    print(f"✅ 已加载 {len(generator.pens_data)} 条用户行为记录")
    
    # 获取用户输入
    try:
        interval = int(input("请输入生成间隔秒数 (默认5秒): ") or "5")
        batch_size = int(input("请输入每批次记录数 (默认10条): ") or "10")
    except ValueError:
        interval = 5
        batch_size = 10
    
    print(f"\n🚀 开始实时生成日志...")
    print(f"   间隔时间: {interval} 秒")
    print(f"   批次大小: {batch_size} 条")
    print(f"   输出目录: web_logs/")
    print("   按 Ctrl+C 停止生成")
    print("-" * 50)
    
    # 开始实时生成
    generator.start_realtime_generation(interval, batch_size)

if __name__ == "__main__":
    main()