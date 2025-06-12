#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的数据生成器
生成符合新日志格式的测试数据，支持七大分析功能
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd
import numpy as np

class EnhancedDataGenerator:
    """增强的数据生成器"""
    
    def __init__(self, data_path: str = "data/"):
        self.data_path = data_path
        
        # 从PENS数据集加载真实数据
        self.load_real_data()
        
        # 预定义数据
        self.categories = ["sports", "news", "lifestyle", "entertainment", "technology", "business", "health"]
        self.topics = {
            "sports": ["soccer", "basketball", "tennis", "olympics", "football"],
            "news": ["politics", "economy", "international", "domestic", "breaking"],
            "lifestyle": ["food", "travel", "fashion", "home", "relationships"],
            "entertainment": ["movies", "music", "celebrity", "tv", "games"],
            "technology": ["ai", "mobile", "software", "hardware", "startups"],
            "business": ["finance", "markets", "companies", "economy", "investing"],
            "health": ["medicine", "fitness", "nutrition", "mental", "research"]
        }
        
        self.devices = ["mobile", "desktop", "tablet"]
        self.os_types = ["iOS", "Android", "Windows", "Mac", "Linux"]
        self.browsers = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
        
        self.countries = ["China", "USA", "UK", "Germany", "Japan", "France", "Canada"]
        self.china_provinces = ["Beijing", "Shanghai", "Guangdong", "Zhejiang", "Jiangsu", "Sichuan"]
        self.cities = {
            "Beijing": ["Beijing"],
            "Shanghai": ["Shanghai"], 
            "Guangdong": ["Guangzhou", "Shenzhen", "Dongguan"],
            "Zhejiang": ["Hangzhou", "Ningbo", "Wenzhou"],
            "Jiangsu": ["Nanjing", "Suzhou", "Wuxi"],
            "Sichuan": ["Chengdu", "Mianyang"]
        }
        
        self.sources = ["新华社", "人民日报", "央视新闻", "财经网", "科技日报", "体育周报", "娱乐八卦"]
        
        # 实体数据
        self.persons = ["习近平", "李克强", "马云", "马化腾", "雷军", "刘强东", "张一鸣", "丁磊"]
        self.organizations = ["阿里巴巴", "腾讯", "百度", "字节跳动", "小米", "华为", "京东", "美团"]
        self.locations = ["北京", "上海", "深圳", "杭州", "广州", "成都", "武汉", "西安"]
        
    def load_real_data(self):
        """加载PENS真实数据"""
        try:
            # 加载新闻数据样本
            self.real_news = []
            with open(f"{self.data_path}PENS/news.tsv", 'r', encoding='utf-8') as f:
                lines = f.readlines()[:1000]  # 只读取前1000行
                for line in lines[1:]:  # 跳过头部
                    parts = line.strip().split('\t')
                    if len(parts) >= 4:
                        self.real_news.append({
                            'news_id': parts[0],
                            'category': parts[1],
                            'topic': parts[2],
                            'headline': parts[3],
                            'content': parts[4] if len(parts) > 4 else ""
                        })
            
            # 加载用户数据样本
            self.real_users = []
            with open(f"{self.data_path}PENS/train.tsv", 'r', encoding='utf-8') as f:
                lines = f.readlines()[:500]  # 只读取前500行
                for line in lines[1:]:
                    parts = line.strip().split('\t')
                    if len(parts) >= 2:
                        self.real_users.append({
                            'user_id': parts[0],
                            'clicked_news': parts[1].split() if len(parts) > 1 else []
                        })
                        
        except Exception as e:
            print(f"加载真实数据失败: {e}")
            # 使用默认数据
            self.real_news = []
            self.real_users = []
    
    def generate_enhanced_event(self) -> Dict[str, Any]:
        """生成增强的事件数据"""
        # 基础信息
        timestamp = datetime.now() - timedelta(
            seconds=random.randint(0, 3600),
            microseconds=random.randint(0, 999999)
        )
        
        user_id = self._get_user_id()
        news_id = self._get_news_id()
        action_type = random.choices(
            ["read", "skip", "share", "like", "comment", "click"],
            weights=[40, 30, 10, 15, 3, 2]
        )[0]
        
        # 设备信息
        device_type = random.choice(self.devices)
        device_info = {
            "device_type": device_type,
            "os": self._get_compatible_os(device_type),
            "browser": random.choice(self.browsers)
        }
        
        # 位置信息
        location = self._generate_location()
        
        # 新闻详情
        news_details = self._generate_news_details(news_id)
        
        # 用户上下文
        user_context = self._generate_user_context(user_id, news_details)
        
        # 交互数据
        interaction_data = self._generate_interaction_data(action_type)
        
        # 停留时间（基于动作类型）
        dwell_time = self._calculate_dwell_time(action_type, news_details.get('word_count', 0))
        
        event = {
            "timestamp": timestamp.isoformat(),
            "user_id": user_id,
            "news_id": news_id,
            "action_type": action_type,
            "session_id": f"session_{random.randint(100000, 999999)}",
            "dwell_time": dwell_time,
            "device_info": device_info,
            "location": location,
            "news_details": news_details,
            "user_context": user_context,
            "interaction_data": interaction_data
        }
        
        return event
    
    def _get_user_id(self) -> str:
        """获取用户ID"""
        if self.real_users and random.random() < 0.7:
            return random.choice(self.real_users)['user_id']
        else:
            return f"U{random.randint(100000, 999999)}"
    
    def _get_news_id(self) -> str:
        """获取新闻ID"""
        if self.real_news and random.random() < 0.6:
            return random.choice(self.real_news)['news_id']
        else:
            return f"N{random.randint(10000, 99999)}"
    
    def _get_compatible_os(self, device_type: str) -> str:
        """根据设备类型获取兼容的操作系统"""
        if device_type == "mobile":
            return random.choice(["iOS", "Android"])
        elif device_type == "desktop":
            return random.choice(["Windows", "Mac", "Linux"])
        else:  # tablet
            return random.choice(["iOS", "Android", "Windows"])
    
    def _generate_location(self) -> Dict[str, str]:
        """生成位置信息"""
        country = random.choice(self.countries)
        
        if country == "China":
            province = random.choice(self.china_provinces)
            city = random.choice(self.cities.get(province, [province]))
            return {
                "country": country,
                "province": province,
                "city": city
            }
        else:
            return {
                "country": country,
                "province": "",
                "city": ""
            }
    
    def _generate_news_details(self, news_id: str) -> Dict[str, Any]:
        """生成新闻详情"""
        # 尝试从真实数据获取
        real_news = None
        if self.real_news:
            real_news = next((n for n in self.real_news if n['news_id'] == news_id), None)
        
        if real_news:
            category = real_news.get('category', random.choice(self.categories))
            topic = real_news.get('topic', random.choice(self.topics.get(category, ['general'])))
            headline = real_news.get('headline', self._generate_headline(category, topic))
            content = real_news.get('content', self._generate_content(category, topic))
        else:
            category = random.choice(self.categories)
            topic = random.choice(self.topics.get(category, ['general']))
            headline = self._generate_headline(category, topic)
            content = self._generate_content(category, topic)
        
        # 生成标签
        tags = self._generate_tags(category, topic)
        
        # 生成实体
        entities = self._generate_entities(content)
        
        # 发布时间
        publish_time = (datetime.now() - timedelta(
            hours=random.randint(0, 72),
            minutes=random.randint(0, 59)
        )).isoformat()
        
        return {
            "category": category,
            "topic": topic,
            "headline": headline,
            "content": content,
            "word_count": len(content.split()) if content else 0,
            "publish_time": publish_time,
            "source": random.choice(self.sources),
            "tags": tags,
            "entities": entities
        }
    
    def _generate_headline(self, category: str, topic: str) -> str:
        """生成新闻标题"""
        headlines = {
            "sports": [
                f"{topic}比赛精彩回顾：精彩瞬间不容错过",
                f"最新{topic}赛事分析：专家预测下轮走势",
                f"{topic}明星球员表现亮眼，粉丝期待更多"
            ],
            "technology": [
                f"人工智能在{topic}领域的最新突破",
                f"{topic}技术革新：改变未来生活方式",
                f"科技巨头在{topic}领域的激烈竞争"
            ],
            "news": [
                f"重要{topic}消息：政策调整影响深远",
                f"{topic}领域新动态：专家深度解读",
                f"关注{topic}发展：未来趋势分析"
            ]
        }
        
        default_headlines = [
            f"{category}领域最新动态报道",
            f"{topic}相关重要消息发布", 
            f"深度解析{category}行业趋势"
        ]
        
        category_headlines = headlines.get(category, default_headlines)
        return random.choice(category_headlines)
    
    def _generate_content(self, category: str, topic: str) -> str:
        """生成新闻内容"""
        content_templates = [
            f"据最新报道，{topic}领域出现了重要进展。专家表示，这一变化将对整个{category}行业产生深远影响。相关数据显示，用户对此类内容的关注度持续上升，市场反应积极。业内人士认为，未来几个月内将有更多相关政策出台，值得密切关注。",
            f"在{category}行业中，{topic}一直是热门话题。最新研究表明，技术创新正在推动该领域快速发展。行业领导者指出，通过持续投资和创新，可以为用户提供更好的体验。分析师预测，这一趋势将在未来继续保持强劲势头。",
            f"近期{topic}相关消息引起广泛关注。据了解，多家知名企业已经开始布局相关业务，市场竞争日趋激烈。用户需求的不断变化推动着{category}行业的转型升级。专家建议，企业应该抓住机遇，加快创新步伐。"
        ]
        
        return random.choice(content_templates)
    
    def _generate_tags(self, category: str, topic: str) -> List[str]:
        """生成标签"""
        base_tags = [category, topic]
        
        additional_tags = {
            "sports": ["比赛", "球员", "赛事", "体育"],
            "technology": ["创新", "科技", "数字化", "未来"],
            "news": ["时事", "政策", "社会", "热点"],
            "business": ["经济", "市场", "企业", "投资"],
            "entertainment": ["娱乐", "明星", "影视", "音乐"]
        }
        
        extra_tags = additional_tags.get(category, ["热门", "资讯"])
        tags = base_tags + random.sample(extra_tags, random.randint(1, 3))
        
        return list(set(tags))  # 去重
    
    def _generate_entities(self, content: str) -> Dict[str, List[str]]:
        """生成实体信息"""
        return {
            "persons": random.sample(self.persons, random.randint(0, 3)),
            "organizations": random.sample(self.organizations, random.randint(0, 2)),
            "locations": random.sample(self.locations, random.randint(0, 2))
        }
    
    def _generate_user_context(self, user_id: str, news_details: Dict) -> Dict[str, Any]:
        """生成用户上下文"""
        # 获取用户历史文章
        previous_articles = [f"N{random.randint(10000, 99999)}" for _ in range(random.randint(0, 5))]
        
        # 确定阅读时间段
        hour = datetime.now().hour
        if 6 <= hour < 12:
            reading_time = "morning"
        elif 12 <= hour < 18:
            reading_time = "afternoon" 
        elif 18 <= hour < 23:
            reading_time = "evening"
        else:
            reading_time = "night"
        
        # 判断是否周末
        is_weekend = datetime.now().weekday() >= 5
        
        # 用户兴趣（基于新闻类别生成相关兴趣）
        category = news_details.get('category', 'general')
        base_interests = [category]
        other_interests = random.sample(self.categories, random.randint(1, 3))
        user_interests = list(set(base_interests + other_interests))
        
        # 参与度分数（基于用户行为模式）
        engagement_score = max(0, min(1, random.gauss(0.6, 0.2)))
        
        return {
            "previous_articles": previous_articles,
            "reading_time_of_day": reading_time,
            "is_weekend": is_weekend,
            "user_interests": user_interests,
            "engagement_score": round(engagement_score, 2)
        }
    
    def _generate_interaction_data(self, action_type: str) -> Dict[str, Any]:
        """生成交互数据"""
        if action_type == "read":
            scroll_depth = max(0.1, min(1.0, random.gauss(0.7, 0.2)))
            clicks_count = random.randint(0, 5)
        elif action_type == "skip":
            scroll_depth = max(0.0, min(0.3, random.gauss(0.1, 0.1)))
            clicks_count = 0
        else:
            scroll_depth = max(0.3, min(1.0, random.gauss(0.8, 0.1)))
            clicks_count = random.randint(1, 3)
        
        return {
            "scroll_depth": round(scroll_depth, 2),
            "clicks_count": clicks_count,
            "shares_count": 1 if action_type == "share" else 0,
            "comments_count": 1 if action_type == "comment" else 0,
            "likes_count": 1 if action_type == "like" else 0
        }
    
    def _calculate_dwell_time(self, action_type: str, word_count: int) -> int:
        """计算停留时间"""
        base_time = {
            "read": max(30, word_count // 10),  # 基于字数的阅读时间
            "skip": random.randint(1, 10),
            "share": random.randint(20, 60),
            "like": random.randint(10, 30),
            "comment": random.randint(60, 180),
            "click": random.randint(5, 20)
        }
        
        base = base_time.get(action_type, 30)
        # 添加随机波动
        variation = int(base * random.gauss(0, 0.3))
        return max(1, base + variation)
    
    def generate_batch_events(self, count: int = 10) -> List[Dict[str, Any]]:
        """生成一批事件"""
        return [self.generate_enhanced_event() for _ in range(count)]
    
    def generate_realistic_session(self, user_id: str = None, session_length: int = None) -> List[Dict[str, Any]]:
        """生成真实的用户会话"""
        if not user_id:
            user_id = self._get_user_id()
        
        if not session_length:
            session_length = random.randint(3, 15)
        
        session_id = f"session_{random.randint(100000, 999999)}"
        session_start = datetime.now() - timedelta(minutes=random.randint(0, 60))
        
        events = []
        current_time = session_start
        
        # 用户在会话中的兴趣可能会集中在某些类别
        preferred_category = random.choice(self.categories)
        
        for i in range(session_length):
            # 时间递增
            current_time += timedelta(seconds=random.randint(10, 120))
            
            event = self.generate_enhanced_event()
            
            # 覆盖用户ID和会话ID
            event['user_id'] = user_id
            event['session_id'] = session_id
            event['timestamp'] = current_time.isoformat()
            
            # 增加对首选类别的倾向
            if random.random() < 0.4:
                event['news_details']['category'] = preferred_category
                event['news_details']['topic'] = random.choice(
                    self.topics.get(preferred_category, ['general'])
                )
            
            events.append(event)
        
        return events

def main():
    """测试数据生成器"""
    generator = EnhancedDataGenerator()
    
    print("生成单个事件:")
    event = generator.generate_enhanced_event()
    print(json.dumps(event, indent=2, ensure_ascii=False))
    
    print("\n生成用户会话:")
    session = generator.generate_realistic_session()
    print(f"会话包含 {len(session)} 个事件")
    
    print("\n生成批次事件:")
    batch = generator.generate_batch_events(5)
    print(f"批次包含 {len(batch)} 个事件")

if __name__ == "__main__":
    main() 