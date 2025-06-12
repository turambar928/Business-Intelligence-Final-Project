#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版实时日志生成器测试
"""

import json
import os
from datetime import datetime
import random

def test_log_generation():
    """测试日志生成"""
    print("🎯 测试实时新闻日志生成器")
    print("="*50)
    
    # 创建输出目录
    output_dir = "web_logs"
    os.makedirs(output_dir, exist_ok=True)
    
    # 模拟生成一条日志
    test_log = {
        "timestamp": datetime.now().isoformat() + 'Z',
        "user_id": "U335175",
        "news_id": "N41340", 
        "action_type": "read",
        "session_id": "session_123456",
        "dwell_time": 116,
        "device_info": {
            "device_type": "mobile",
            "os": "iOS",
            "browser": "Safari"
        },
        "location": {
            "country": "China",
            "province": "Beijing",
            "city": "Beijing"
        },
        "news_details": {
            "category": "sports",
            "topic": "soccer",
            "headline": "测试新闻标题",
            "content": "这是一条测试新闻内容",
            "word_count": 100,
            "publish_time": datetime.now().isoformat() + 'Z',
            "source": "test_source",
            "tags": ["体育", "足球"],
            "entities": {
                "persons": [],
                "organizations": [],
                "locations": []
            }
        },
        "user_context": {
            "previous_articles": ["N41339", "N41338"],
            "reading_time_of_day": "afternoon",
            "is_weekend": False,
            "user_interests": ["体育", "科技"],
            "engagement_score": 0.75
        },
        "interaction_data": {
            "scroll_depth": 0.8,
            "clicks_count": 3,
            "shares_count": 1,
            "comments_count": 0,
            "likes_count": 1
        },
        "ai_analysis": {
            "sentiment_score": 0.65,
            "topic_keywords": ["足球", "比赛"],
            "viral_potential": 0.72,
            "trend_score": 0.58,
            "recommendation_relevance": 0.83,
            "content_quality": 0.77,
            "user_profile_match": 0.91
        }
    }
    
    # 保存测试日志
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"test_news_logs_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(test_log, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 测试日志已生成: {filepath}")
        
        # 显示文件内容
        print("\n📄 生成的日志内容:")
        print(json.dumps(test_log, ensure_ascii=False, indent=2))
        
        return True
        
    except Exception as e:
        print(f"❌ 生成测试日志失败: {e}")
        return False

if __name__ == "__main__":
    test_log_generation() 