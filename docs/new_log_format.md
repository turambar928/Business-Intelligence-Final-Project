# 新闻实时分析系统 - 日志格式规范

## 基础日志格式
```json
{
    "timestamp": "2024-01-15T10:30:45.123Z",
    "user_id": "U335175", 
    "news_id": "N41340",
    "action_type": "read|skip|share|like|comment|click",
    "session_id": "session_123456",
    "dwell_time": 116,  // 停留时间(秒)
    "device_info": {
        "device_type": "mobile|desktop|tablet",
        "os": "iOS|Android|Windows|Mac",
        "browser": "Chrome|Safari|Firefox"
    },
    "location": {
        "country": "China",
        "province": "Beijing", 
        "city": "Beijing"
    },
    "news_details": {
        "category": "sports|news|lifestyle|entertainment|technology",
        "topic": "soccer|politics|health|business",
        "headline": "新闻标题",
        "content": "新闻内容",
        "word_count": 500,
        "publish_time": "2024-01-15T08:00:00Z",
        "source": "news_source_name",
        "tags": ["体育", "足球", "比赛"],
        "entities": {
            "persons": ["梅西", "C罗"],
            "organizations": ["巴塞罗那", "皇马"],
            "locations": ["西班牙", "巴塞罗那"]
        }
    },
    "user_context": {
        "previous_articles": ["N41339", "N41338"], // 最近阅读的文章
        "reading_time_of_day": "morning|afternoon|evening|night",
        "is_weekend": true,
        "user_interests": ["体育", "科技", "娱乐"],
        "engagement_score": 0.75  // 用户参与度得分
    },
    "interaction_data": {
        "scroll_depth": 0.8,  // 滚动深度
        "clicks_count": 3,    // 点击次数
        "shares_count": 1,    // 分享次数
        "comments_count": 0,  // 评论次数
        "likes_count": 1      // 点赞次数
    }
}
```

## 扩展字段（用于AI分析）
```json
{
    "ai_analysis": {
        "sentiment_score": 0.65,      // 情感分析得分 (-1到1)
        "topic_keywords": ["足球", "比赛", "胜利"],
        "viral_potential": 0.72,      // 病毒传播潜力 (0到1)
        "trend_score": 0.58,          // 趋势得分
        "recommendation_relevance": 0.83, // 推荐相关性
        "content_quality": 0.77,      // 内容质量得分
        "user_profile_match": 0.91    // 用户画像匹配度
    }
}
```

## 批量处理格式
对于Kafka批量数据，每条消息包含：
```json
{
    "batch_id": "batch_20240115_103045",
    "events": [
        // 多个单条日志记录
    ],
    "metadata": {
        "source": "news_app|web_portal|mobile_app",
        "version": "1.0",
        "processed_time": "2024-01-15T10:30:45.123Z"
    }
}
``` 