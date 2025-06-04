-- 新闻实时分析系统数据库初始化脚本
-- 创建数据库
CREATE DATABASE IF NOT EXISTS news_analytics CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE news_analytics;

-- 用户行为分析表
CREATE TABLE IF NOT EXISTS user_behavior_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    avg_click_rate DECIMAL(5,4) DEFAULT 0,
    active_users INT DEFAULT 0,
    total_impressions INT DEFAULT 0,
    total_clicks INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_batch_timestamp (batch_id, timestamp),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 新闻热度分析表
CREATE TABLE IF NOT EXISTS news_popularity_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    clicks INT DEFAULT 0,
    impressions INT DEFAULT 0,
    ctr DECIMAL(5,4) DEFAULT 0,
    popularity_score DECIMAL(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_news_batch (news_id, batch_id),
    INDEX idx_news_id (news_id),
    INDEX idx_popularity_score (popularity_score DESC),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 趋势分析表
CREATE TABLE IF NOT EXISTS trending_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    trend_score DECIMAL(10,2) DEFAULT 0,
    category VARCHAR(50),
    keywords TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_news_id (news_id),
    INDEX idx_trend_score (trend_score DESC),
    INDEX idx_timestamp (timestamp),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 情感分析表
CREATE TABLE IF NOT EXISTS sentiment_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    sentiment_score DECIMAL(3,2) DEFAULT 0, -- -1到1之间
    sentiment_label VARCHAR(20), -- positive, negative, neutral
    confidence DECIMAL(3,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_news_id (news_id),
    INDEX idx_sentiment_score (sentiment_score),
    INDEX idx_sentiment_label (sentiment_label),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 推荐结果表
CREATE TABLE IF NOT EXISTS recommendation_results (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    rank_score DECIMAL(5,4) DEFAULT 0,
    recommendation_type VARCHAR(50) DEFAULT 'collaborative_filtering',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_news_id (news_id),
    INDEX idx_rank_score (rank_score DESC),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 新闻基础信息表
CREATE TABLE IF NOT EXISTS news_info (
    news_id VARCHAR(100) PRIMARY KEY,
    title TEXT,
    content LONGTEXT,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    abstract TEXT,
    title_entities TEXT,
    created_time DATETIME,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_created_time (created_time),
    FULLTEXT KEY ft_title (title),
    FULLTEXT KEY ft_content (content)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户基础信息表
CREATE TABLE IF NOT EXISTS user_info (
    user_id VARCHAR(100) PRIMARY KEY,
    registration_time DATETIME,
    last_active_time DATETIME,
    total_clicks INT DEFAULT 0,
    total_impressions INT DEFAULT 0,
    favorite_categories TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_last_active (last_active_time),
    INDEX idx_total_clicks (total_clicks)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 实时统计汇总表
CREATE TABLE IF NOT EXISTS real_time_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stat_time DATETIME NOT NULL,
    stat_type VARCHAR(50) NOT NULL, -- hourly, daily, etc.
    total_users INT DEFAULT 0,
    total_impressions INT DEFAULT 0,
    total_clicks INT DEFAULT 0,
    avg_ctr DECIMAL(5,4) DEFAULT 0,
    top_news JSON,
    trending_categories JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_stat (stat_time, stat_type),
    INDEX idx_stat_time (stat_time),
    INDEX idx_stat_type (stat_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 查询日志表（用于性能监控）
CREATE TABLE IF NOT EXISTS query_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    query_id VARCHAR(100) NOT NULL,
    query_type VARCHAR(50) NOT NULL,
    query_sql TEXT,
    execution_time_ms INT DEFAULT 0,
    result_count INT DEFAULT 0,
    user_id VARCHAR(100),
    query_params JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_query_type (query_type),
    INDEX idx_execution_time (execution_time_ms),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建存储过程：获取热门新闻
DELIMITER $$
CREATE PROCEDURE GetTrendingNews(
    IN time_window INT DEFAULT 3600,
    IN limit_count INT DEFAULT 20
)
BEGIN
    SELECT 
        n.news_id,
        n.title,
        n.category,
        SUM(p.clicks) as total_clicks,
        SUM(p.impressions) as total_impressions,
        AVG(p.ctr) as avg_ctr,
        MAX(t.trend_score) as max_trend_score
    FROM news_popularity_analytics p
    JOIN news_info n ON p.news_id = n.news_id
    LEFT JOIN trending_analytics t ON p.news_id = t.news_id
    WHERE p.timestamp >= DATE_SUB(NOW(), INTERVAL time_window SECOND)
    GROUP BY n.news_id, n.title, n.category
    ORDER BY max_trend_score DESC, total_clicks DESC
    LIMIT limit_count;
END$$

-- 创建存储过程：获取用户推荐
CREATE PROCEDURE GetUserRecommendations(
    IN target_user_id VARCHAR(100),
    IN limit_count INT DEFAULT 10
)
BEGIN
    SELECT 
        r.news_id,
        n.title,
        n.category,
        r.rank_score,
        n.abstract
    FROM recommendation_results r
    JOIN news_info n ON r.news_id = n.news_id
    WHERE r.user_id = target_user_id
    AND r.timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    ORDER BY r.rank_score DESC
    LIMIT limit_count;
END$$

-- 创建存储过程：获取实时统计
CREATE PROCEDURE GetRealTimeStats(
    IN stat_period VARCHAR(20) DEFAULT 'hourly'
)
BEGIN
    SELECT 
        stat_time,
        total_users,
        total_impressions,
        total_clicks,
        avg_ctr,
        top_news,
        trending_categories
    FROM real_time_stats
    WHERE stat_type = stat_period
    AND stat_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
    ORDER BY stat_time DESC;
END$$

DELIMITER ;

-- 插入示例数据
INSERT INTO news_info (news_id, title, category, subcategory, abstract, created_time) VALUES
('N1001', 'Technology Innovation Trends in 2024', 'tech', 'innovation', 'Latest trends in technology and innovation for the upcoming year', NOW()),
('N1002', 'Global Economic Outlook', 'finance', 'economy', 'Analysis of global economic trends and predictions', NOW()),
('N1003', 'Sports Championship Results', 'sports', 'championship', 'Latest results from major sports championships', NOW()),
('N1004', 'Health and Wellness Tips', 'health', 'wellness', 'Expert advice on maintaining health and wellness', NOW()),
('N1005', 'Entertainment Industry News', 'entertainment', 'movies', 'Latest updates from the entertainment world', NOW());

-- 创建索引优化查询性能
CREATE INDEX idx_user_behavior_composite ON user_behavior_analytics(timestamp, avg_click_rate);
CREATE INDEX idx_news_popularity_composite ON news_popularity_analytics(timestamp, popularity_score, news_id);
CREATE INDEX idx_trending_composite ON trending_analytics(timestamp, trend_score, category);

-- 创建视图：热门新闻实时视图
CREATE VIEW v_trending_news_realtime AS
SELECT 
    n.news_id,
    n.title,
    n.category,
    p.clicks,
    p.impressions,
    p.ctr,
    t.trend_score,
    p.timestamp as last_updated
FROM news_popularity_analytics p
JOIN news_info n ON p.news_id = n.news_id
LEFT JOIN trending_analytics t ON p.news_id = t.news_id AND p.batch_id = t.batch_id
WHERE p.timestamp >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
ORDER BY t.trend_score DESC, p.clicks DESC;

-- 创建视图：用户活跃度统计
CREATE VIEW v_user_activity_stats AS
SELECT 
    DATE(timestamp) as stat_date,
    AVG(avg_click_rate) as daily_avg_ctr,
    SUM(active_users) as total_active_users,
    SUM(total_impressions) as daily_impressions,
    SUM(total_clicks) as daily_clicks
FROM user_behavior_analytics
WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY DATE(timestamp)
ORDER BY stat_date DESC; 