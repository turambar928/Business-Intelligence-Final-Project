-- 新闻实时分析系统数据库初始化脚本 (简化兼容版本)
-- 创建数据库
CREATE DATABASE IF NOT EXISTS news_analytics CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE news_analytics;

-- 用户行为分析表
DROP TABLE IF EXISTS user_behavior_analytics;
CREATE TABLE user_behavior_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    avg_click_rate DECIMAL(5,4) DEFAULT 0,
    active_users INT DEFAULT 0,
    total_impressions INT DEFAULT 0,
    total_clicks INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 新闻热度分析表
DROP TABLE IF EXISTS news_popularity_analytics;
CREATE TABLE news_popularity_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    clicks INT DEFAULT 0,
    impressions INT DEFAULT 0,
    ctr DECIMAL(5,4) DEFAULT 0,
    popularity_score DECIMAL(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 趋势分析表
DROP TABLE IF EXISTS trending_analytics;
CREATE TABLE trending_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    trend_score DECIMAL(10,2) DEFAULT 0,
    category VARCHAR(50),
    keywords TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 情感分析表
DROP TABLE IF EXISTS sentiment_analytics;
CREATE TABLE sentiment_analytics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    sentiment_score DECIMAL(3,2) DEFAULT 0,
    sentiment_label VARCHAR(20),
    confidence DECIMAL(3,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 推荐结果表
DROP TABLE IF EXISTS recommendation_results;
CREATE TABLE recommendation_results (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    timestamp DATETIME NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    news_id VARCHAR(100) NOT NULL,
    rank_score DECIMAL(5,4) DEFAULT 0,
    recommendation_type VARCHAR(50) DEFAULT 'collaborative_filtering',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 新闻基础信息表
DROP TABLE IF EXISTS news_info;
CREATE TABLE news_info (
    news_id VARCHAR(100) PRIMARY KEY,
    title TEXT,
    content LONGTEXT,
    category VARCHAR(50),
    subcategory VARCHAR(50),
    abstract TEXT,
    title_entities TEXT,
    created_time DATETIME,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户基础信息表
DROP TABLE IF EXISTS user_info;
CREATE TABLE user_info (
    user_id VARCHAR(100) PRIMARY KEY,
    registration_time DATETIME,
    last_active_time DATETIME,
    total_clicks INT DEFAULT 0,
    total_impressions INT DEFAULT 0,
    favorite_categories TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 实时统计汇总表
DROP TABLE IF EXISTS real_time_stats;
CREATE TABLE real_time_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stat_time DATETIME NOT NULL,
    stat_type VARCHAR(50) NOT NULL,
    total_users INT DEFAULT 0,
    total_impressions INT DEFAULT 0,
    total_clicks INT DEFAULT 0,
    avg_ctr DECIMAL(5,4) DEFAULT 0,
    top_news TEXT,
    trending_categories TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 查询日志表
DROP TABLE IF EXISTS query_logs;
CREATE TABLE query_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    query_id VARCHAR(100) NOT NULL,
    query_type VARCHAR(50) NOT NULL,
    query_sql TEXT,
    execution_time_ms INT DEFAULT 0,
    result_count INT DEFAULT 0,
    user_id VARCHAR(100),
    query_params TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建基础索引
ALTER TABLE user_behavior_analytics ADD INDEX idx_timestamp (timestamp);
ALTER TABLE user_behavior_analytics ADD INDEX idx_batch_timestamp (batch_id, timestamp);

ALTER TABLE news_popularity_analytics ADD INDEX idx_news_id (news_id);
ALTER TABLE news_popularity_analytics ADD INDEX idx_timestamp (timestamp);
ALTER TABLE news_popularity_analytics ADD INDEX idx_popularity_score (popularity_score);

ALTER TABLE trending_analytics ADD INDEX idx_news_id (news_id);
ALTER TABLE trending_analytics ADD INDEX idx_timestamp (timestamp);
ALTER TABLE trending_analytics ADD INDEX idx_trend_score (trend_score);
ALTER TABLE trending_analytics ADD INDEX idx_category (category);

ALTER TABLE sentiment_analytics ADD INDEX idx_news_id (news_id);
ALTER TABLE sentiment_analytics ADD INDEX idx_timestamp (timestamp);
ALTER TABLE sentiment_analytics ADD INDEX idx_sentiment_label (sentiment_label);

ALTER TABLE recommendation_results ADD INDEX idx_user_id (user_id);
ALTER TABLE recommendation_results ADD INDEX idx_news_id (news_id);
ALTER TABLE recommendation_results ADD INDEX idx_timestamp (timestamp);

ALTER TABLE news_info ADD INDEX idx_category (category);
ALTER TABLE news_info ADD INDEX idx_created_time (created_time);

ALTER TABLE user_info ADD INDEX idx_last_active (last_active_time);

ALTER TABLE real_time_stats ADD INDEX idx_stat_time (stat_time);
ALTER TABLE real_time_stats ADD INDEX idx_stat_type (stat_type);

ALTER TABLE query_logs ADD INDEX idx_query_type (query_type);
ALTER TABLE query_logs ADD INDEX idx_created_at (created_at);

-- 插入示例数据
INSERT INTO news_info (news_id, title, category, subcategory, abstract, created_time) VALUES
('N1001', 'Technology Innovation Trends in 2024', 'tech', 'innovation', 'Latest trends in technology and innovation for the upcoming year', NOW()),
('N1002', 'Global Economic Outlook', 'finance', 'economy', 'Analysis of global economic trends and predictions', NOW()),
('N1003', 'Sports Championship Results', 'sports', 'championship', 'Latest results from major sports championships', NOW()),
('N1004', 'Health and Wellness Tips', 'health', 'wellness', 'Expert advice on maintaining health and wellness', NOW()),
('N1005', 'Entertainment Industry News', 'entertainment', 'movies', 'Latest updates from the entertainment world', NOW());

-- 显示创建结果
SHOW TABLES;
SELECT 'Database initialization completed successfully!' as Status;
