-- =====================================================
-- 新闻实时分析系统 - 增强数据库表结构
-- 支持七大分析功能的完整数据库设计
-- =====================================================

-- 1. 新闻基础信息表
CREATE TABLE IF NOT EXISTS news_articles (
    news_id VARCHAR(20) PRIMARY KEY,
    headline TEXT NOT NULL,
    content LONGTEXT,
    category VARCHAR(50),
    topic VARCHAR(100),
    word_count INT DEFAULT 0,
    publish_time TIMESTAMP,
    source VARCHAR(100),
    tags JSON,
    entities JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_category (category),
    INDEX idx_topic (topic),
    INDEX idx_publish_time (publish_time),
    INDEX idx_word_count (word_count)
);

-- 2. 用户基础信息表
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(20) PRIMARY KEY,
    registration_date TIMESTAMP,
    location_country VARCHAR(50),
    location_province VARCHAR(50),
    location_city VARCHAR(50),
    interests JSON,
    demographics JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_location (location_country, location_province, location_city),
    INDEX idx_registration (registration_date)
);

-- 3. 用户行为事件表（核心表）
CREATE TABLE IF NOT EXISTS user_events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    news_id VARCHAR(20) NOT NULL,
    action_type ENUM('read', 'skip', 'share', 'like', 'comment', 'click') NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    session_id VARCHAR(50),
    dwell_time INT DEFAULT 0,
    device_type VARCHAR(20),
    os VARCHAR(20),
    browser VARCHAR(30),
    scroll_depth DECIMAL(3,2) DEFAULT 0,
    clicks_count INT DEFAULT 0,
    shares_count INT DEFAULT 0,
    comments_count INT DEFAULT 0,
    likes_count INT DEFAULT 0,
    reading_time_of_day ENUM('morning', 'afternoon', 'evening', 'night'),
    is_weekend BOOLEAN DEFAULT FALSE,
    engagement_score DECIMAL(3,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (news_id) REFERENCES news_articles(news_id),
    INDEX idx_user_time (user_id, timestamp),
    INDEX idx_news_time (news_id, timestamp),
    INDEX idx_action_time (action_type, timestamp),
    INDEX idx_session (session_id),
    INDEX idx_dwell_time (dwell_time),
    INDEX idx_engagement (engagement_score)
);

-- 4. AI分析结果表
CREATE TABLE IF NOT EXISTS ai_analysis_results (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    news_id VARCHAR(20) NOT NULL,
    sentiment_score DECIMAL(3,2) DEFAULT 0,
    topic_keywords JSON,
    viral_potential DECIMAL(3,2) DEFAULT 0,
    trend_score DECIMAL(3,2) DEFAULT 0,
    content_quality DECIMAL(3,2) DEFAULT 0,
    analysis_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (news_id) REFERENCES news_articles(news_id),
    INDEX idx_sentiment (sentiment_score),
    INDEX idx_viral (viral_potential),
    INDEX idx_trend (trend_score),
    INDEX idx_analysis_time (analysis_timestamp)
);

-- 5. 新闻生命周期分析表（分析1：单个新闻生命周期）
CREATE TABLE IF NOT EXISTS news_lifecycle (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    news_id VARCHAR(20) NOT NULL,
    time_period VARCHAR(20) NOT NULL, -- '1h', '6h', '12h', '1d', '3d', '7d'
    total_reads INT DEFAULT 0,
    total_shares INT DEFAULT 0,
    total_likes INT DEFAULT 0,
    total_comments INT DEFAULT 0,
    unique_readers INT DEFAULT 0,
    avg_dwell_time DECIMAL(8,2) DEFAULT 0,
    popularity_score DECIMAL(5,2) DEFAULT 0,
    growth_rate DECIMAL(5,2) DEFAULT 0,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (news_id) REFERENCES news_articles(news_id),
    UNIQUE KEY unique_news_period (news_id, time_period),
    INDEX idx_period (time_period),
    INDEX idx_popularity (popularity_score),
    INDEX idx_growth (growth_rate)
);

-- 6. 新闻分类趋势表（分析2：不同类别新闻变化）
CREATE TABLE IF NOT EXISTS category_trends (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    topic VARCHAR(100),
    date_hour TIMESTAMP NOT NULL,
    total_articles INT DEFAULT 0,
    total_reads INT DEFAULT 0,
    total_interactions INT DEFAULT 0,
    avg_engagement DECIMAL(3,2) DEFAULT 0,
    trending_keywords JSON,
    hot_topics JSON,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_category_hour (category, topic, date_hour),
    INDEX idx_category_time (category, date_hour),
    INDEX idx_engagement_trend (avg_engagement),
    INDEX idx_hot_topics (date_hour)
);

-- 7. 用户兴趣变化表（分析3：用户兴趣变化）
CREATE TABLE IF NOT EXISTS user_interest_evolution (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    date_period DATE NOT NULL,
    interest_categories JSON,
    interest_scores JSON,
    reading_patterns JSON,
    behavior_changes JSON,
    engagement_trend DECIMAL(3,2) DEFAULT 0,
    diversity_score DECIMAL(3,2) DEFAULT 0, -- 兴趣多样性
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    UNIQUE KEY unique_user_period (user_id, date_period),
    INDEX idx_user_date (user_id, date_period),
    INDEX idx_engagement_trend_user (engagement_trend),
    INDEX idx_diversity (diversity_score)
);

-- 8. 多维度统计查询表（分析4：多条件统计查询）
CREATE TABLE IF NOT EXISTS multidim_statistics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stat_type ENUM('time_based', 'topic_based', 'title_length', 'content_length', 'user_based') NOT NULL,
    dimension_keys JSON, -- 存储查询维度的键值对
    time_range VARCHAR(50),
    result_metrics JSON, -- 存储统计结果
    sample_size INT DEFAULT 0,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stat_type (stat_type),
    INDEX idx_time_range (time_range),
    INDEX idx_calculated (calculated_at)
);

-- 9. 爆款新闻预测表（分析5：爆款新闻分析）
CREATE TABLE IF NOT EXISTS viral_news_prediction (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    news_id VARCHAR(20) NOT NULL,
    viral_score DECIMAL(5,2) DEFAULT 0,
    predicted_peak_time TIMESTAMP,
    key_factors JSON, -- 影响爆款的关键因素
    social_signals JSON, -- 社交媒体信号
    trend_indicators JSON,
    actual_performance JSON, -- 实际表现（用于模型训练）
    prediction_accuracy DECIMAL(3,2), -- 预测准确度
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (news_id) REFERENCES news_articles(news_id),
    INDEX idx_viral_score (viral_score),
    INDEX idx_predicted_time (predicted_peak_time),
    INDEX idx_accuracy (prediction_accuracy)
);

-- 10. 实时推荐结果表（分析6：实时个性化推荐）
CREATE TABLE IF NOT EXISTS real_time_recommendations (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(20) NOT NULL,
    recommended_news JSON, -- 推荐的新闻ID列表
    recommendation_scores JSON, -- 对应的推荐分数
    recommendation_reasons JSON, -- 推荐理由
    context_factors JSON, -- 上下文因素
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    clicked_news JSON, -- 用户实际点击的新闻
    recommendation_effectiveness DECIMAL(3,2), -- 推荐效果
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    INDEX idx_user_generated (user_id, generated_at),
    INDEX idx_effectiveness (recommendation_effectiveness)
);

-- 11. 查询日志表（分析7：查询日志记录）
CREATE TABLE IF NOT EXISTS query_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    query_id VARCHAR(50) UNIQUE NOT NULL,
    query_type ENUM('lifecycle', 'category_trend', 'user_interest', 'multidim_stat', 'viral_prediction', 'recommendation', 'other') NOT NULL,
    sql_query TEXT NOT NULL,
    query_parameters JSON,
    execution_time_ms INT DEFAULT 0,
    result_count INT DEFAULT 0,
    user_agent VARCHAR(200),
    ip_address VARCHAR(45),
    query_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT,
    INDEX idx_query_type (query_type),
    INDEX idx_execution_time (execution_time_ms),
    INDEX idx_timestamp (query_timestamp),
    INDEX idx_performance (execution_time_ms, result_count)
);

-- 12. 系统性能指标表
CREATE TABLE IF NOT EXISTS performance_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_type VARCHAR(50) NOT NULL,
    metric_value DECIMAL(10,2) NOT NULL,
    metric_unit VARCHAR(20),
    dimensions JSON,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_metric_type (metric_type),
    INDEX idx_measured_time (measured_at)
);

-- 13. 实时处理批次记录表
CREATE TABLE IF NOT EXISTS processing_batches (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(50) UNIQUE NOT NULL,
    source VARCHAR(50),
    records_count INT DEFAULT 0,
    processing_start TIMESTAMP,
    processing_end TIMESTAMP,
    processing_duration_ms INT,
    status ENUM('processing', 'completed', 'failed') DEFAULT 'processing',
    error_details TEXT,
    INDEX idx_batch_id (batch_id),
    INDEX idx_status (status),
    INDEX idx_processing_time (processing_start, processing_end)
);

-- =====================================================
-- 创建视图以支持复杂分析查询
-- =====================================================

-- 新闻热度实时视图
CREATE OR REPLACE VIEW news_popularity_realtime AS
SELECT 
    n.news_id,
    n.headline,
    n.category,
    n.topic,
    COUNT(DISTINCT e.user_id) as unique_readers,
    COUNT(e.id) as total_interactions,
    AVG(e.dwell_time) as avg_dwell_time,
    SUM(CASE WHEN e.action_type = 'share' THEN 1 ELSE 0 END) as shares,
    SUM(CASE WHEN e.action_type = 'like' THEN 1 ELSE 0 END) as likes,
    AVG(e.engagement_score) as avg_engagement,
    MAX(e.timestamp) as last_interaction
FROM news_articles n
LEFT JOIN user_events e ON n.news_id = e.news_id
WHERE e.timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY n.news_id, n.headline, n.category, n.topic;

-- 用户活跃度视图
CREATE OR REPLACE VIEW user_activity_summary AS
SELECT 
    u.user_id,
    COUNT(DISTINCT e.news_id) as articles_read,
    COUNT(e.id) as total_actions,
    AVG(e.dwell_time) as avg_reading_time,
    AVG(e.engagement_score) as avg_engagement,
    COUNT(DISTINCT DATE(e.timestamp)) as active_days,
    MAX(e.timestamp) as last_activity
FROM users u
LEFT JOIN user_events e ON u.user_id = e.user_id
WHERE e.timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY u.user_id; 