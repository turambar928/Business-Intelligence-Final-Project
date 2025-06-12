# 🎯 增强新闻实时分析系统运行指南

## 📋 系统概述

增强的新闻实时分析系统支持七大核心分析功能：

1. **📈 新闻生命周期分析** - 单个新闻在不同时间段的流行变化
2. **📊 分类趋势统计** - 不同类别新闻的变化情况
3. **👥 用户兴趣演化** - 用户兴趣变化统计查询
4. **🔍 多维度统计查询** - 按时间、主题、长度、用户等多条件查询
5. **🔥 爆款新闻预测** - 分析什么样的新闻最可能成为爆款
6. **💡 实时个性化推荐** - 根据用户浏览内容进行新闻推荐
7. **📝 查询日志记录** - 记录所有SQL查询和性能指标

## 🏗️ 系统架构

```
数据源 → Kafka → Spark Streaming → AI分析引擎 → MySQL → 可视化分析
```

## 🚀 快速启动指南

### 步骤1: 环境准备

```bash
# 1. 安装Python依赖
pip install -r requirements.txt

# 2. 创建必要目录
mkdir -p logs data/results

# 3. 设置执行权限
chmod +x scripts/start_kafka.sh
```

### 步骤2: 启动Kafka

```bash
# 启动Kafka和Zookeeper
bash scripts/start_kafka.sh
```

**注意**: 如果Kafka路径不是默认路径，请编辑 `scripts/start_kafka.sh` 中的 `KAFKA_HOME` 变量。

### 步骤3: 初始化数据库

```bash
# 运行数据库初始化脚本
python setup/init_enhanced_database.py
```

系统会提示输入MySQL连接信息：
- 主机地址 (默认: localhost)
- 用户名 (默认: root)
- 密码
- 数据库名 (默认: news_analytics)

### 步骤4: 快速测试

```bash
# 运行系统测试
python scripts/quick_test.py
```

确保所有测试通过后再进行下一步。

### 步骤5: 启动系统

```bash
# 启动完整系统
python scripts/run_enhanced_system.py
```

系统启动后会显示实时状态面板，包含：
- ✅ 各组件运行状态
- 📊 实时数据统计
- 💡 操作提示

## 📊 查看分析结果

### 方法1: 运行分析示例

在新终端中运行：

```bash
python examples/seven_analysis_examples.py
```

### 方法2: 直接查询MySQL

```sql
-- 1. 查看新闻生命周期
SELECT * FROM news_lifecycle ORDER BY popularity_score DESC LIMIT 10;

-- 2. 查看分类趋势
SELECT * FROM category_trends ORDER BY date_hour DESC LIMIT 10;

-- 3. 查看用户兴趣演化
SELECT * FROM user_interest_evolution ORDER BY calculated_at DESC LIMIT 10;

-- 4. 查看爆款预测
SELECT * FROM viral_news_prediction WHERE viral_score > 0.7 ORDER BY viral_score DESC;

-- 5. 查看推荐结果
SELECT * FROM real_time_recommendations ORDER BY generated_at DESC LIMIT 10;

-- 6. 查看查询日志
SELECT query_type, COUNT(*) as count, AVG(execution_time_ms) as avg_time 
FROM query_logs GROUP BY query_type;
```

### 方法3: 实时数据监控

```bash
# 监控Kafka消息
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic news-events

# 监控系统日志
tail -f logs/enhanced_system.log
```

## 🔧 配置说明

### MySQL配置

编辑相关脚本中的MySQL配置：

```python
mysql_config = {
    'host': 'localhost',      # MySQL主机
    'user': 'root',           # 用户名
    'password': 'your_pass',  # 密码
    'database': 'news_analytics'  # 数据库名
}
```

### Kafka配置

编辑Kafka相关配置：

```python
kafka_config = {
    'bootstrap_servers': 'localhost:9092',  # Kafka服务器
    'topic': 'news-events'                  # Topic名称
}
```

### Spark配置

Spark配置在 `src/enhanced_spark_streaming_analyzer.py` 中：

```python
spark = SparkSession.builder \
    .appName("EnhancedNewsStreamingAnalyzer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()
```

## 📈 数据格式

### 输入数据格式

系统接收JSON格式的新闻事件：

```json
{
    "timestamp": "2024-01-15T10:30:45.123Z",
    "user_id": "U335175",
    "news_id": "N41340",
    "action_type": "read",
    "dwell_time": 116,
    "device_info": {
        "device_type": "mobile",
        "os": "iOS",
        "browser": "Safari"
    },
    "news_details": {
        "category": "sports",
        "topic": "soccer",
        "headline": "新闻标题",
        "content": "新闻内容",
        "word_count": 500
    },
    "user_context": {
        "user_interests": ["体育", "科技"],
        "engagement_score": 0.75
    },
    "interaction_data": {
        "scroll_depth": 0.8,
        "clicks_count": 3,
        "shares_count": 1
    }
}
```

## 🎯 使用场景

### 场景1: 新闻编辑室

```bash
# 查看当前热门新闻
python -c "
from examples.seven_analysis_examples import SevenAnalysisExamples
analyzer = SevenAnalysisExamples({'host':'localhost','user':'root','password':'','database':'news_analytics'})
analyzer.analysis_2_category_trends('news', 6)
"
```

### 场景2: 内容运营

```bash
# 预测爆款新闻
python -c "
from examples.seven_analysis_examples import SevenAnalysisExamples
analyzer = SevenAnalysisExamples({'host':'localhost','user':'root','password':'','database':'news_analytics'})
analyzer.analysis_5_viral_prediction(0.6)
"
```

### 场景3: 用户研究

```bash
# 分析用户兴趣变化
python -c "
from examples.seven_analysis_examples import SevenAnalysisExamples
analyzer = SevenAnalysisExamples({'host':'localhost','user':'root','password':'','database':'news_analytics'})
analyzer.analysis_3_user_interests('U335175', 7)
"
```

## 🔍 故障排除

### 常见问题

**1. Kafka启动失败**

```bash
# 检查端口占用
lsof -i :9092
lsof -i :2181

# 清理Kafka数据
rm -rf /tmp/kafka-logs*
rm -rf /tmp/zookeeper*

# 重新启动
bash scripts/start_kafka.sh
```

**2. MySQL连接失败**

```bash
# 检查MySQL服务
sudo systemctl status mysql

# 重启MySQL
sudo systemctl restart mysql

# 检查用户权限
mysql -u root -p -e "SHOW GRANTS FOR 'root'@'localhost';"
```

**3. Spark内存不足**

编辑 `src/enhanced_spark_streaming_analyzer.py`：

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()
```

**4. Python依赖问题**

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 重新安装依赖
pip install -r requirements.txt
```

### 日志位置

- **系统日志**: `logs/enhanced_system.log`
- **Kafka日志**: `logs/kafka.log`
- **Zookeeper日志**: `logs/zookeeper.log`
- **数据库日志**: MySQL默认日志位置

### 性能调优

**1. 增加Kafka分区**

```bash
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic news-events --partitions 6
```

**2. 优化MySQL查询**

```sql
-- 添加索引
CREATE INDEX idx_user_events_time ON user_events(timestamp);
CREATE INDEX idx_news_category ON news_articles(category, topic);

-- 查看慢查询
SELECT * FROM query_logs WHERE execution_time_ms > 1000 ORDER BY execution_time_ms DESC;
```

**3. 调整Spark批次大小**

```python
# 在启动脚本中调整
.trigger(processingTime="15 seconds")  # 从10秒调整到15秒
```

## 📊 监控指标

系统提供多种监控指标：

- **吞吐量**: 每秒处理的事件数
- **延迟**: 端到端处理延迟
- **错误率**: 处理失败的事件比例
- **资源使用**: CPU、内存、磁盘使用情况

查看性能指标：

```sql
SELECT * FROM performance_metrics ORDER BY measured_at DESC LIMIT 10;
```

## 🎉 成功运行标志

当您看到以下信息时，系统运行成功：

1. ✅ Kafka服务正常 (端口2181, 9092)
2. ✅ MySQL连接正常
3. ✅ Spark流处理正在运行
4. ✅ 数据生成器正在发送数据
5. 📊 AI分析结果正在更新
6. 📈 数据库表中有实时数据

## 📞 技术支持

如遇问题，请检查：

1. **系统日志**: 查看详细错误信息
2. **依赖版本**: 确保使用推荐版本
3. **资源使用**: 确保有足够的内存和CPU
4. **网络连接**: 确保各组件可以正常通信

系统正常运行后，您就可以体验完整的七大分析功能了！🎯 