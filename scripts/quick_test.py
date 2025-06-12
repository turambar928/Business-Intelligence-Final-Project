#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试脚本
验证增强系统的各个组件是否正常工作
"""

import json
import time
import mysql.connector
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime, timedelta
import sys
import os

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.enhanced_data_generator import EnhancedDataGenerator

class QuickTester:
    """快速测试器"""
    
    def __init__(self):
        self.mysql_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '',
            'database': 'news_analytics'
        }
        
        self.kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'news-events'
        }
    
    def test_mysql_connection(self):
        """测试MySQL连接"""
        print("🔍 测试MySQL连接...")
        
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 检查表是否存在
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            
            print(f"✅ MySQL连接成功")
            print(f"📋 发现 {len(tables)} 个表")
            
            # 检查关键表
            key_tables = ['news_articles', 'users', 'user_events']
            for table in key_tables:
                if table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  ✅ {table}: {count} 条记录")
                else:
                    print(f"  ❌ {table}: 表不存在")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ MySQL连接失败: {e}")
            return False
    
    def test_kafka_connection(self):
        """测试Kafka连接"""
        print("🔍 测试Kafka连接...")
        
        try:
            # 测试生产者
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # 发送测试消息
            test_message = {
                'test': True,
                'timestamp': datetime.now().isoformat(),
                'message': 'Kafka连接测试'
            }
            
            producer.send(self.kafka_config['topic'], test_message)
            producer.flush()
            producer.close()
            
            print("✅ Kafka连接成功")
            print("✅ 测试消息发送成功")
            
            return True
            
        except Exception as e:
            print(f"❌ Kafka连接失败: {e}")
            return False
    
    def test_data_generator(self):
        """测试数据生成器"""
        print("🔍 测试数据生成器...")
        
        try:
            generator = EnhancedDataGenerator('data/')
            
            # 生成单个事件
            event = generator.generate_enhanced_event()
            print("✅ 单个事件生成成功")
            
            # 验证事件结构
            required_fields = ['timestamp', 'user_id', 'news_id', 'action_type']
            for field in required_fields:
                if field not in event:
                    print(f"  ❌ 缺少字段: {field}")
                    return False
                else:
                    print(f"  ✅ {field}: {event[field]}")
            
            # 生成批次事件
            batch = generator.generate_batch_events(5)
            print(f"✅ 批次事件生成成功: {len(batch)} 条")
            
            # 生成用户会话
            session = generator.generate_realistic_session()
            print(f"✅ 用户会话生成成功: {len(session)} 条")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据生成器测试失败: {e}")
            return False
    
    def test_data_pipeline(self):
        """测试完整数据流水线"""
        print("🔍 测试数据流水线...")
        
        try:
            # 1. 生成测试数据
            generator = EnhancedDataGenerator('data/')
            events = generator.generate_batch_events(3)
            
            # 2. 发送到Kafka
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
            )
            
            for event in events:
                producer.send(self.kafka_config['topic'], event)
            
            producer.flush()
            producer.close()
            print(f"✅ 发送 {len(events)} 条测试数据到Kafka")
            
            # 3. 验证Kafka消费
            consumer = KafkaConsumer(
                self.kafka_config['topic'],
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=5000
            )
            
            received_count = 0
            for message in consumer:
                received_count += 1
                print(f"  📨 接收消息: {message.value['user_id']} - {message.value['action_type']}")
                if received_count >= len(events):
                    break
            
            consumer.close()
            print(f"✅ 从Kafka接收 {received_count} 条消息")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据流水线测试失败: {e}")
            return False
    
    def test_ai_analysis(self):
        """测试AI分析功能"""
        print("🔍 测试AI分析功能...")
        
        try:
            from src.enhanced_ai_analysis_engine import EnhancedAIAnalysisEngine
            
            # 创建AI分析引擎
            ai_engine = EnhancedAIAnalysisEngine(self.mysql_config)
            
            # 生成测试数据
            generator = EnhancedDataGenerator('data/')
            test_data = generator.generate_batch_events(5)
            
            # 执行分析
            results = ai_engine.analyze_batch(test_data)
            
            print("✅ AI分析执行成功")
            print(f"  📊 处理记录数: {results.get('processed_count', 0)}")
            
            # 检查分析结果
            analysis_results = results.get('analysis_results', {})
            for analysis_type, data in analysis_results.items():
                if isinstance(data, list):
                    print(f"  ✅ {analysis_type}: {len(data)} 条结果")
                else:
                    print(f"  ✅ {analysis_type}: 已完成")
            
            return True
            
        except Exception as e:
            print(f"❌ AI分析测试失败: {e}")
            return False
    
    def test_database_operations(self):
        """测试数据库操作"""
        print("🔍 测试数据库操作...")
        
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()
            
            # 测试插入用户事件
            test_event = {
                'user_id': 'TEST_USER',
                'news_id': 'TEST_NEWS',
                'action_type': 'read',
                'timestamp': datetime.now(),
                'dwell_time': 120,
                'device_type': 'mobile',
                'engagement_score': 0.75
            }
            
            # 插入测试数据
            insert_query = """
            INSERT INTO user_events (user_id, news_id, action_type, timestamp, dwell_time, device_type, engagement_score)
            VALUES (%(user_id)s, %(news_id)s, %(action_type)s, %(timestamp)s, %(dwell_time)s, %(device_type)s, %(engagement_score)s)
            """
            
            cursor.execute(insert_query, test_event)
            conn.commit()
            print("✅ 测试数据插入成功")
            
            # 查询测试数据
            cursor.execute("SELECT * FROM user_events WHERE user_id = 'TEST_USER'")
            result = cursor.fetchone()
            
            if result:
                print("✅ 测试数据查询成功")
            else:
                print("❌ 测试数据查询失败")
                return False
            
            # 清理测试数据
            cursor.execute("DELETE FROM user_events WHERE user_id = 'TEST_USER'")
            conn.commit()
            print("✅ 测试数据清理完成")
            
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ 数据库操作测试失败: {e}")
            return False
    
    def run_all_tests(self):
        """运行所有测试"""
        print("🎯 增强系统快速测试")
        print("="*40)
        
        tests = [
            ("MySQL连接", self.test_mysql_connection),
            ("Kafka连接", self.test_kafka_connection),
            ("数据生成器", self.test_data_generator),
            ("数据库操作", self.test_database_operations),
            ("数据流水线", self.test_data_pipeline),
            ("AI分析", self.test_ai_analysis)
        ]
        
        results = {}
        
        for test_name, test_func in tests:
            print(f"\n{'='*20}")
            try:
                results[test_name] = test_func()
            except Exception as e:
                print(f"❌ 测试 {test_name} 异常: {e}")
                results[test_name] = False
        
        # 测试结果汇总
        print(f"\n{'='*40}")
        print("📊 测试结果汇总:")
        
        passed = 0
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ 通过" if result else "❌ 失败"
            print(f"  {test_name}: {status}")
            if result:
                passed += 1
        
        print(f"\n🎯 总体结果: {passed}/{total} 通过")
        
        if passed == total:
            print("🎉 所有测试通过！系统可以正常运行")
            print("\n💡 接下来可以:")
            print("  1. 启动完整系统: python scripts/run_enhanced_system.py")
            print("  2. 运行分析示例: python examples/seven_analysis_examples.py")
        else:
            print("⚠️ 部分测试失败，请检查配置")
        
        return passed == total

def main():
    """主函数"""
    print("请输入MySQL密码（用于测试）:")
    mysql_password = input().strip()
    
    tester = QuickTester()
    tester.mysql_config['password'] = mysql_password
    
    success = tester.run_all_tests()
    
    if not success:
        print("\n🔧 故障排除建议:")
        print("1. 检查MySQL服务是否启动: sudo systemctl status mysql")
        print("2. 检查Kafka服务是否启动: bash scripts/start_kafka.sh")
        print("3. 检查Python依赖: pip install -r requirements.txt")
        print("4. 重新初始化数据库: python setup/init_enhanced_database.py")

if __name__ == "__main__":
    main() 