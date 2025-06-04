# -*- coding: utf-8 -*-
"""
测试数据生成器 - 模拟新闻曝光日志数据
"""

import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

class NewsDataSimulator:
    """新闻数据模拟器"""
    
    def __init__(self, kafka_servers=['localhost:9092'], topic='news_impression_logs'):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=kafka_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            self.topic = topic
            
            # 模拟新闻ID池
            self.news_pool = [f"N{i:04d}" for i in range(1, 1001)]  # 1000个新闻
            
            # 模拟用户ID池
            self.user_pool = [f"U{i:05d}" for i in range(1, 10001)]  # 10000个用户
            
            # 新闻类别
            self.categories = [
                'sports', 'entertainment', 'autos', 'tv', 'news', 
                'finance', 'lifestyle', 'movies', 'weather', 'travel',
                'health', 'foodanddrink', 'music', 'kids', 'video'
            ]
            
            print("数据模拟器初始化成功")
        except Exception as e:
            print(f"Kafka连接失败: {e}")
            print("请确保Kafka服务正在运行 (localhost:9092)")
            self.producer = None
    
    def generate_impression_log(self):
        """生成单条曝光日志"""
        user_id = random.choice(self.user_pool)
        
        # 随机选择曝光的新闻数量 (5-20个)
        impression_count = random.randint(5, 20)
        impressed_news = random.sample(self.news_pool, impression_count)
        
        # 随机决定点击的新闻 (0-5个)
        click_count = random.randint(0, min(5, impression_count))
        clicked_news = random.sample(impressed_news, click_count)
        unclicked_news = [news for news in impressed_news if news not in clicked_news]
        
        # 生成用户历史点击 (10-50个)
        history_count = random.randint(10, 50)
        user_history = random.sample(self.news_pool, history_count)
        
        log = {
            "user_id": user_id,
            "timestamp": datetime.now().isoformat(),
            "clicked_news": clicked_news,
            "unclicked_news": unclicked_news,
            "user_history": user_history,
            "session_id": f"session_{random.randint(1000, 9999)}",
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "location": random.choice(["Beijing", "Shanghai", "Guangzhou", "Shenzhen"])
        }
        
        return log
    
    def start_simulation(self, rate_per_second=10, duration_minutes=60):
        """开始数据模拟
        
        Args:
            rate_per_second: 每秒生成的数据条数
            duration_minutes: 运行时长（分钟）
        """
        if not self.producer:
            print("Kafka连接未建立，无法开始模拟")
            return
            
        print(f"开始数据模拟: {rate_per_second}条/秒, 持续{duration_minutes}分钟")
        print(f"目标Topic: {self.topic}")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        count = 0
        error_count = 0
        
        try:
            while datetime.now() < end_time:
                start_time = time.time()
                
                for _ in range(rate_per_second):
                    try:
                        log = self.generate_impression_log()
                        future = self.producer.send(self.topic, value=log, key=log['user_id'])
                        count += 1
                    except Exception as e:
                        error_count += 1
                        if error_count % 10 == 0:
                            print(f"发送错误: {e}")
                
                if count % 100 == 0:
                    print(f"已发送 {count} 条数据, 错误 {error_count} 条")
                
                # 控制发送频率
                elapsed = time.time() - start_time
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                
        except KeyboardInterrupt:
            print("数据模拟被中断")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            print(f"数据模拟结束，共发送 {count} 条数据，错误 {error_count} 条")

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='新闻数据模拟器')
    parser.add_argument('--rate', type=int, default=5, help='每秒数据条数 (默认: 5)')
    parser.add_argument('--duration', type=int, default=10, help='运行时长(分钟) (默认: 10)')
    parser.add_argument('--kafka-servers', default='localhost:9092', help='Kafka服务器地址')
    parser.add_argument('--topic', default='news_impression_logs', help='Kafka Topic名称')
    
    args = parser.parse_args()
    
    print("="*50)
    print("新闻数据模拟器")
    print("="*50)
    print(f"Kafka服务器: {args.kafka_servers}")
    print(f"Topic: {args.topic}")
    print(f"数据速率: {args.rate} 条/秒")
    print(f"运行时长: {args.duration} 分钟")
    print("="*50)
    
    # 创建模拟器
    simulator = NewsDataSimulator(
        kafka_servers=[args.kafka_servers], 
        topic=args.topic
    )
    
    # 开始模拟
    simulator.start_simulation(
        rate_per_second=args.rate, 
        duration_minutes=args.duration
    )

if __name__ == "__main__":
    main()