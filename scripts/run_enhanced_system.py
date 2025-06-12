#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强新闻分析系统启动脚本
一键启动完整的实时分析流水线
"""

import os
import sys
import time
import json
import threading
import subprocess
import logging
from datetime import datetime
from kafka import KafkaProducer
from src.enhanced_data_generator import EnhancedDataGenerator

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class EnhancedSystemLauncher:
    """增强系统启动器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.setup_logging()
        
        # 配置
        self.kafka_config = {
            'bootstrap_servers': 'localhost:9092',
            'topic': 'news-events'
        }
        
        self.mysql_config = {
            'host': 'localhost',
            'user': 'root',
            'password': '',  # 需要用户输入
            'database': 'news_analytics'
        }
        
        # 组件状态
        self.producer = None
        self.data_generator = None
        self.streaming_process = None
        
        # 控制标志
        self.running = False
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/enhanced_system.log'),
                logging.StreamHandler()
            ]
        )
        
        # 创建日志目录
        os.makedirs('logs', exist_ok=True)
    
    def check_prerequisites(self):
        """检查前置条件"""
        print("🔍 检查系统前置条件...")
        
        # 检查Kafka
        if not self._check_kafka():
            print("❌ Kafka未启动，请先启动Kafka")
            return False
        
        # 检查MySQL
        if not self._check_mysql():
            print("❌ MySQL连接失败，请检查配置")
            return False
        
        # 检查Python依赖
        if not self._check_dependencies():
            print("❌ Python依赖不完整")
            return False
        
        print("✅ 前置条件检查通过")
        return True
    
    def _check_kafka(self):
        """检查Kafka状态"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            producer.close()
            return True
        except Exception as e:
            self.logger.error(f"Kafka检查失败: {e}")
            return False
    
    def _check_mysql(self):
        """检查MySQL连接"""
        try:
            import mysql.connector
            conn = mysql.connector.connect(**self.mysql_config)
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"MySQL检查失败: {e}")
            return False
    
    def _check_dependencies(self):
        """检查Python依赖"""
        required_packages = [
            'kafka-python', 'mysql-connector-python', 'pyspark',
            'pandas', 'numpy', 'scikit-learn', 'textblob'
        ]
        
        missing = []
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
            except ImportError:
                missing.append(package)
        
        if missing:
            print(f"缺少依赖包: {missing}")
            print("请运行: pip install " + " ".join(missing))
            return False
        
        return True
    
    def start_data_generation(self):
        """启动数据生成"""
        print("🏭 启动数据生成器...")
        
        try:
            # 初始化Kafka生产者
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3
            )
            
            # 初始化数据生成器
            self.data_generator = EnhancedDataGenerator('data/')
            
            # 启动生成线程
            self.running = True
            generation_thread = threading.Thread(target=self._generate_data_loop)
            generation_thread.daemon = True
            generation_thread.start()
            
            print("✅ 数据生成器启动成功")
            
        except Exception as e:
            print(f"❌ 数据生成器启动失败: {e}")
            raise
    
    def _generate_data_loop(self):
        """数据生成循环"""
        batch_count = 0
        
        while self.running:
            try:
                batch_count += 1
                
                # 生成一批事件（模拟不同场景）
                if batch_count % 3 == 1:
                    # 普通批次
                    events = self.data_generator.generate_batch_events(8)
                elif batch_count % 3 == 2:
                    # 用户会话
                    events = self.data_generator.generate_realistic_session()
                else:
                    # 混合数据
                    events = self.data_generator.generate_batch_events(5)
                    events.extend(self.data_generator.generate_realistic_session(session_length=3))
                
                # 发送到Kafka
                for event in events:
                    self.producer.send(self.kafka_config['topic'], event)
                
                self.producer.flush()
                
                print(f"📤 发送批次 #{batch_count}: {len(events)} 条事件")
                
                # 等待一段时间（模拟真实数据流）
                time.sleep(15)  # 15秒发送一批
                
            except Exception as e:
                self.logger.error(f"数据生成失败: {e}")
                time.sleep(5)
    
    def start_spark_streaming(self):
        """启动Spark流处理"""
        print("⚡ 启动Spark流处理...")
        
        try:
            # 构建启动命令
            cmd = [
                'python', 'src/enhanced_spark_streaming_analyzer.py'
            ]
            
            # 设置环境变量
            env = os.environ.copy()
            env['PYTHONPATH'] = os.getcwd()
            
            # 启动Spark流处理
            self.streaming_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                env=env,
                cwd=os.getcwd()
            )
            
            print("✅ Spark流处理启动成功")
            
            # 启动日志监控线程
            log_thread = threading.Thread(target=self._monitor_spark_logs)
            log_thread.daemon = True
            log_thread.start()
            
        except Exception as e:
            print(f"❌ Spark流处理启动失败: {e}")
            raise
    
    def _monitor_spark_logs(self):
        """监控Spark日志"""
        while self.running and self.streaming_process:
            try:
                line = self.streaming_process.stdout.readline()
                if line:
                    # 过滤并显示重要日志
                    if any(keyword in line for keyword in ['批次', '分析', '错误', 'ERROR', 'WARNING']):
                        print(f"🔍 Spark: {line.strip()}")
                
                if self.streaming_process.poll() is not None:
                    break
                    
            except Exception as e:
                self.logger.error(f"日志监控失败: {e}")
                break
    
    def start_system(self):
        """启动完整系统"""
        print("🚀 启动增强新闻分析系统")
        print("="*50)
        
        try:
            # 1. 检查前置条件
            if not self.check_prerequisites():
                return False
            
            # 2. 启动数据生成
            self.start_data_generation()
            time.sleep(3)  # 等待生产者初始化
            
            # 3. 启动Spark流处理
            self.start_spark_streaming()
            time.sleep(5)  # 等待Spark初始化
            
            print("\n🎉 系统启动完成!")
            print("📊 实时分析正在进行中...")
            print("📝 按 Ctrl+C 停止系统")
            
            # 显示状态面板
            self._show_status_panel()
            
            return True
            
        except Exception as e:
            print(f"❌ 系统启动失败: {e}")
            self.stop_system()
            return False
    
    def _show_status_panel(self):
        """显示状态面板"""
        start_time = datetime.now()
        
        try:
            while self.running:
                # 清屏（在支持的终端中）
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print("🎯 增强新闻分析系统 - 实时状态")
                print("="*60)
                print(f"🕒 运行时间: {datetime.now() - start_time}")
                print(f"📡 Kafka状态: {'✅ 正常' if self._check_kafka() else '❌ 异常'}")
                print(f"🗄️ MySQL状态: {'✅ 正常' if self._check_mysql() else '❌ 异常'}")
                print(f"🏭 数据生成: {'✅ 运行中' if self.running else '❌ 停止'}")
                print(f"⚡ Spark流处理: {'✅ 运行中' if self.streaming_process and self.streaming_process.poll() is None else '❌ 停止'}")
                
                print("\n📊 功能模块:")
                print("  1. 📈 新闻生命周期分析")
                print("  2. 📊 分类趋势统计")
                print("  3. 👥 用户兴趣演化")
                print("  4. 🔍 多维度统计查询")
                print("  5. 🔥 爆款新闻预测")
                print("  6. 💡 实时个性化推荐")
                print("  7. 📝 查询日志记录")
                
                print("\n💡 提示:")
                print("  - 打开另一个终端运行: python examples/seven_analysis_examples.py")
                print("  - 查看MySQL数据: SELECT * FROM news_lifecycle LIMIT 5;")
                print("  - 按 Ctrl+C 停止系统")
                
                time.sleep(10)  # 每10秒更新一次
                
        except KeyboardInterrupt:
            print("\n🛑 收到停止信号...")
            self.stop_system()
    
    def stop_system(self):
        """停止系统"""
        print("\n🛑 正在停止系统...")
        
        self.running = False
        
        # 停止数据生成
        if self.producer:
            self.producer.close()
            print("✅ 数据生成器已停止")
        
        # 停止Spark流处理
        if self.streaming_process:
            self.streaming_process.terminate()
            self.streaming_process.wait(timeout=10)
            print("✅ Spark流处理已停止")
        
        print("✅ 系统停止完成")

def main():
    """主函数"""
    print("🎯 增强新闻分析系统启动器")
    print("="*40)
    
    # 获取配置
    mysql_password = input("请输入MySQL密码: ").strip()
    
    # 创建启动器
    launcher = EnhancedSystemLauncher()
    launcher.mysql_config['password'] = mysql_password
    
    try:
        # 启动系统
        success = launcher.start_system()
        
        if success:
            # 系统运行中，等待用户中断
            while launcher.running:
                time.sleep(1)
        
    except KeyboardInterrupt:
        print("\n🛑 用户手动停止")
    except Exception as e:
        print(f"\n❌ 系统运行异常: {e}")
    finally:
        launcher.stop_system()

if __name__ == "__main__":
    main() 