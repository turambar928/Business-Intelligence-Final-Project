#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻实时分析系统 - 快速启动脚本
"""

import os
import sys
import time
import subprocess
import threading
import logging
from datetime import datetime

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/startup.log'),
            logging.StreamHandler()
        ]
    )

def check_dependencies():
    """检查依赖服务"""
    print("检查系统依赖...")
    
    # 检查Java
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✓ Java 已安装")
        else:
            print("✗ Java 未安装或配置错误")
            return False
    except FileNotFoundError:
        print("✗ Java 未安装")
        return False
    
    # 检查Python依赖
    try:
        import pyspark
        import kafka
        import pymysql
        import pandas
        import numpy
        import sklearn
        print("✓ Python依赖已安装")
    except ImportError as e:
        print(f"✗ Python依赖缺失: {e}")
        print("请运行: pip install -r requirements.txt")
        return False
    
    return True

def check_services():
    """检查服务状态"""
    print("\n检查服务状态...")
    
    services_status = {
        'kafka': False,
        'mysql': False,
        'zookeeper': False
    }
    
    # 检查Kafka (端口9092)
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('localhost', 9092))
        if result == 0:
            services_status['kafka'] = True
            print("✓ Kafka服务正在运行 (端口 9092)")
        else:
            print("✗ Kafka服务未运行 (端口 9092)")
        sock.close()
    except Exception as e:
        print(f"✗ Kafka检查失败: {e}")
    
    # 检查MySQL (端口3306)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('localhost', 3306))
        if result == 0:
            services_status['mysql'] = True
            print("✓ MySQL服务正在运行 (端口 3306)")
        else:
            print("✗ MySQL服务未运行 (端口 3306)")
        sock.close()
    except Exception as e:
        print(f"✗ MySQL检查失败: {e}")
    
    # 检查Zookeeper (端口2181)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex(('localhost', 2181))
        if result == 0:
            services_status['zookeeper'] = True
            print("✓ Zookeeper服务正在运行 (端口 2181)")
        else:
            print("✗ Zookeeper服务未运行 (端口 2181)")
        sock.close()
    except Exception as e:
        print(f"✗ Zookeeper检查失败: {e}")
    
    return services_status

def create_kafka_topic():
    """创建Kafka Topic"""
    print("\n创建Kafka Topic...")
    
    try:
        # 假设Kafka安装在用户主目录
        kafka_dir = os.path.expanduser("~/kafka_2.13-2.8.2")
        
        if os.path.exists(kafka_dir):
            topic_cmd = [
                f"{kafka_dir}/bin/kafka-topics.sh",
                "--create",
                "--topic", "news_impression_logs",
                "--bootstrap-server", "localhost:9092",
                "--partitions", "3",
                "--replication-factor", "1",
                "--if-not-exists"
            ]
            
            result = subprocess.run(topic_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print("✓ Kafka Topic 创建成功")
                return True
            else:
                print(f"Kafka Topic 创建失败: {result.stderr}")
        else:
            print(f"Kafka目录不存在: {kafka_dir}")
            print("请手动创建Topic: kafka-topics.sh --create --topic news_impression_logs --bootstrap-server localhost:9092")
    except Exception as e:
        print(f"创建Kafka Topic失败: {e}")
    
    return False

def start_data_simulator():
    """启动数据模拟器"""
    print("\n启动数据模拟器...")
    
    try:
        simulator_process = subprocess.Popen([
            sys.executable, 'utils/data_simulator.py',
            '--rate', '3',
            '--duration', '120'  # 运行2小时
        ])
        print("✓ 数据模拟器已启动")
        return simulator_process
    except Exception as e:
        print(f"启动数据模拟器失败: {e}")
        return None

def start_spark_analyzer():
    """启动Spark分析器"""
    print("\n启动Spark分析器...")
    
    try:
        analyzer_process = subprocess.Popen([
            sys.executable, 'main.py'
        ])
        print("✓ Spark分析器已启动")
        return analyzer_process
    except Exception as e:
        print(f"启动Spark分析器失败: {e}")
        return None

def show_service_instructions():
    """显示服务启动指令"""
    print("\n" + "="*60)
    print("服务启动指令 (请在不同终端中执行)")
    print("="*60)
    
    print("\n1. 启动Zookeeper:")
    print("   cd ~/kafka_2.13-2.8.2")
    print("   bin/zookeeper-server-start.sh config/zookeeper.properties")
    
    print("\n2. 启动Kafka:")
    print("   cd ~/kafka_2.13-2.8.2")
    print("   bin/kafka-server-start.sh config/server.properties")
    
    print("\n3. 启动MySQL:")
    print("   sudo systemctl start mysql")
    print("   # 或者")
    print("   sudo service mysql start")
    
    print("\n4. 创建数据库:")
    print("   mysql -u root -p < sql/init_database.sql")
    
    print("\n5. 安装Python依赖:")
    print("   pip install -r requirements.txt")
    
    print("\n" + "="*60)

def main():
    """主函数"""
    print("="*60)
    print("新闻实时分析系统 - 启动向导")
    print("="*60)
    print(f"启动时间: {datetime.now()}")
    
    # 创建必要目录
    os.makedirs('logs', exist_ok=True)
    os.makedirs('checkpoints', exist_ok=True)
    os.makedirs('utils', exist_ok=True)
    
    # 设置日志
    setup_logging()
    
    # 检查依赖
    if not check_dependencies():
        print("\n请先安装缺失的依赖")
        show_service_instructions()
        return
    
    # 检查服务状态
    services = check_services()
    
    missing_services = [name for name, status in services.items() if not status]
    
    if missing_services:
        print(f"\n缺失的服务: {', '.join(missing_services)}")
        show_service_instructions()
        
        print("\n请启动缺失的服务后重新运行此脚本")
        print("或者按回车键继续 (将跳过服务检查)...")
        input()
    
    # 创建Kafka Topic
    create_kafka_topic()
    
    print("\n" + "="*60)
    print("选择启动模式:")
    print("1. 完整模式 (数据模拟器 + Spark分析器)")
    print("2. 仅启动数据模拟器")
    print("3. 仅启动Spark分析器")
    print("4. 显示监控命令")
    print("="*60)
    
    choice = input("请选择 (1-4): ").strip()
    
    processes = []
    
    if choice == '1':
        # 完整模式
        print("\n启动完整系统...")
        
        # 先启动数据模拟器
        simulator = start_data_simulator()
        if simulator:
            processes.append(simulator)
            time.sleep(5)  # 等待5秒让数据开始生成
        
        # 再启动分析器
        analyzer = start_spark_analyzer()
        if analyzer:
            processes.append(analyzer)
        
    elif choice == '2':
        # 仅数据模拟器
        simulator = start_data_simulator()
        if simulator:
            processes.append(simulator)
    
    elif choice == '3':
        # 仅分析器
        analyzer = start_spark_analyzer()
        if analyzer:
            processes.append(analyzer)
    
    elif choice == '4':
        # 显示监控命令
        print("\n监控命令:")
        print("1. 查看Kafka消息:")
        print("   kafka-console-consumer --topic news_impression_logs --bootstrap-server localhost:9092")
        
        print("\n2. 查看Spark UI:")
        print("   浏览器访问: http://localhost:4040")
        
        print("\n3. 查看MySQL数据:")
        print("   mysql -u root -p news_analytics")
        print("   SELECT COUNT(*) FROM user_behavior_analytics;")
        
        print("\n4. 查看日志:")
        print("   tail -f logs/news_analytics.log")
        
        return
    
    # 等待用户中断
    if processes:
        print(f"\n系统已启动 {len(processes)} 个进程")
        print("按 Ctrl+C 停止所有进程...")
        
        try:
            # 等待所有进程
            for process in processes:
                process.wait()
        except KeyboardInterrupt:
            print("\n正在停止所有进程...")
            for process in processes:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
            print("所有进程已停止")

if __name__ == "__main__":
    main() 