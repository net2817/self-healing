# -*- coding:utf-8 -*-
import subprocess
import time
import psutil
import os
import sys
import datetime
from loguru import logger
from datetime import datetime, timedelta,date
from kafka import KafkaConsumer, TopicPartition

import hmac
import hashlib
import base64
import urllib.parse
import requests
import json

# 定义进程名
process_name = 'java.exe'

#定义初始化偏移量 
total_offset_old = 0

# 配置Kafka消费者
KAFKA_SERVER = ['*.*.*.*:9092']
KAFKA_TOPIC = '***'
KAFKA_GROUP_ID = '**'
KAFKA_AUTO_OFFSET_RESET = 'latest'

# 初始化时间戳和阈值
last_msg_time = datetime.now()
threshold = timedelta(minutes=5)


# 钉钉机器人的访问令牌和加签密钥
url = "https://oapi.dingtalk.com/robot/send?access_token=2bb6bc4aed306d05a38b37ef8f6a7f2302*********************"
secret = "SEC842743b3be8ea14bbbee9e3586cbd790ae2b4ef724e093e*****************"

# 发送钉钉消息的函数
def send_alert(message):
    # 获取当前时间戳（单位：毫秒）
    timestamp = str(round(time.time() * 1000))

    # 将时间戳和加签密钥拼接成字符串
    string_to_sign = f"{timestamp}\n{secret}"

    # 使用HMAC-SHA256算法对字符串进行加密，并将结果转换为Base64编码的字符串
    sign = base64.b64encode(hmac.new(secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()).decode()

    # 在请求参数中添加时间戳和加签信息
    url_with_sign = f"{url}&timestamp={timestamp}&sign={urllib.parse.quote(sign)}"

    # 获取当前日期字符串
    date_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    # 在消息内容中添加时间戳
    message_with_timestamp = f"{date_str}发生：{message}"

    # 钉钉消息内容
    data = {
        "msgtype": "text",
        "text": {
            "content": message_with_timestamp
        }
    }

    # 发送POST请求
    response = requests.post(url_with_sign, headers={"Content-Type": "application/json"}, data=json.dumps(data))

    # 输出响应结果
    print(response.json())

# 检查进程是否存在
def check_process(process_name):
    #获取进程详细信息
    ret = subprocess.getoutput('wmic.exe process where name="java.exe" get commandline 2>nul|findstr "realtime_streaming.jar" 1>nul 2>nul && echo 1||echo 0')

    #如果有返回信息，进程存在，否则进程不存在。
    if ret == "1":
            print(f"process {process_name} true!\n")
            return True
    print(f"process {process_name} false!\n")
    logger.info(f'check process def !')
    return False
#判断当前时间是不是在执行时间区间内
def check_time():
    now_time = datetime.now().strftime('%H:%M:%S')
    dayofweek = datetime.now().weekday()
    print(now_time)
    print(dayofweek)

    if dayofweek == 5:
       print(f'false 6{dayofweek}')
       logger.error(f'check time false {dayofweek}')
       return False
    elif dayofweek == 4 and "18:00:00" < now_time and  now_time < "24:59:59":
       logger.error(f'check time false {dayofweek} {now_time}')
       print(f'false 5 {dayofweek} {now_time}')
       return False
    elif dayofweek == 6 and "00:00:00" < now_time and now_time < "17:00:00":
        logger.error(f'check time false {dayofweek} {now_time}')
        print(f'false 7{dayofweek} {now_time}')
        return False
    elif "18:00:00" < now_time < "24:59:59" or "00:00:00" < now_time < "17:00:00":
        logger.info(f'check time true {dayofweek} {now_time}')
        print(f'starttime1 ok {dayofweek} {now_time}')
        return True
    else:
        print(f'false {dayofweek} {now_time}')
        logger.error(f'check time false {dayofweek} {now_time}')
        return False

# 定义监控Kafka消费队列的函数
def check_kafka():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER,group_id=KAFKA_GROUP_ID,api_version=(0,10,2))
# 获取指定 topic 的所有分区
    partitions = consumer.partitions_for_topic(KAFKA_TOPIC)

    total_offset_new = 0
    global total_offset_old
    for partition in partitions:
        # 获取当前分区的最新偏移量
        tp = TopicPartition(KAFKA_TOPIC,partition)
        latest_offset = consumer.end_offsets([tp])[tp]
        total_offset_new += latest_offset
        print(latest_offset)
        print(tp)
        print(total_offset_new)
        logger.info(f'{latest_offset}{tp}{total_offset_new}')

    # 判断最新偏移量是否与上一次相等
    if total_offset_new == total_offset_old:
        print(f'Kafka not alive on partition {total_offset_new}!')
        logger.info(f'Kafka not alive on partition {total_offset_new}!')
        return False
    else:
        print(f'Kafka alive on partition {total_offset_old}. Latest offset: {total_offset_new}')
        logger.info(f'Kafka alive on partition {total_offset_old}. Latest offset: {total_offset_new}')
        total_offset_old = total_offset_new
        return True

# 主程序
if __name__ == "__main__":
    logger.add('mod3_info.log',format="{time} | {level} | {message}",level="INFO")
    logger.add('mod3_error.log',format="{time} | {level} | {message}",level="ERROR")
    while True:
        # 检查进程和Kafka消费队列是否正常
        process_ok = check_process(process_name)
        kafka_ok = check_kafka()
        logger.info(f'check process and kafka !')

        #如果在执行时间区内，进程与kafka全部判断，否则只判断进程
        time_ok = check_time()
        if time_ok:
        # 如果进程或Kafka消费队列出现故障，执行自愈操作
            if not process_ok or not kafka_ok:
                # 判断进程是否存在，存在杀死进程
                if process_ok:
                   cmd = f"taskkill /F /IM {process_name}"
                   subprocess.Popen(cmd, shell=True)
                   logger.info(f'kill process')
            
                # 重启进程
                subprocess.Popen(f"java -classpath E:/soft/kafka-clients-3.1.0.jar;E:/soft/realtime_streaming.jar;E:/soft/mysql-connector-java-8.0.26.jar com.jewelake.streaming.realtime.IQFeedToKafka")
                logger.info(f'open process')
            
                time.sleep(10)
                # 发送警报通知管理员
                send_alert("iqfeed reload")
                logger.info(f'send_alert')
            
                # 休眠10秒钟后再次检查进程和Kafka消费队列
                logger.info(f'sleep 10s')
        else:
            if not process_ok:
                # 重启进程
                subprocess.Popen(f"java -classpath E:/soft/kafka-clients-3.1.0.jar;E:/soft/realtime_streaming.jar;E:/soft/mysql-connector-java-8.0.26.jar com.jewelake.streaming.realtime.IQFeedToKafka")
                logger.info(f'open process')
            
                time.sleep(10)
                # 发送警报通知管理员
                send_alert("iqfeed reload")
                logger.info(f'send_alert')
            
                # 休眠10秒钟后再次检查进程和Kafka消费队列
                logger.info(f'sleep 10s')
            
        time.sleep(10)
