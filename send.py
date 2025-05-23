import json
import time
import logging
import pika
from typing import Optional, Dict, Any

from config import conf

logger = logging.getLogger(__name__)


class MessageProducerClient:
    """消息生产者客户端，负责向RabbitMQ发送消息"""

    def __init__(self):
        """初始化消息生产者客户端"""
        # password = conf().get("rabbitmq_password")
        # username = conf().get("rabbitmq_username")
        # self.connection_params = pika.ConnectionParameters(
        #     host=conf().get("rabbitmq_host"),
        #     virtual_host=conf().get("rabbitmq_virtual_host"),
        #     port=conf().get("rabbitmq_port"),
        #     credentials=pika.PlainCredentials(username, password)
        # )
        password = "guest1"
        username = "guest1"
        self.connection_params = pika.ConnectionParameters(
            host="47.98.205.97",
            virtual_host="/wechat",
            port="5672",
            credentials=pika.PlainCredentials(username, password)
        )
        self.connection = None
        self.channel = None
        self.exchange_name = "chat_messages"  # 消息交换机名称

    def connect(self):
        """建立RabbitMQ连接"""
        try:
            self.connection = pika.BlockingConnection(self.connection_params)
            self.channel = self.connection.channel()

            # 声明交换机
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type='topic',
                durable=True
            )

            logger.info("Message producer client successfully connected to RabbitMQ")
            return True
        except Exception as e:
            logger.error(f"Message producer client failed to connect to RabbitMQ: {str(e)}")
            return False

    def publish_message(self, message_data: Dict[str, Any], session_id: Optional[str] = None) -> bool:
        """发布消息到聊天消息交换机

        Args:
            message_data: 消息数据字典
            session_id: 会话ID，如果不提供则从message_data中获取

        Returns:
            bool: 发送成功返回True，否则返回False
        """
        try:
            if not self.channel or not self.connection or self.connection.is_closed:
                if not self.connect():
                    logger.error("Cannot publish message, RabbitMQ connection not available")
                    return False

            # 确定session_id
            if not session_id:
                session_id = message_data.get("session_id", f"session_{int(time.time())}")

            # 确保消息数据中有session_id字段
            if "session_id" not in message_data:
                message_data["session_id"] = session_id

            # 添加时间戳（如果没有）
            if "create_time" not in message_data:
                message_data["create_time"] = int(time.time())

            # 序列化消息
            json_data = json.dumps(message_data)

            # 发布消息
            routing_key = f"chat_session.{session_id}"  # 路由键

            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=routing_key,
                body=json_data,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 消息持久化
                    content_type='application/json'
                )
            )

            logger.debug(f"Published message with routing key {routing_key}")
            return True

        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            # 尝试重新连接
            self.connect()
            return False

    def publish_text_message(self, content: str, from_user_id: str, to_user_id: str,
                             is_group: bool = False, session_id: Optional[str] = None, 
                             msg_type: str = "text", url: str = "") -> bool:
        """发布文本或媒体消息（便捷方法）

        Args:
            content: 消息内容
            from_user_id: 发送者ID
            to_user_id: 接收者ID
            is_group: 是否为群消息
            session_id: 会话ID，如果不提供则自动生成
            msg_type: 消息类型，如"text"或"image"
            url: 媒体文件URL或ID

        Returns:
            bool: 发送成功返回True，否则返回False
        """
        # 打印消息参数
        logger.info("====== 准备发布消息 ======")
        logger.info(f"消息内容: {content}")
        logger.info(f"消息类型: {msg_type}")
        logger.info(f"URL: {url}")
        logger.info(f"发送者ID: {from_user_id}")
        logger.info(f"接收者ID: {to_user_id}")
        logger.info(f"是群消息: {is_group}")
        logger.info(f"会话ID: {session_id}")
        
        # 构造消息数据
        message = {
            "from_user_id": from_user_id,
            "to_user_id": to_user_id,
            "content": content,
            "msg_type": msg_type,  # 使用传入的消息类型
            "is_group": is_group,
            "create_time": int(time.time()),
            "url": url  # 添加URL字段
        }

        # 设置会话ID
        if not session_id:
            if is_group:
                session_id = to_user_id  # 群聊使用群ID作为会话ID
            else:
                session_id = from_user_id  # 私聊使用发送者ID作为会话ID

        message["session_id"] = session_id

        # 添加群聊特有字段
        if is_group:
            message["other_user_id"] = to_user_id  # 群ID
            message["actual_user_id"] = from_user_id  # 实际发送者ID
        else:
            message["other_user_id"] = to_user_id  # 接收者ID
            
        # 打印最终消息结构
        logger.info(f"最终消息结构: {message}")
        logger.info("====== 消息准备完成 ======")

        # 发布消息
        result = self.publish_message(message, session_id)
        logger.info(f"消息发布结果: {'成功' if result else '失败'}")
        return result

    def close(self):
        """关闭RabbitMQ连接"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.close()

            if self.connection and self.connection.is_open:
                self.connection.close()

            logger.info("Closed RabbitMQ connection")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")


# 测试消息生产者
if __name__ == "__main__":
    import logging

    # logging.basicConfig(level=logging.DEBUG)

    producer = MessageProducerClient()
    if producer.connect():
        # 发送私聊消息
        producer.publish_text_message(
            content="我是科比",
            from_user_id="agent-86cdf093-d133-412e-8a40-a51b1d520a7c",
            to_user_id="bot456",
            is_group=False
        )

        # 发送群聊消息
        producer.publish_text_message(
            content="大家好，这是一条群测试消息",
            from_user_id="agent-86cdf093-d133-412e-8a40-a51b1d520a7c",
            to_user_id="group789",
            is_group=True
        )

        # 关闭连接
        producer.close()
