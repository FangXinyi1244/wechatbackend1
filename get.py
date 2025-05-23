import json
import logging
import threading
import pika
from typing import List, Callable


logger = logging.getLogger(__name__)


class ResultConsumer(threading.Thread):
    """结果消息消费者（线程安全版本）"""

    def __init__(self, msg_types: List[str], session_id:str = None,queue_name: str = None, callback: Callable = None):
        """
        Args:
            session_ids: 需要监听的会话ID列表，用于生成路由键
            queue_name: 指定队列名称（若需持久化）
            callback: 可选的消息处理回调函数
        """
        super().__init__()
        self._msg_types = msg_types
        self._queue_name = queue_name
        self._connection = None
        self._channel = None
        self._running = False
        self._callback = callback
        self._session_id = session_id
        self._init_connection_params()

    def _init_connection_params(self):
        """初始化连接参数（与生产者一致）"""
        password = "guest1"
        username = "guest1"
        self._connection_params = pika.ConnectionParameters(
            host="47.98.205.97",
            virtual_host="/wechat",
            port="5672",
            credentials=pika.PlainCredentials(username, password)
        )

    def _connect(self):
        """建立连接并初始化拓扑结构"""
        try:
            self._connection = pika.BlockingConnection(self._connection_params)
            self._channel = self._connection.channel()

            # 声明直连交换机（与生产者一致）
            self._channel.exchange_declare(
                exchange="chat_responses",
                exchange_type='topic',
                durable=True
            )

            # 声明队列（自动删除队列或持久化队列）
            queue_args = {}
            if not self._queue_name:
                # 匿名临时队列（自动删除）
                result = self._channel.queue_declare(queue='', exclusive=True)
                self._queue_name = result.method.queue
            else:
                # 持久化队列（需指定名称）
                self._channel.queue_declare(
                    queue=self._queue_name,
                    durable=True,
                    arguments=queue_args
                )

            # 绑定路由键
            for sid in self._msg_types:
                routing_key = f"chat_result.{self._session_id}.{sid}"
                self._channel.queue_bind(
                    exchange="chat_responses",
                    queue=self._queue_name,
                    routing_key=routing_key
                )

            logger.info(f"Consumer connected to queue: {self._queue_name}")
            return True
        except Exception as e:
            logger.error(f"Consumer connection failed: {e}")
            return False

    def _message_handler(self, ch, method, properties, body):
        """消息处理回调函数"""
        try:
            msg = json.loads(body)
            logger.info(f"====== 收到消息 ======")
            logger.info(f"消息内容: {json.dumps(msg, ensure_ascii=False)}")
            logger.info(f"会话ID: {msg.get('session_id')}")

            # 如果有自定义回调，则调用
            if self._callback:
                logger.info("调用自定义回调处理消息")
                self._callback(msg)
            else:
                # 默认处理逻辑
                logger.info(f"默认处理消息: {msg.get('session_id')}")

            # 手动确认消息
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("====== 消息处理完成 ======")
        except json.JSONDecodeError:
            logger.error("Failed to parse JSON message")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def run(self):
        """启动消费线程"""
        if not self._connect():
            return

        self._running = True
        try:
            # 配置QoS（公平分发）
            self._channel.basic_qos(prefetch_count=1)

            # 开始消费
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._message_handler,
                auto_ack=False
            )

            # 持续监听（阻塞直到连接关闭）
            while self._running:
                self._connection.process_data_events()
                if self._connection.is_closed:
                    if not self._connect():
                        break
        except Exception as e:
            logger.error(f"Consumer thread error: {e}")
        finally:
            self.stop()

    def stop(self):
        """停止消费者，安全地关闭连接和清理资源"""
        # 首先标记线程为非运行状态
        self._running = False
        logger.info("Stopping consumer thread...")
        
        try:
            # 1. 首先检查并删除队列
            if self._channel is not None:
                try:
                    if getattr(self._channel, '_impl', None) is not None and hasattr(self._channel, 'is_open') and self._channel.is_open:
                        if self._queue_name and not self._queue_name.startswith('amq.'):
                            try:
                                logger.info(f"Attempting to delete queue: {self._queue_name}")
                                self._channel.queue_delete(queue=self._queue_name)
                                logger.info(f"Queue {self._queue_name} deleted successfully")
                            except pika.exceptions.ChannelClosedByBroker as e:
                                logger.warning(f"Channel closed by broker when deleting queue: {e}")
                            except Exception as e:
                                logger.warning(f"Could not delete queue {self._queue_name}: {e}")
                except Exception as e:
                    logger.warning(f"Error checking channel status: {e}")
            
            # 2. 关闭信道
            if self._channel is not None:
                try:
                    if getattr(self._channel, '_impl', None) is not None and hasattr(self._channel, 'is_open') and self._channel.is_open:
                        logger.info("Closing channel...")
                        self._channel.close()
                        logger.info("Channel closed")
                    else:
                        logger.info("Channel already closed or invalid")
                except Exception as e:
                    logger.warning(f"Error closing channel: {e}")
                finally:
                    self._channel = None
            
            # 3. 关闭连接
            if self._connection is not None:
                try:
                    if hasattr(self._connection, 'is_open') and self._connection.is_open:
                        logger.info("Closing connection...")
                        self._connection.close()
                        logger.info("Connection closed")
                    else:
                        logger.info("Connection already closed or invalid")
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
                finally:
                    self._connection = None
                    
            logger.info("Consumer stopped and resources cleaned up successfully")
        except Exception as e:
            logger.error(f"Unexpected error during consumer cleanup: {e}", exc_info=True)
        finally:
            # 确保变量被设置为None，以帮助垃圾回收
            self._channel = None
            self._connection = None

# 使用示例 --------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 自定义回调函数
    def custom_callback(msg):
        print(f"Custom processing for message: {msg}")

    # 创建消费者线程（监听会话ID）
    consumer = ResultConsumer(
        msg_types=["text", "image", "voice"],
        queue_name="result_chat_queue",  # 使用持久化队列时指定名称
        callback=custom_callback  # 传入自定义回调函数
    )

    # 启动消费线程
    consumer.start()

    # 主线程继续执行其他操作...
    try:
        while True:
            pass
    except KeyboardInterrupt:
        consumer.stop()
