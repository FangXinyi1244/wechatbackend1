import time
import threading
import queue
import json
import requests
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename

from send import MessageProducerClient
from get import ResultConsumer

app = Flask(__name__)
CORS(app)

# 用于存储每个session_id的最新AI回复
session_reply_queues = {}
# 存储每个session_id对应的消费者对象，用于后续清理
session_consumers = {}

# 创建上传文件目录
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def start_consumer_for_session(session_id):
    """为每个session_id启动一个消费者线程，将AI回复放入队列"""
    if session_id in session_reply_queues:
        return
    q = queue.Queue()
    session_reply_queues[session_id] = q

    def callback(msg):
        # 只处理当前session_id的消息
        if msg.get("session_id") == session_id:
            q.put(msg.get("content", ""))

    consumer = ResultConsumer(
        msg_types=["text", "image"],  # 增加image类型
        session_id=session_id,
        callback=callback
    )
    # 存储消费者对象，用于后续清理
    session_consumers[session_id] = consumer
    t = threading.Thread(target=consumer.run, daemon=True)
    t.start()

def upload_image_to_dify(local_file_path, file_type):
    """上传图片到Dify API并获取文件ID"""
    api_key = "app-1Z4ZFou3mClikBHkTvhBwGnM"  # Dify API密钥
    url = "https://api.dify.ai/v1/files/upload"
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    
    try:
        with open(local_file_path, "rb") as image_file:
            files = {
                "file": (os.path.basename(local_file_path), image_file, file_type),
            }
            data = {
                "user": "aug618",
            }
            
            print(f"正在上传图片到Dify: {url}")
            print(f"头部信息: {headers}")
            print(f"文件信息: {os.path.basename(local_file_path)}, 类型: {file_type}")
            
            response = requests.post(url, headers=headers, files=files, data=data)
            response.raise_for_status()
            
            result = response.json()
            print(f"Dify响应: {result}")
            
            # 返回上传后的文件ID
            return result.get("id")
    except Exception as e:
        print(f"上传到Dify失败: {e}")
        if 'response' in locals():
            print(f"响应状态码: {response.status_code}")
            print(f"响应内容: {response.text}")
        return None

@app.route('/send_message', methods=['POST'])
def send_message():
    data = request.get_json()
    
    # 打印接收到的请求数据
    print("====== 接收到的消息请求 ======")
    print(f"请求数据: {json.dumps(data, ensure_ascii=False, indent=2)}")
    
    content = data.get('content', '')
    from_user_id = data.get('from_user_id', '')
    to_user_id = data.get('to_user_id', '')
    is_group = data.get('is_group', False)
    group_id = data.get('group_id', '')
    session_id = data.get('session_id', '')
    msg_type = data.get('msg_type', 'text')  # 获取消息类型，默认为text
    url = data.get('url', '')  # 获取URL，默认为空字符串
    
    # 打印关键参数
    print(f"消息内容: {content}")
    print(f"消息类型: {msg_type}")
    print(f"URL: {url}")
    print(f"发送者ID: {from_user_id}")
    print(f"接收者ID: {to_user_id}")
    print(f"是群消息: {is_group}")
    print(f"群组ID: {group_id}")
    print(f"会话ID: {session_id}")

    # 启动消费者监听本session_id
    start_consumer_for_session(session_id)

    producer = MessageProducerClient()
    producer.connect()
    
    # 使用更新后的publish_text_message方法发送消息，包括msg_type和url
    ok = producer.publish_text_message(
        content=content,
        from_user_id=from_user_id,
        to_user_id=to_user_id,
        is_group=is_group,
        session_id=session_id,
        msg_type=msg_type,
        url=url
    )
    
    # 打印发送结果
    print(f"消息发送结果: {'成功' if ok else '失败'}")
    print("====== 消息请求处理结束 ======\n")
    
    producer.close()
    if ok:
        return jsonify({"code": 0, "msg": "消息已发送"})
    else:
        return jsonify({"code": 1, "msg": "消息发送失败"}), 500

@app.route('/get_message', methods=['GET'])
def get_message():
    session_id = request.args.get('session_id')
    
    # 打印接收到的请求参数
    print("====== 轮询消息请求 ======")
    print(f"会话ID: {session_id}")
    
    if not session_id or session_id not in session_reply_queues:
        print("会话ID无效或不存在")
        print("====== 轮询请求结束 ======\n")
        return jsonify({"content": ""})

    q = session_reply_queues[session_id]
    try:
        # 最多等待2秒
        content = q.get(timeout=2)
        print(f"获取到回复: {content[:50]}{'...' if len(content) > 50 else ''}")
        print("====== 轮询请求结束 ======\n")
        return jsonify({"content": content})
    except queue.Empty:
        print("等待超时，无回复")
        print("====== 轮询请求结束 ======\n")
        return jsonify({"content": ""})

@app.route('/cleanup_session', methods=['POST'])
def cleanup_session():
    """清理指定会话的资源"""
    data = request.get_json()
    session_id = data.get('session_id')
    
    if not session_id:
        return jsonify({"code": 1, "msg": "需要提供session_id"}), 400
    
    # 停止消费者线程
    if session_id in session_consumers:
        try:
            consumer = session_consumers[session_id]
            consumer.stop()  # 停止消费者线程，这会关闭连接并删除队列
            del session_consumers[session_id]
        except Exception as e:
            print(f"停止消费者异常: {e}")
    
    # 清理队列
    if session_id in session_reply_queues:
        del session_reply_queues[session_id]
    
    return jsonify({"code": 0, "msg": "会话资源已清理"})

@app.route('/upload_image', methods=['POST'])
def upload_image():
    """处理图片上传"""
    print("====== 接收到图片上传请求 ======")
    
    # 检查是否有文件
    if 'file' not in request.files:
        print("没有文件部分")
        return jsonify({"code": 1, "msg": "没有文件部分"}), 400
        
    file = request.files['file']
    session_id = request.form.get('session_id', '')
    user = request.form.get('user', '')
    
    print(f"会话ID: {session_id}")
    print(f"用户: {user}")
    
    # 检查文件是否有效
    if file.filename == '':
        print("无选中文件")
        return jsonify({"code": 1, "msg": "无选中文件"}), 400
        
    # 保存文件到临时位置
    filename = secure_filename(file.filename)
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(file_path)
    print(f"文件已保存到: {file_path}")
    
    # 上传到Dify API获取文件ID
    try:
        dify_file_id = upload_image_to_dify(file_path, file.content_type)
        
        if dify_file_id:
            print(f"Dify文件ID: {dify_file_id}")
            return jsonify({
                "code": 0,
                "msg": "图片上传成功",
                "url": dify_file_id
            })
        else:
            return jsonify({"code": 2, "msg": "上传到Dify失败"}), 500
    except Exception as e:
        print(f"处理图片时出错: {e}")
        return jsonify({"code": 3, "msg": f"处理图片时出错: {str(e)}"}), 500
    finally:
        # 清理临时文件
        try:
            os.remove(file_path)
            print(f"临时文件已删除: {file_path}")
        except:
            print(f"删除临时文件失败: {file_path}")
        
        print("====== 图片上传请求处理完成 ======")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9919)