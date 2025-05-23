def conf():
    """返回应用配置"""
    return {
    "channel_type": "rabbitmq_wechat",
    "dify_api_base": "https://api.dify.ai/v1",
    "dify_api_key": "app-Int627Wj6idMUAqxEJp039Sq",
    "dify_app_type": "chatbot",
    "group_chat_prefix": [
        "@bot"
    ],
    "group_name_white_list": [
        "ALL_GROUP"
    ],
    "letta_agent_id": "agent-86cdf093-d133-412e-8a40-a51b1d520a7c",
    "letta_base_url": "http://k6e8af.natappfree.cc",
    "model": "dify",
    "mysql_db": "wechatbot",
    "mysql_host": "47.98.205.97",
    "mysql_password": "fxy123456",
    "mysql_port": 13306,
    "mysql_user": "root",
    "rabbitmq_host": "47.98.205.97",
    "rabbitmq_password": "guest1",
    "rabbitmq_port": 5672,
    "rabbitmq_username": "guest1",
    "rabbitmq_virtual_host": "/wechat",
    "single_chat_prefix": [
        ""
    ],
    "single_chat_reply_prefix": "",

    "text_to_voice": "dify",
    "voice_to_text": "dify"
}
