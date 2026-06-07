import os
import asyncio
import pytest

from mcp2mqtt import server
from mcp2mqtt.server import Config


@pytest.fixture(autouse=True)
def load_config():
    # 加载项目根目录的 config.yaml 并注入到 server 模块的全局变量
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.yaml'))
    cfg = Config.load(config_path)
    server.config = cfg
    return cfg


def test_tuya_control_end_to_end(monkeypatch, load_config):
    """集成测试：模拟 MQTT 端并调用 handle_call_tool 验证消息发送与响应处理"""
    sent = {}

    async def fake_connect_and_send(self, topic: str, message: str, response_topic: str = None, timeout: int = 5, query_only: bool = False):
        # 记录发送内容并模拟对方响应（HA 通常会把状态回写到 response_topic）
        sent['topic'] = topic
        sent['message'] = message
        sent['response_topic'] = response_topic
        sent['query_only'] = query_only
        # 模拟延迟
        await asyncio.sleep(0.01)
        # 模拟收到的响应与发送消息一致（如 HA 会把 'on'/'off' 写回 state 主题）
        return message

    # 替换 MQTTConnection.connect_and_send 为模拟实现
    monkeypatch.setattr(server, 'MQTTConnection', lambda cfg: type('X', (), {'connect_and_send': fake_connect_and_send})())

    # 测试控制模式：发送 'on'
    result = asyncio.run(server.handle_call_tool('tuya_control', {'state': 'on'}))
    assert sent['topic'] == server.config.tools['tuya_control'].mqtt_topic
    assert sent['message'] == 'on'
    assert sent['query_only'] == False
    assert isinstance(result, list)
    assert result[0].text == 'on'


def test_tuya_control_query_mode(monkeypatch, load_config):
    """集成测试：验证 tuya_control 的查询模式 (state=query)"""
    sent = {}

    async def fake_connect_and_send(self, topic: str, message: str, response_topic: str = None, timeout: int = 5, query_only: bool = False):
        sent['topic'] = topic
        sent['message'] = message
        sent['response_topic'] = response_topic
        sent['query_only'] = query_only
        await asyncio.sleep(0.01)
        # 模拟返回当前灯状态 'on'
        return 'on'

    monkeypatch.setattr(server, 'MQTTConnection', lambda cfg: type('X', (), {'connect_and_send': fake_connect_and_send})())

    # 调用查询模式
    result = asyncio.run(server.handle_call_tool('tuya_control', {'state': 'query'}))

    # 验证 query_only=True，且没有发送消息
    assert sent['query_only'] == True
    assert sent['message'] == ''
    assert isinstance(result, list)
    # 返回的文本应该包含状态信息
    assert '开' in result[0].text or 'on' in result[0].text.lower()
