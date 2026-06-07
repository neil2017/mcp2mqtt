import os
from mcp2mqtt.server import Config, Tool, format_tool_message, diagnose_mqtt_broker


def test_config_load_from_yaml():
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.yaml'))
    config = Config.load(config_path)

    assert config.mqtt_broker != ''
    assert config.mqtt_port > 0
    assert isinstance(config.tools, dict)
    # 验证 tuya_control 工具已加载
    assert 'tuya_control' in config.tools
    assert config.tools['tuya_control'].name == 'tuya_control'
    assert config.tools['tuya_control'].mqtt_topic == 'ha/light/lou_ti_deng/set'
    assert config.tools['tuya_control'].response_format == '{state}'


def test_format_tool_message_for_generic_tool():
    tool_config = Tool(
        name='tuya_control',
        description='控制楼梯灯开关 (连接到HA)',
        parameters=[
            {'name': 'state', 'type': 'string', 'description': '灯的状态(on/off)', 'required': True, 'enum': ['on', 'off']}
        ],
        mqtt_topic='ha/light/lou_ti_deng/set',
        response_topic='ha/light/lou_ti_deng/state',
        response_format='{state}'
    )
    message = format_tool_message(tool_config, {'state': 'on'})

    assert message == 'on'


def test_format_tool_message_for_generic_tool_missing_template():
    tool_config = Tool(
        name='generic_tool',
        description='Generic tool',
        parameters=[
            {'name': 'value', 'type': 'string', 'description': 'value', 'required': True}
        ],
        mqtt_topic='generic/topic',
        response_topic='generic/response',
        response_format=''
    )
    message = format_tool_message(tool_config, {'value': '42'})

    assert message == '42'


def test_diagnose_mqtt_broker_reachable():
    """测试 MQTT Broker 诊断 - 可达的 broker"""
    result = diagnose_mqtt_broker('broker.emqx.io', 1883)
    assert '连接成功' in result


def test_diagnose_mqtt_broker_unreachable():
    """测试 MQTT Broker 诊断 - 不可达的 broker"""
    result = diagnose_mqtt_broker('192.0.2.1', 1883)  # TEST-NET 地址，肯定不可达
    assert '连接失败' in result or '连接异常' in result


def test_diagnose_mqtt_broker_dns_fail():
    """测试 MQTT Broker 诊断 - DNS 解析失败（使用一个肯定不存在的域名）"""
    # 使用一个包含非法字符的域名，确保 DNS 解析失败
    result = diagnose_mqtt_broker('', 1883)
    # 空主机名应该导致错误
    assert '诊断过程出错' in result or 'DNS 解析失败' in result or '连接失败' in result or '连接异常' in result
