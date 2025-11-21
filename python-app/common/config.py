"""
Kafka設定ファイル
"""

# Kafka接続設定
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Schema Registry設定
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# トピック設定
DEFAULT_TOPIC = 'test-topic'

# Producer設定
PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'key_serializer': lambda k: k.encode('utf-8') if k else None,
    'value_serializer': lambda v: v.encode('utf-8') if isinstance(v, str) else v,
    'acks': 'all',  # すべてのレプリカから確認
    'retries': 3,
    'max_in_flight_requests_per_connection': 5,
    'compression_type': 'snappy',
}

# Consumer設定
CONSUMER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'key_deserializer': lambda k: k.decode('utf-8') if k else None,
    'value_deserializer': lambda v: v.decode('utf-8') if v else None,
    'auto_offset_reset': 'earliest',  # 最初から読む
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5000,
    'session_timeout_ms': 10000,
    'max_poll_records': 500,
}
