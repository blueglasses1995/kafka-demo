"""
Avro形式でメッセージを送信するProducer
Schema Registryを使用
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from datetime import datetime
from common.config import KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL


# Avroスキーマ定義
USER_SCHEMA = """
{
  "type": "record",
  "name": "User",
  "namespace": "com.example.kafka",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null},
    {"name": "age", "type": ["null", "int"], "default": null},
    {"name": "created_at", "type": "string"}
  ]
}
"""


class User:
    """ユーザーデータクラス"""
    def __init__(self, user_id, name, email=None, age=None):
        self.user_id = user_id
        self.name = name
        self.email = email
        self.age = age
        self.created_at = datetime.now().isoformat()

    def to_dict(self):
        return {
            'user_id': self.user_id,
            'name': self.name,
            'email': self.email,
            'age': self.age,
            'created_at': self.created_at
        }


def user_to_dict(user, ctx):
    """ユーザーオブジェクトを辞書に変換（シリアライズ用）"""
    return user.to_dict()


class AvroProducer:
    def __init__(self, topic='users-avro'):
        self.topic = topic

        # Schema Registryクライアント
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Avroシリアライザー
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            USER_SCHEMA,
            user_to_dict
        )

        # Producer設定
        producer_conf = {
            'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS),
            'acks': 'all'
        }
        self.producer = Producer(producer_conf)

        print(f"✅ Avro Producer初期化完了 (トピック: {self.topic})")
        print(f"   Schema Registry: {SCHEMA_REGISTRY_URL}")

    def delivery_report(self, err, msg):
        """配信レポートコールバック"""
        if err is not None:
            print(f"❌ メッセージ配信失敗: {err}")
        else:
            print(f"✅ メッセージ配信成功: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")

    def send_user(self, user):
        """ユーザー情報を送信"""
        try:
            # Avroシリアライズ
            serialized_value = self.avro_serializer(
                user,
                SerializationContext(self.topic, MessageField.VALUE)
            )

            # メッセージ送信
            self.producer.produce(
                topic=self.topic,
                key=user.user_id.encode('utf-8'),
                value=serialized_value,
                on_delivery=self.delivery_report
            )

            # 非同期送信のため、バッファをフラッシュして送信完了を待つ
            self.producer.poll(0)

        except Exception as e:
            print(f"❌ 送信エラー: {e}")

    def flush(self):
        """すべてのメッセージを送信完了まで待つ"""
        self.producer.flush()

    def close(self):
        """Producerを閉じる"""
        self.producer.flush()
        print("👋 Avro Producer終了")


def main():
    """使用例"""
    producer = AvroProducer()

    try:
        print("\n" + "="*60)
        print("Avro形式でユーザーデータを送信")
        print("="*60 + "\n")

        # ユーザーデータ作成と送信
        users = [
            User('user001', 'Alice', 'alice@example.com', 30),
            User('user002', 'Bob', 'bob@example.com', 25),
            User('user003', 'Charlie', None, 35),  # emailなし
            User('user004', 'Diana', 'diana@example.com', None),  # ageなし
            User('user005', 'Eve', 'eve@example.com', 28),
        ]

        for user in users:
            print(f"送信: {user.to_dict()}")
            producer.send_user(user)

        # すべて送信完了を待つ
        producer.flush()

        print("\n✨ すべての送信が完了しました！")
        print("   Kafka UIで確認: http://localhost:8080")

    except KeyboardInterrupt:
        print("\n⚠️  中断されました")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
