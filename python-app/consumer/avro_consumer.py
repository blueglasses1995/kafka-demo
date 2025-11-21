"""
Avro形式のメッセージを受信するConsumer
Schema Registryを使用
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import signal
from common.config import KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL


class AvroConsumer:
    def __init__(self, topic='users-avro', group_id='avro-consumer-group'):
        self.topic = topic
        self.group_id = group_id
        self.running = True

        # Schema Registryクライアント
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Avroデシリアライザー
        # スキーマはSchema Registryから自動取得
        self.avro_deserializer = AvroDeserializer(
            schema_registry_client,
            schema_str=None  # Noneの場合、Schema Registryから取得
        )

        # Consumer設定
        consumer_conf = {
            'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS),
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }

        self.consumer = Consumer(consumer_conf)
        self.consumer.subscribe([topic])

        print(f"✅ Avro Consumer初期化完了")
        print(f"   トピック: {self.topic}")
        print(f"   グループID: {self.group_id}")
        print(f"   Schema Registry: {SCHEMA_REGISTRY_URL}")
        print(f"   購読開始...\n")

    def consume_messages(self):
        """メッセージを消費"""
        message_count = 0

        try:
            while self.running:
                # メッセージをポーリング（タイムアウト1秒）
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    raise KafkaException(msg.error())

                message_count += 1

                # Avroデシリアライズ
                user_data = self.avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )

                print(f"\n📨 メッセージ受信 #{message_count}")
                print(f"   トピック: {msg.topic()}")
                print(f"   パーティション: {msg.partition()}")
                print(f"   オフセット: {msg.offset()}")
                print(f"   キー: {msg.key().decode('utf-8') if msg.key() else None}")
                print(f"   ユーザーデータ:")
                print(f"      user_id: {user_data['user_id']}")
                print(f"      name: {user_data['name']}")
                print(f"      email: {user_data.get('email', 'N/A')}")
                print(f"      age: {user_data.get('age', 'N/A')}")
                print(f"      created_at: {user_data['created_at']}")

                # メッセージ処理
                self.process_user(user_data)

        except KeyboardInterrupt:
            print("\n⚠️  受信を中断しました")
        except Exception as e:
            print(f"\n❌ エラー: {e}")
        finally:
            self.close()

    def process_user(self, user_data):
        """ユーザーデータ処理ロジック"""
        # ここにビジネスロジックを実装
        # 例: データベースへの保存など

        # 年齢チェック
        age = user_data.get('age')
        if age:
            if age < 20:
                print(f"   🔞 未成年ユーザー")
            elif age >= 60:
                print(f"   👴 シニアユーザー")
            else:
                print(f"   👤 一般ユーザー")

    def close(self):
        """Consumerを閉じる"""
        self.consumer.close()
        print("\n👋 Avro Consumer終了")


def main():
    """使用例"""
    consumer = AvroConsumer()

    def signal_handler(sig, frame):
        print('\n⚠️  終了シグナルを受信しました...')
        consumer.running = False

    signal.signal(signal.SIGINT, signal_handler)

    print("="*60)
    print("Avroメッセージの受信を開始します")
    print("終了するにはCtrl+Cを押してください")
    print("="*60)

    # メッセージ消費開始
    consumer.consume_messages()


if __name__ == '__main__':
    main()
