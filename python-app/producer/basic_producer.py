"""
基本的なKafka Producer
シンプルなメッセージ送信の例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from datetime import datetime
from common.config import PRODUCER_CONFIG, DEFAULT_TOPIC


class BasicProducer:
    def __init__(self, topic=DEFAULT_TOPIC):
        self.topic = topic
        # JSON用のシリアライザーを設定
        config = PRODUCER_CONFIG.copy()
        config['value_serializer'] = lambda v: json.dumps(v).encode('utf-8')

        self.producer = KafkaProducer(**config)
        print(f"✅ Producer初期化完了 (トピック: {self.topic})")

    def send_message(self, key, value):
        """メッセージを同期送信"""
        try:
            # メッセージ送信
            future = self.producer.send(
                self.topic,
                key=key,
                value=value
            )

            # 送信完了を待つ（同期）
            record_metadata = future.get(timeout=10)

            print(f"✅ 送信成功:")
            print(f"   トピック: {record_metadata.topic}")
            print(f"   パーティション: {record_metadata.partition}")
            print(f"   オフセット: {record_metadata.offset}")
            print(f"   キー: {key}")
            print(f"   値: {value}")

            return True

        except KafkaError as e:
            print(f"❌ 送信エラー: {e}")
            return False

    def send_async(self, key, value):
        """メッセージを非同期送信（コールバック付き）"""
        def on_success(record_metadata):
            print(f"✅ 非同期送信成功: partition={record_metadata.partition}, offset={record_metadata.offset}")

        def on_error(error):
            print(f"❌ 非同期送信失敗: {error}")

        # 非同期送信
        self.producer.send(self.topic, key=key, value=value).add_callback(
            on_success
        ).add_errback(on_error)

    def send_batch(self, messages):
        """バッチ送信"""
        print(f"\n📦 {len(messages)}件のメッセージをバッチ送信します...")

        for i, msg in enumerate(messages):
            self.send_async(msg['key'], msg['value'])

            # 進捗表示
            if (i + 1) % 10 == 0:
                print(f"   {i + 1}/{len(messages)} 送信中...")

        # すべてのメッセージを送信するまで待つ
        self.producer.flush()
        print(f"✅ バッチ送信完了: {len(messages)}件")

    def close(self):
        """Producerを閉じる"""
        self.producer.close()
        print("👋 Producer終了")


def main():
    """使用例"""
    producer = BasicProducer()

    try:
        print("\n" + "="*60)
        print("1. 単一メッセージ送信")
        print("="*60)

        # 単一メッセージ送信
        message = {
            'user_id': 'user001',
            'action': 'login',
            'timestamp': datetime.now().isoformat()
        }
        producer.send_message(key='user001', value=message)

        time.sleep(1)

        print("\n" + "="*60)
        print("2. 複数メッセージ送信")
        print("="*60)

        # 複数メッセージ送信
        for i in range(5):
            message = {
                'user_id': f'user{i:03d}',
                'action': 'page_view',
                'page': f'/page/{i}',
                'timestamp': datetime.now().isoformat()
            }
            producer.send_message(key=f'user{i:03d}', value=message)
            time.sleep(0.5)

        print("\n" + "="*60)
        print("3. バッチ送信（非同期）")
        print("="*60)

        # バッチ送信
        batch_messages = []
        for i in range(20):
            batch_messages.append({
                'key': f'batch_user{i:03d}',
                'value': {
                    'user_id': f'batch_user{i:03d}',
                    'action': 'purchase',
                    'amount': (i + 1) * 100,
                    'timestamp': datetime.now().isoformat()
                }
            })

        producer.send_batch(batch_messages)

        print("\n✨ すべての送信が完了しました！")

    except KeyboardInterrupt:
        print("\n⚠️  中断されました")
    finally:
        producer.close()


if __name__ == '__main__':
    main()
