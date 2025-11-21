"""
基本的なKafka Consumer
メッセージを継続的に受信する例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import signal
from common.config import CONSUMER_CONFIG, DEFAULT_TOPIC


class BasicConsumer:
    def __init__(self, topic=DEFAULT_TOPIC, group_id='python-consumer-group'):
        self.topic = topic
        self.group_id = group_id
        self.running = True

        # JSON用のデシリアライザーを設定
        config = CONSUMER_CONFIG.copy()
        config['group_id'] = group_id
        config['value_deserializer'] = lambda m: json.loads(m.decode('utf-8'))

        self.consumer = KafkaConsumer(topic, **config)

        print(f"✅ Consumer初期化完了")
        print(f"   トピック: {self.topic}")
        print(f"   グループID: {self.group_id}")
        print(f"   購読開始...\n")

    def consume_messages(self, max_messages=None):
        """メッセージを消費"""
        message_count = 0

        try:
            for message in self.consumer:
                if not self.running:
                    break

                message_count += 1

                print(f"\n📨 メッセージ受信 #{message_count}")
                print(f"   トピック: {message.topic}")
                print(f"   パーティション: {message.partition}")
                print(f"   オフセット: {message.offset}")
                print(f"   キー: {message.key}")
                print(f"   値: {json.dumps(message.value, indent=2, ensure_ascii=False)}")
                print(f"   タイムスタンプ: {message.timestamp}")

                # メッセージ処理（ここにビジネスロジックを追加）
                self.process_message(message.value)

                # 最大メッセージ数に達したら終了
                if max_messages and message_count >= max_messages:
                    print(f"\n✅ {max_messages}件のメッセージを処理しました")
                    break

        except KeyboardInterrupt:
            print("\n⚠️  受信を中断しました")
        finally:
            self.close()

    def process_message(self, data):
        """メッセージ処理ロジック"""
        # ここにビジネスロジックを実装
        # 例: データベースへの保存、外部APIへの送信など

        if 'action' in data:
            action = data['action']
            if action == 'login':
                print(f"   🔐 ログインイベント: {data.get('user_id')}")
            elif action == 'purchase':
                print(f"   💰 購入イベント: 金額={data.get('amount')}円")
            elif action == 'page_view':
                print(f"   👀 ページビュー: {data.get('page')}")

    def get_consumer_lag(self):
        """コンシューマーラグを確認"""
        partitions = self.consumer.assignment()

        if not partitions:
            print("パーティションが割り当てられていません")
            return

        print("\n📊 コンシューマーラグ情報:")
        for partition in partitions:
            # 現在のオフセット
            committed = self.consumer.committed(partition)
            current_offset = committed if committed else 0

            # エンドオフセット
            end_offsets = self.consumer.end_offsets([partition])
            end_offset = end_offsets[partition]

            # ラグ計算
            lag = end_offset - current_offset

            print(f"   パーティション {partition.partition}:")
            print(f"      現在オフセット: {current_offset}")
            print(f"      最新オフセット: {end_offset}")
            print(f"      ラグ: {lag}")

    def close(self):
        """Consumerを閉じる"""
        self.consumer.close()
        print("\n👋 Consumer終了")


def main():
    """使用例"""
    # Ctrl+Cでの終了をハンドリング
    consumer = BasicConsumer()

    def signal_handler(sig, frame):
        print('\n⚠️  終了シグナルを受信しました...')
        consumer.running = False

    signal.signal(signal.SIGINT, signal_handler)

    print("="*60)
    print("メッセージの受信を開始します")
    print("終了するにはCtrl+Cを押してください")
    print("="*60)

    # メッセージ消費開始（無限ループ）
    consumer.consume_messages()


if __name__ == '__main__':
    main()
