# Kafka Python Demo

PythonでApache Kafkaを使うための環境構築とサンプルコード集です。

## 📋 目次

- [必要要件](#必要要件)
- [クイックスタート](#クイックスタート)
- [サンプルコード](#サンプルコード)
- [Kafka管理UI](#kafka管理ui)
- [トラブルシューティング](#トラブルシューティング)

## 🚀 必要要件

- Docker & Docker Compose
- Python 3.8以上
- pip

## ⚡ クイックスタート

### 1. Kafkaクラスタの起動

```bash
# リポジトリをクローン
cd kafka-demo

# Docker ComposeでKafkaクラスタを起動
docker-compose up -d

# 起動確認（すべてのコンテナが"Up"になるまで待つ）
docker-compose ps
```

起動するサービス：
- **ZooKeeper** (ポート: 2181)
- **Kafka Broker** (ポート: 9092)
- **Schema Registry** (ポート: 8081)
- **Kafka UI** (ポート: 8080)

### 2. Python環境のセットアップ

```bash
# 仮想環境作成（推奨）
python3 -m venv venv
source venv/bin/activate  # Windowsの場合: venv\Scripts\activate

# 依存関係インストール
pip install -r requirements.txt
```

### 3. サンプル実行

#### 基本的なProducer/Consumer

```bash
# ターミナル1: Consumerを起動（メッセージ受信）
cd python-app
python consumer/basic_consumer.py

# ターミナル2: Producerを起動（メッセージ送信）
cd python-app
python producer/basic_producer.py
```

#### Avro形式のメッセージ送受信

```bash
# ターミナル1: Avro Consumerを起動
cd python-app
python consumer/avro_consumer.py

# ターミナル2: Avro Producerを起動
cd python-app
python producer/avro_producer.py
```

## 📝 サンプルコード

### Producer例

#### 1. basic_producer.py
基本的なメッセージ送信

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# メッセージ送信
producer.send('test-topic', {'message': 'Hello Kafka!'})
producer.flush()
```

機能:
- ✅ 単一メッセージ送信
- ✅ 複数メッセージ送信
- ✅ バッチ送信（非同期）
- ✅ 送信確認（同期/非同期）

#### 2. avro_producer.py
Avro形式でのメッセージ送信

機能:
- ✅ Schema Registry統合
- ✅ 型安全なメッセージ
- ✅ スキーマバージョン管理

### Consumer例

#### 1. basic_consumer.py
基本的なメッセージ受信

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"Received: {message.value}")
```

機能:
- ✅ 継続的なメッセージ受信
- ✅ 自動オフセット管理
- ✅ Consumer Groupサポート
- ✅ コンシューマーラグ確認

#### 2. avro_consumer.py
Avro形式のメッセージ受信

機能:
- ✅ Schema Registryからスキーマ自動取得
- ✅ 型安全なデシリアライズ

## 🎛️ Kafka管理UI

Kafkaクラスタの状態をブラウザで確認できます。

### Kafka UI

URL: http://localhost:8080

できること:
- トピック一覧表示・作成・削除
- メッセージの確認
- Consumer Group状態の確認
- Schema Registry管理
- ブローカー情報の確認

## 🔧 カスタマイズ

### 設定ファイル

`python-app/common/config.py`でKafka接続設定をカスタマイズできます。

```python
# Kafka接続設定
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Producer設定
PRODUCER_CONFIG = {
    'acks': 'all',  # 信頼性
    'retries': 3,
    'compression_type': 'snappy',
}

# Consumer設定
CONSUMER_CONFIG = {
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
}
```

### 新しいトピックの作成

```bash
# Docker Composeで起動している場合
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic my-new-topic \
  --partitions 3 \
  --replication-factor 1
```

## 🐛 トラブルシューティング

### Kafkaに接続できない

```bash
# Kafkaコンテナのログ確認
docker-compose logs kafka

# Kafkaが起動しているか確認
docker-compose ps

# 再起動
docker-compose restart kafka
```

### Schema Registryに接続できない

```bash
# Schema Registryのログ確認
docker-compose logs schema-registry

# 起動確認
curl http://localhost:8081/subjects
```

### メッセージが送信/受信できない

1. トピックが存在するか確認
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

2. Consumer Groupの状態確認
```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group python-consumer-group \
  --describe
```

3. Kafka UIで確認: http://localhost:8080

### Pythonパッケージのインストールエラー

```bash
# pipをアップグレード
pip install --upgrade pip

# 依存関係を再インストール
pip install -r requirements.txt --force-reinstall
```

## 📚 次のステップ

### Kafkaカリキュラム

詳細な学習資料は [kafka-curriculum](./kafka-curriculum/README.md) を参照してください。

内容:
- Apache Kafkaの全体像
- Producer/Consumerの詳細
- Topics & Partitions
- Consumer Groups
- Replication
- Kafka Connect
- Kafka Streams
- ksqlDB
- Schema Registry
- Monitoring & Operations

### 実践プロジェクト

以下のようなプロジェクトに挑戦してみましょう:

1. **リアルタイムログ収集システム**
   - アプリケーションログをKafkaに送信
   - Elasticsearchに保存
   - Kibanaで可視化

2. **ストリーム処理パイプライン**
   - センサーデータの収集
   - リアルタイム集計
   - 異常検知

3. **マイクロサービス連携**
   - イベント駆動アーキテクチャ
   - 非同期メッセージング
   - CQRS実装

## 🛑 環境の停止とクリーンアップ

```bash
# Kafkaクラスタ停止（データは保持）
docker-compose stop

# Kafkaクラスタ停止＋削除（データも削除）
docker-compose down

# ボリュームも含めて完全削除
docker-compose down -v
```

## 📄 ライセンス

MIT License

## 🤝 コントリビューション

Issue・Pull Requestをお待ちしています！

---

Happy Kafka Learning! 🎉
