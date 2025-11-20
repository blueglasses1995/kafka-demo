# レッスン1: Kafka Producer

## 概要

Kafka Producerは、データをKafkaトピックに送信するクライアントアプリケーションです。アプリケーションからイベント、ログ、メトリクスなどのデータをKafkaに発行する役割を担います。

## メリット

### 1. 高スループット
- バッチング機能により、1秒間に数百万メッセージを送信可能
- 圧縮により、ネットワーク帯域幅を効率的に使用

### 2. 非同期処理
- メッセージ送信がアプリケーションをブロックしない
- コールバックやFutureで結果を受け取る

### 3. 柔軟な信頼性設定
- ACK設定により、パフォーマンスと信頼性のトレードオフを調整可能
- べき等性とトランザクションをサポート

### 4. 自動リトライ
- 一時的な障害時に自動的に再送信
- 設定可能なリトライ回数と間隔

### 5. パーティショニング制御
- カスタムパーティショナーでデータ分散を制御
- キーベースでメッセージの順序を保証

### 6. メトリクスとモニタリング
- 詳細なメトリクスを提供
- 送信レート、エラー率、レイテンシーなどを監視可能

## デメリット

### 1. 設定の複雑さ
- 多数の設定パラメータがあり、最適化が難しい
- 誤った設定はパフォーマンス低下やデータロスにつながる

### 2. メモリ管理
- バッファリングのためメモリを消費
- 大量送信時はOOMのリスク

### 3. 順序保証の制約
- デフォルトでは複数のパーティション間で順序が保証されない
- 順序保証のためにはパーティション数やin-flightリクエスト数を調整必要

### 4. エラーハンドリングの複雑さ
- 非同期処理のため、エラーハンドリングが複雑
- デッドレターキューなどの機構を自前で実装必要

### 5. ネットワーク障害への対応
- ブローカーダウン時の挙動を理解する必要
- バッファフル時のブロッキング動作に注意

## 技術的原理

### メッセージ送信の内部フロー

```
Application Code
    ↓
[1] Serializer（シリアライザー）
    ↓ バイト配列に変換
[2] Partitioner（パーティショナー）
    ↓ パーティション決定
[3] RecordAccumulator（レコードアキュムレータ）
    ↓ バッチバッファに蓄積
[4] Sender Thread（送信スレッド）
    ↓ ブローカーへ送信
[5] Broker（ブローカー）
    ↓ 永続化
[6] ACK返却
    ↓
Callback / Future完了
```

### 重要な設定パラメータ

#### パフォーマンス関連
```properties
# バッファメモリ（全体）
buffer.memory=33554432  # 32MB

# バッチサイズ（パーティションごと）
batch.size=16384  # 16KB

# バッチ待機時間
linger.ms=10  # 10ミリ秒

# 圧縮タイプ
compression.type=snappy  # none, gzip, snappy, lz4, zstd
```

#### 信頼性関連
```properties
# ACK設定
acks=all  # 0, 1, all(-1)

# リトライ回数
retries=2147483647  # 最大値（実質無制限）

# リトライ間隔
retry.backoff.ms=100

# べき等性
enable.idempotence=true

# タイムアウト
request.timeout.ms=30000
delivery.timeout.ms=120000
```

### シリアライザー

データをバイト配列に変換：

```
Object → Byte[]
```

標準シリアライザー:
- StringSerializer
- IntegerSerializer
- LongSerializer
- ByteArraySerializer
- AvroSerializer（要Schema Registry）

### パーティショニングロジック

```
if (key != null) {
    partition = hash(key) % num_partitions
} else {
    partition = round_robin()  // Kafka 2.4+: Sticky Partitioning
}
```

## ユースケース

### 1. ログ収集
アプリケーションログをKafkaに集約:
```
Web Server → Kafka → Elasticsearch/Splunk
```

### 2. メトリクス送信
システムメトリクスのリアルタイム送信:
```
Monitoring Agent → Kafka → Time Series DB
```

### 3. イベント駆動アーキテクチャ
ビジネスイベントの発行:
```
Order Service → Kafka → Inventory, Notification, Analytics
```

### 4. CDC (Change Data Capture)
データベース変更のストリーミング:
```
Database → Debezium → Kafka → Data Warehouse
```

### 5. IoTデータ収集
センサーデータの送信:
```
IoT Device → Gateway → Kafka → Analytics
```

## 実装例

### 基本的なProducer（Java）

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class BasicProducer {
    public static void main(String[] args) {
        // 1. 設定
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. Producerインスタンス作成
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // 3. レコード作成と送信
            for (int i = 0; i < 10; i++) {
                String key = "key-" + i;
                String value = "message-" + i;

                ProducerRecord<String, String> record =
                    new ProducerRecord<>("my-topic", key, value);

                // 同期送信（ブロッキング）
                RecordMetadata metadata = producer.send(record).get();

                System.out.printf("Sent: key=%s, partition=%d, offset=%d%n",
                    key, metadata.partition(), metadata.offset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 4. クリーンアップ
            producer.close();
        }
    }
}
```

### 非同期送信とコールバック

```java
public class AsyncProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 100; i++) {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>("my-topic", "key-" + i, "value-" + i);

                // 非同期送信 + コールバック
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            System.out.printf("Success: partition=%d, offset=%d%n",
                                metadata.partition(), metadata.offset());
                        } else {
                            System.err.println("Error: " + exception.getMessage());
                        }
                    }
                });
            }

            // すべてのメッセージを送信完了まで待つ
            producer.flush();

        } finally {
            producer.close();
        }
    }
}
```

### 信頼性の高いProducer設定

```java
public class ReliableProducer {
    public static void main(String[] args) {
        Properties props = new Properties();

        // 基本設定
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 信頼性設定
        props.put(ProducerConfig.ACKS_CONFIG, "all");  // すべてのレプリカから確認
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);  // 無制限リトライ
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // べき等性

        // パフォーマンス設定
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);  // 32KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);

        // タイムアウト設定
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 送信処理...
    }
}
```

### カスタムパーティショナー

```java
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);

        // VIPユーザーは常にパーティション0へ
        if (key.toString().startsWith("VIP")) {
            return 0;
        }

        // その他はハッシュベース
        return Math.abs(key.hashCode()) % (numPartitions - 1) + 1;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}

// 使用例
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
```

### Python実装例

```python
from kafka import KafkaProducer
import json

# Producerの作成
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',
    compression_type='snappy'
)

# メッセージ送信
for i in range(10):
    key = f'key-{i}'
    value = {'message': f'Hello {i}', 'timestamp': i}

    # 非同期送信
    future = producer.send('my-topic', key=key, value=value)

    # 結果を取得（ブロッキング）
    try:
        record_metadata = future.get(timeout=10)
        print(f'Sent to partition {record_metadata.partition}, offset {record_metadata.offset}')
    except Exception as e:
        print(f'Error: {e}')

# クリーンアップ
producer.flush()
producer.close()
```

## 実践演習

### 演習1: 基本的なProducer作成

1. Kafkaクラスターを起動
2. トピック`test-topic`を作成（3パーティション）
3. 100メッセージを送信するProducerを実装
4. 各メッセージのパーティションとオフセットを表示

### 演習2: パフォーマンス測定

異なる設定でスループットを比較:
- バッチサイズ: 1KB vs 32KB
- 圧縮: なし vs snappy
- ACK: 1 vs all

### 演習3: エラーハンドリング

1. ブローカーをシャットダウン
2. Producerのリトライ動作を観察
3. 適切なタイムアウトとエラーハンドリングを実装

## ベストプラクティス

### 1. リソース管理
```java
// try-with-resourcesを使用
try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
    // 送信処理
}  // 自動的にclose()が呼ばれる
```

### 2. べき等性の有効化
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
// 自動的に以下も設定される:
// acks=all
// retries=Integer.MAX_VALUE
// max.in.flight.requests.per.connection=5
```

### 3. 適切なバッチング
```java
// スループット重視
props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);

// レイテンシ重視
props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
```

### 4. メトリクスの監視
```java
Map<MetricName, ? extends Metric> metrics = producer.metrics();
for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
    System.out.println(entry.getKey().name() + ": " + entry.getValue().metricValue());
}
```

## トラブルシューティング

### 問題1: メッセージが送信されない

**原因**:
- ブローカーに接続できない
- バッファがフル

**解決策**:
```java
props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);  // タイムアウト設定
props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // バッファ増加
```

### 問題2: スループットが低い

**原因**:
- バッチサイズが小さい
- 圧縮が無効

**解決策**:
```java
props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
```

### 問題3: メッセージの重複

**原因**:
- べき等性が無効
- ネットワークリトライ

**解決策**:
```java
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

## まとめ

Kafka Producerの重要ポイント:

1. **非同期処理**によりアプリケーションをブロックしない
2. **バッチングと圧縮**で高スループットを実現
3. **ACK設定**で信頼性とパフォーマンスをバランス
4. **べき等性**でメッセージ重複を防止
5. **適切な設定**がパフォーマンスの鍵

次のレッスンでは、Consumerについて学びます。

---

**前へ**: [第2章: 技術的原理](../02-technical-principles.md) | **次へ**: [レッスン2: Consumer](./02-consumer.md)
