# レッスン2: Kafka Consumer

## 概要

Kafka Consumerは、Kafkaトピックからデータを読み取るクライアントアプリケーションです。トピックを購読し、メッセージを取得して処理します。

## メリット

### 1. スケーラブルな並列処理
- Consumer Groupにより複数コンシューマーで負荷分散
- パーティション単位で並列処理
- 動的にコンシューマーを追加・削除可能

### 2. オフセット管理
- 処理済み位置を自動/手動で管理
- 障害発生時に処理を再開可能
- At-least-once配信を保証

### 3. 柔軟な読み取り
- リアルタイム読み取り（最新データ）
- 過去データの再処理（任意のオフセットから）
- タイムスタンプベースの読み取り

### 4. 耐障害性
- コンシューマー障害時に自動リバランス
- パーティション再割り当てで処理継続
- オフセットコミットでデータロスを防止

### 5. バックプレッシャー制御
- フェッチサイズと頻度を制御
- 処理能力に応じてメッセージ取得量を調整
- メモリオーバーフローを防止

### 6. 複数トピック購読
- 単一コンシューマーで複数トピックを購読
- パターンマッチングでトピック動的購読

## デメリット

### 1. リバランスのオーバーヘッド
- コンシューマー追加/削除時に全体が一時停止
- リバランス中は処理が進まない
- 大規模グループでは時間がかかる

### 2. 重複処理のリスク
- オフセットコミット前の障害で重複
- At-least-once配信のため、べき等処理が必要
- Exactly-once実装の複雑さ

### 3. パーティション制約
- コンシューマー数はパーティション数まで
- パーティション数以上のコンシューマーは遊休状態
- スケーリングにパーティション追加が必要

### 4. オフセット管理の複雑さ
- 自動コミットのタイミング調整が難しい
- 手動コミットはコードが複雑に
- コミット失敗時のハンドリング

### 5. レイテンシ
- ポーリングベースのため最小レイテンシが存在
- プッシュ型に比べてリアルタイム性が低い
- fetch.min.bytesで待機が発生する可能性

## 技術的原理

### メッセージ取得の内部フロー

```
[1] Consumer.poll() 呼び出し
    ↓
[2] Fetcher: ブローカーにFetchRequest送信
    ↓
[3] Broker: メッセージを返却
    ↓
[4] Deserializer: バイト配列をオブジェクトに変換
    ↓
[5] アプリケーション: メッセージ処理
    ↓
[6] オフセットコミット（自動 or 手動）
    ↓
[7] __consumer_offsets トピックに保存
```

### オフセット管理

```
Partition: [0] [1] [2] [3] [4] [5] [6] [7] [8] [9]
                        ↑           ↑
                  Committed      Current
                  Offset         Position
```

- **Current Position**: 次に読み取るオフセット
- **Committed Offset**: 最後にコミットしたオフセット
- 再起動時はCommitted Offsetから再開

### ポーリングループ

```java
while (true) {
    // 1. メッセージをフェッチ（最大poll.timeout.msまで待機）
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));

    // 2. メッセージ処理
    for (ConsumerRecord<K, V> record : records) {
        process(record);
    }

    // 3. オフセットコミット
    consumer.commitSync();
}
```

### 重要な設定パラメータ

#### 基本設定
```properties
# ブローカー接続
bootstrap.servers=localhost:9092

# グループID（必須）
group.id=my-consumer-group

# デシリアライザー
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

#### オフセット管理
```properties
# 自動コミット有効/無効
enable.auto.commit=true

# 自動コミット間隔
auto.commit.interval.ms=5000

# 初期オフセット位置（earliest: 最初から, latest: 最新から）
auto.offset.reset=latest
```

#### フェッチ設定
```properties
# 1回のpollで返す最大レコード数
max.poll.records=500

# 最小フェッチサイズ（これ以下だと待機）
fetch.min.bytes=1

# 最大待機時間
fetch.max.wait.ms=500

# パーティションあたりの最大フェッチサイズ
max.partition.fetch.bytes=1048576  # 1MB
```

#### セッション管理
```properties
# セッションタイムアウト
session.timeout.ms=10000

# ハートビート間隔
heartbeat.interval.ms=3000

# poll()呼び出しの最大間隔
max.poll.interval.ms=300000  # 5分
```

### Consumer Group調整プロトコル

```
Consumer Group: my-app
Coordinator: Broker 1

[1] Consumer起動 → JoinGroup Request
[2] Coordinator → リバランス開始
[3] すべてのConsumerにパーティション割り当て
[4] Consumer → SyncGroup Request
[5] Coordinator → 割り当て情報返却
[6] Consumer → 処理開始
[7] 定期的にハートビート送信
```

## ユースケース

### 1. リアルタイム処理
```
Kafka → Consumer → データベース更新
```
例: ユーザーアクションをリアルタイムで処理

### 2. ETLパイプライン
```
Kafka → Consumer → 変換処理 → Data Warehouse
```
例: ログデータを変換してBigQueryに保存

### 3. マイクロサービス連携
```
Order Service → Kafka → Inventory Consumer → 在庫更新
```
例: 注文イベントを購読して在庫を減らす

### 4. 分析処理
```
Kafka → Consumer → 集計・分析 → ダッシュボード
```
例: メトリクスを集計してリアルタイムダッシュボード表示

### 5. アーカイブ
```
Kafka → Consumer → S3/HDFS
```
例: すべてのイベントを長期保存

## 実装例

### 基本的なConsumer（Java）

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BasicConsumer {
    public static void main(String[] args) {
        // 1. 設定
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. Consumerインスタンス作成
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            // 3. トピック購読
            consumer.subscribe(Collections.singletonList("my-topic"));

            // 4. ポーリングループ
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic=%s, partition=%d, offset=%d, key=%s, value=%s%n",
                        record.topic(), record.partition(), record.offset(),
                        record.key(), record.value());
                }
            }
        } finally {
            // 5. クリーンアップ
            consumer.close();
        }
    }
}
```

### 手動オフセットコミット

```java
public class ManualCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // 自動コミット無効

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // メッセージ処理
                    processRecord(record);
                }

                // すべてのメッセージ処理後にコミット（同期）
                consumer.commitSync();
            }
        } catch (CommitFailedException e) {
            System.err.println("Commit failed: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        // ビジネスロジック
        System.out.println("Processing: " + record.value());
    }
}
```

### 非同期コミット

```java
public class AsyncCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "async-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                // 非同期コミット（ブロックしない）
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                          Exception exception) {
                        if (exception != null) {
                            System.err.println("Commit failed: " + exception.getMessage());
                        } else {
                            System.out.println("Commit succeeded: " + offsets);
                        }
                    }
                });
            }
        } finally {
            // 終了時は同期コミットで確実に
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        System.out.println("Processing: " + record.value());
    }
}
```

### パーティション単位のオフセット管理

```java
public class PartitionCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "partition-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // パーティションごとに処理
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        processRecord(record);
                    }

                    // 各パーティションの最後のオフセットをコミット
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    Map<TopicPartition, OffsetAndMetadata> commitOffset = new HashMap<>();
                    commitOffset.put(partition, new OffsetAndMetadata(lastOffset + 1));

                    consumer.commitSync(commitOffset);
                    System.out.println("Committed partition " + partition + " up to offset " + lastOffset);
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        System.out.println("Processing: " + record.value());
    }
}
```

### リバランスリスナー

```java
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import java.util.Collection;

public class RebalanceConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rebalance-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // リバランスリスナー
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // パーティションが取り上げられる前
                System.out.println("Partitions revoked: " + partitions);
                // 処理中のメッセージをコミット
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // 新しいパーティションが割り当てられた後
                System.out.println("Partitions assigned: " + partitions);
            }
        };

        consumer.subscribe(Collections.singletonList("my-topic"), listener);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        System.out.println("Processing: " + record.value());
    }
}
```

### Python実装例

```python
from kafka import KafkaConsumer
import json

# Consumerの作成
consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    group_id='my-python-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: m.decode('utf-8') if m else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000
)

# メッセージの消費
try:
    for message in consumer:
        print(f'Topic: {message.topic}')
        print(f'Partition: {message.partition}')
        print(f'Offset: {message.offset}')
        print(f'Key: {message.key}')
        print(f'Value: {message.value}')
        print('---')

except KeyboardInterrupt:
    print('Interrupted')

finally:
    consumer.close()
```

### 特定オフセットからの読み取り

```java
public class SeekConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "seek-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        TopicPartition partition = new TopicPartition("my-topic", 0);
        consumer.assign(Collections.singletonList(partition));

        // 特定のオフセットから読み取り
        consumer.seek(partition, 100);

        // または最初から
        // consumer.seekToBeginning(Collections.singletonList(partition));

        // または最新から
        // consumer.seekToEnd(Collections.singletonList(partition));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset=%d, key=%s, value=%s%n",
                        record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
```

## 実践演習

### 演習1: 基本的なConsumer作成

1. Lesson 1で作成したProducerでメッセージ送信
2. Consumerでメッセージを消費
3. オフセットの変化を観察

### 演習2: 手動コミット実装

1. 自動コミットを無効化
2. 10メッセージごとにコミット
3. 途中でConsumerを停止し、再開時の動作を確認

### 演習3: Consumer Group動作確認

1. 同じGroupIDで複数Consumerを起動
2. パーティション割り当てを観察
3. 1つのConsumerを停止してリバランスを確認

## ベストプラクティス

### 1. 適切なpoll()間隔
```java
// 長すぎるとmax.poll.interval.msに引っかかる
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    // 重い処理はバックグラウンドスレッドで
    processRecordsAsync(records);
}
```

### 2. Graceful Shutdown
```java
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    System.out.println("Shutting down gracefully...");
    consumer.wakeup();  // poll()を中断
}));

try {
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        // 処理
    }
} catch (WakeupException e) {
    // 正常終了
} finally {
    consumer.close();
}
```

### 3. べき等処理の実装
```java
private void processRecord(ConsumerRecord<String, String> record) {
    String idempotencyKey = record.topic() + "-" + record.partition() + "-" + record.offset();

    if (processedRecords.contains(idempotencyKey)) {
        System.out.println("Already processed, skipping");
        return;
    }

    // ビジネスロジック
    doBusinessLogic(record.value());

    // 処理済みとして記録
    processedRecords.add(idempotencyKey);
}
```

### 4. エラーハンドリング
```java
while (true) {
    try {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
            try {
                processRecord(record);
            } catch (RecoverableException e) {
                // リトライ可能なエラー
                retryProcess(record);
            } catch (NonRecoverableException e) {
                // リトライ不可能なエラー
                sendToDeadLetterQueue(record);
            }
        }

        consumer.commitSync();

    } catch (Exception e) {
        System.err.println("Fatal error: " + e.getMessage());
        break;
    }
}
```

## トラブルシューティング

### 問題1: リバランスが頻繁に発生

**原因**:
- poll()間隔が長い
- max.poll.interval.msを超過

**解決策**:
```properties
max.poll.interval.ms=600000  # 10分に延長
max.poll.records=100  # 1回の処理量を削減
```

### 問題2: メッセージの重複処理

**原因**:
- オフセットコミット前に障害
- At-least-once配信

**解決策**:
- べき等処理を実装
- トランザクション使用
- 重複検出機構の実装

### 問題3: 処理が遅い

**原因**:
- Consumer数が少ない
- パーティション数が少ない

**解決策**:
- Consumer数を増やす（パーティション数まで）
- トピックのパーティション数を増やす

## まとめ

Kafka Consumerの重要ポイント:

1. **Consumer Group**で並列処理とスケーリング
2. **オフセット管理**で処理位置を追跡
3. **手動コミット**で正確な処理保証
4. **リバランス**を理解して適切にハンドリング
5. **べき等処理**でAt-least-onceに対応

次のレッスンでは、TopicsとPartitionsについて詳しく学びます。

---

**前へ**: [レッスン1: Producer](./01-producer.md) | **次へ**: [レッスン3: Topics & Partitions](./03-topics-partitions.md)
