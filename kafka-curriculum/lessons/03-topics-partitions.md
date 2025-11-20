# レッスン3: Topics & Partitions

## 概要

TopicはKafkaのメッセージカテゴリであり、Partitionはトピックの物理的な分割単位です。この2つの概念は、Kafkaのスケーラビリティと並列処理の基盤となっています。

## メリット

### 1. スケーラビリティ
- パーティション分割により水平スケーリング
- 各パーティションは独立して処理可能
- クラスター全体で負荷分散

### 2. 並列処理
- 複数プロデューサーが並行書き込み
- 複数コンシューマーが並行読み取り
- パーティション数に応じてスループット向上

### 3. 順序保証
- パーティション内でメッセージ順序を保証
- キーベースで同じパーティションに送信
- イベント順序が重要なケースに対応

### 4. データ分離
- トピックごとに保持ポリシーを設定
- 異なるアクセス制御を適用
- 論理的なデータ分類

### 5. 耐障害性
- パーティションレプリケーションで冗長化
- ブローカー障害時も他のパーティションは継続
- データロスを防止

### 6. パフォーマンスチューニング
- パーティション数でスループット調整
- セグメントサイズでディスクI/O最適化
- 圧縮設定でストレージ効率化

## デメリット

### 1. パーティション数の選択が難しい
- 多すぎるとメタデータオーバーヘッド
- 少なすぎるとスケーラビリティ制限
- 後から増やすとリバランス発生

### 2. 順序保証の制約
- パーティション間の順序は保証されない
- グローバル順序が必要な場合は1パーティション
- パフォーマンスとのトレードオフ

### 3. リバランスのコスト
- パーティション追加時にConsumer Groupがリバランス
- データ再配置が必要
- 一時的な処理停止

### 4. ストレージ管理
- 各パーティションがディスク領域を消費
- パーティション数が多いとファイル数が膨大
- ZooKeeper/KRaftメタデータ増加

### 5. ホットパーティション
- 不適切なキー選択でパーティション偏り
- 特定パーティションへの負荷集中
- パフォーマンス低下

## 技術的原理

### トピックとパーティションの関係

```
Topic: user-events (4 partitions)

Partition 0: [msg0] [msg4] [msg8]  [msg12] ...
Partition 1: [msg1] [msg5] [msg9]  [msg13] ...
Partition 2: [msg2] [msg6] [msg10] [msg14] ...
Partition 3: [msg3] [msg7] [msg11] [msg15] ...
```

### パーティションの物理構造

```
/var/lib/kafka/data/
├── user-events-0/
│   ├── 00000000000000000000.log       # セグメント
│   ├── 00000000000000000000.index     # オフセットインデックス
│   ├── 00000000000000000000.timeindex # タイムインデックス
│   ├── 00000000000000100000.log
│   ├── 00000000000000100000.index
│   └── 00000000000000100000.timeindex
├── user-events-1/
│   ├── 00000000000000000000.log
│   └── ...
├── user-events-2/
└── user-events-3/
```

### セグメントの仕組み

```
Partition内部:

Active Segment (書き込み中)
├── 00000000000000200000.log     [現在書き込み中]

Closed Segments (読み取り専用)
├── 00000000000000000000.log     [0 - 99,999]
├── 00000000000000100000.log     [100,000 - 199,999]
```

セグメント切り替え条件:
- サイズが閾値に達した（log.segment.bytes）
- 時間が経過した（log.segment.ms）

### パーティショニング戦略

#### 1. キーベースパーティショニング
```
hash(key) % num_partitions = partition_id

例:
key="user123" → hash → 12345 % 4 = 1 → Partition 1
key="user456" → hash → 67890 % 4 = 2 → Partition 2
```

#### 2. ラウンドロビン（キーなし）
```
Kafka 2.4以降: Sticky Partitioning
バッチが埋まるまで同じパーティションに送信
→ バッチ効率向上
```

#### 3. カスタムパーティショナー
```java
// 地域別パーティショニング
if (key.startsWith("US")) return 0;
if (key.startsWith("EU")) return 1;
if (key.startsWith("ASIA")) return 2;
```

### トピック設定パラメータ

```properties
# セグメント設定
log.segment.bytes=1073741824        # 1GB
log.segment.ms=604800000            # 7日

# 保持ポリシー
log.retention.bytes=-1              # 無制限
log.retention.ms=604800000          # 7日

# クリーンアップポリシー
log.cleanup.policy=delete           # delete or compact

# 圧縮設定
compression.type=producer           # producer, gzip, snappy, lz4, zstd, none

# レプリケーション
min.insync.replicas=2
```

## ユースケース

### 1. イベントストリーム（多パーティション）
```
Topic: clickstream (100 partitions)
理由: 高スループット、順序不要
```

### 2. トランザクションログ（少パーティション、キーあり）
```
Topic: bank-transactions (10 partitions)
Key: account-id
理由: アカウントごとの順序保証
```

### 3. CDC（キーベース）
```
Topic: database-changes (20 partitions)
Key: primary-key
理由: 同じレコードの変更は順序保証
```

### 4. グローバル順序（1パーティション）
```
Topic: audit-log (1 partition)
理由: すべてのイベントの完全な順序が必要
```

### 5. 地域別分離（カスタムパーティショニング）
```
Topic: user-data (3 partitions)
Partition 0: US users
Partition 1: EU users (GDPR対応)
Partition 2: ASIA users
```

## 実装例

### トピック作成（CLI）

```bash
# 基本的なトピック作成
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 2

# 詳細設定付きトピック作成
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 10 \
  --replication-factor 3 \
  --config retention.ms=86400000 \
  --config segment.bytes=536870912 \
  --config compression.type=snappy \
  --config min.insync.replicas=2

# トピック一覧表示
kafka-topics.sh --list \
  --bootstrap-server localhost:9092

# トピック詳細表示
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

### トピック作成（Java Admin API）

```java
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicCreation {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. AdminClient作成
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {

            // 2. トピック設定
            String topicName = "my-new-topic";
            int numPartitions = 5;
            short replicationFactor = 2;

            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "86400000");        // 1日
            topicConfig.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");     // 1GB
            topicConfig.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy");
            topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");

            // 3. NewTopic作成
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor)
                .configs(topicConfig);

            // 4. トピック作成
            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));

            // 5. 結果確認
            result.all().get();
            System.out.println("Topic created successfully: " + topicName);

        } catch (TopicExistsException e) {
            System.err.println("Topic already exists");
        }
    }
}
```

### トピック詳細取得

```java
public class TopicDescription {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {

            // トピック詳細取得
            DescribeTopicsResult result = adminClient.describeTopics(
                Collections.singletonList("my-topic")
            );

            TopicDescription description = result.all().get().get("my-topic");

            System.out.println("Topic: " + description.name());
            System.out.println("Partitions: " + description.partitions().size());

            for (TopicPartitionInfo partition : description.partitions()) {
                System.out.println("\nPartition " + partition.partition() + ":");
                System.out.println("  Leader: " + partition.leader().id());
                System.out.print("  Replicas: ");
                partition.replicas().forEach(node -> System.out.print(node.id() + " "));
                System.out.print("\n  ISR: ");
                partition.isr().forEach(node -> System.out.print(node.id() + " "));
                System.out.println();
            }
        }
    }
}
```

### パーティション数変更

```bash
# パーティション数を増やす（減らすことはできない）
kafka-topics.sh --alter \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 10
```

```java
public class AlterPartitions {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {

            // パーティション数を10に増やす
            Map<String, NewPartitions> newPartitions = new HashMap<>();
            newPartitions.put("my-topic", NewPartitions.increaseTo(10));

            CreatePartitionsResult result = adminClient.createPartitions(newPartitions);
            result.all().get();

            System.out.println("Partitions increased successfully");
        }
    }
}
```

### トピック設定変更

```bash
# 設定変更
kafka-configs.sh --alter \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic \
  --add-config retention.ms=172800000,compression.type=lz4

# 設定確認
kafka-configs.sh --describe \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name my-topic
```

```java
public class AlterConfig {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "my-topic");

            // 設定変更
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            Collection<AlterConfigOp> ops = Arrays.asList(
                new AlterConfigOp(
                    new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "172800000"),
                    AlterConfigOp.OpType.SET
                ),
                new AlterConfigOp(
                    new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4"),
                    AlterConfigOp.OpType.SET
                )
            );

            configs.put(resource, ops);

            AlterConfigsResult result = adminClient.incrementalAlterConfigs(configs);
            result.all().get();

            System.out.println("Config altered successfully");
        }
    }
}
```

### 特定パーティションへの送信

```java
public class PartitionSpecificProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "my-topic";

        // パーティションを明示的に指定
        ProducerRecord<String, String> record1 =
            new ProducerRecord<>(topic, 0, "key1", "value1");  // Partition 0へ

        ProducerRecord<String, String> record2 =
            new ProducerRecord<>(topic, 2, "key2", "value2");  // Partition 2へ

        try {
            producer.send(record1).get();
            producer.send(record2).get();
            System.out.println("Messages sent to specific partitions");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
```

### パーティション情報の取得

```java
public class PartitionInfo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "my-topic";

        // パーティション情報取得
        List<org.apache.kafka.common.PartitionInfo> partitions = producer.partitionsFor(topic);

        System.out.println("Topic: " + topic);
        System.out.println("Partition count: " + partitions.size());

        for (org.apache.kafka.common.PartitionInfo partition : partitions) {
            System.out.println("\nPartition: " + partition.partition());
            System.out.println("  Leader: " + partition.leader().id());
            System.out.println("  Replicas: " + partition.replicas().length);
        }

        producer.close();
    }
}
```

## 実践演習

### 演習1: パーティション数とスループット

1. 1, 3, 10パーティションのトピックを作成
2. 各トピックに10万メッセージ送信
3. スループットを測定・比較

### 演習2: 順序保証の確認

1. 3パーティションのトピック作成
2. 同じキーで10メッセージ送信
3. すべて同じパーティションに到達することを確認

### 演習3: パーティション追加の影響

1. 3パーティションのトピック作成
2. 複数Consumerで消費開始
3. パーティションを5に増やす
4. リバランス動作を観察

## ベストプラクティス

### 1. パーティション数の選択

```
# 計算式
max(目標スループット / プロデューサースループット,
    目標スループット / コンシューマースループット)

例:
目標: 100 MB/s
Producer: 10 MB/s/partition
Consumer: 20 MB/s/partition

Partitions = max(100/10, 100/20) = max(10, 5) = 10
```

### 2. キー選択

```java
// 良い例: 均等分散
String key = userId;  // ユーザーIDはランダム

// 悪い例: 偏り
String key = userType;  // "premium" vs "free" で偏りが発生
```

### 3. トピック命名規則

```
<domain>.<entity>.<event-type>

例:
- ecommerce.orders.created
- ecommerce.orders.updated
- ecommerce.inventory.depleted
- analytics.pageviews.raw
```

### 4. 設定のチューニング

```properties
# 高スループット
segment.bytes=1073741824       # 1GB
compression.type=snappy
batch.size=32768

# 長期保存
retention.ms=2592000000        # 30日
segment.ms=86400000            # 1日

# CDC
cleanup.policy=compact
min.cleanable.dirty.ratio=0.5
```

## トラブルシューティング

### 問題1: パーティション偏り

**診断**:
```bash
kafka-consumer-groups.sh --describe \
  --bootstrap-server localhost:9092 \
  --group my-group
```

**解決策**:
- キー選択を見直す
- カスタムパーティショナー実装

### 問題2: パーティション数不足

**症状**: Consumer追加してもスループット向上しない

**解決策**:
```bash
kafka-topics.sh --alter \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 20
```

### 問題3: セグメントファイル多すぎ

**症状**: ファイル数が多くOSリミット到達

**解決策**:
```properties
segment.bytes=2147483648  # 2GBに増加
log.retention.ms=172800000  # 保持期間短縮
```

## まとめ

TopicsとPartitionsの重要ポイント:

1. **パーティション数**がスケーラビリティを決定
2. **キーベース**パーティショニングで順序保証
3. **適切な設定**でパフォーマンス最適化
4. **命名規則**で管理性向上
5. **監視**で偏りを早期発見

次のレッスンでは、Consumer Groupsについて詳しく学びます。

---

**前へ**: [レッスン2: Consumer](./02-consumer.md) | **次へ**: [レッスン4: Consumer Groups](./04-consumer-groups.md)
