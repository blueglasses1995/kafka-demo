# レッスン4: Consumer Groups

## 概要

Consumer Groupは、複数のConsumerインスタンスをグループ化し、トピックのパーティションを分散して処理するための機能です。スケーラビリティと耐障害性を実現する中核的な概念です。

## メリット

### 1. スケーラブルな並列処理
- 複数Consumerでパーティションを分散処理
- Consumer追加で処理能力を簡単に向上
- 動的なスケールアウト/スケールイン

### 2. 高可用性
- Consumer障害時に自動的にリバランス
- パーティション再割り当てで処理継続
- ダウンタイム最小化

### 3. 負荷分散
- パーティションを均等に割り当て
- 各Consumerの負荷を自動調整
- ホットスポット回避

### 4. メッセージ配信保証
- 各パーティションは1つのConsumerのみが処理
- メッセージ重複なし（グループ内）
- At-least-once配信保証

### 5. 独立した処理
- 異なるConsumer Groupは独立して動作
- 同じトピックを複数アプリケーションで消費可能
- マイクロサービスアーキテクチャに最適

### 6. オフセット管理
- グループごとにオフセットを管理
- グループ内で処理進捗を共有
- 障害復旧が容易

## デメリット

### 1. リバランスのオーバーヘッド
- Consumer追加/削除時に全Consumerが一時停止
- リバランス中は処理が進まない
- 大規模グループでは数秒〜数十秒かかる

### 2. パーティション制約
- Consumer数はパーティション数まで有効
- パーティション数より多いConsumerは遊休
- スケーリングにパーティション増加が必要

### 3. リバランス中のメッセージ重複
- リバランス前のオフセットコミット失敗で重複
- 処理中メッセージの再処理
- べき等処理の実装が必要

### 4. セッション管理の複雑さ
- タイムアウト設定の調整が必要
- ネットワーク遅延への対応
- ハートビート失敗で不要なリバランス

### 5. デバッグの難しさ
- 複数Consumerの状態把握が困難
- リバランスタイミングの予測困難
- ログ分散による原因究明の難しさ

## 技術的原理

### Consumer Groupの基本構造

```
Topic: orders (4 partitions)
Consumer Group: order-processors (3 consumers)

Partition 0 → Consumer 1
Partition 1 → Consumer 1
Partition 2 → Consumer 2
Partition 3 → Consumer 3
```

### パーティション割り当て戦略

#### 1. Range Assignor（デフォルト）
```
Topic A (6 partitions), Topic B (6 partitions)
Consumers: 3

Consumer 1: A0, A1, B0, B1
Consumer 2: A2, A3, B2, B3
Consumer 3: A4, A5, B4, B5
```
トピックごとに連続パーティションを割り当て

#### 2. Round Robin Assignor
```
Topic A (6 partitions), Topic B (6 partitions)
Consumers: 3

Consumer 1: A0, A3, B0, B3
Consumer 2: A1, A4, B1, B4
Consumer 3: A2, A5, B2, B5
```
すべてのパーティションを順番に割り当て

#### 3. Sticky Assignor
- 可能な限り既存の割り当てを維持
- リバランスのオーバーヘッド削減
- Consumer追加時の再割り当て最小化

#### 4. Cooperative Sticky Assignor（推奨）
- インクリメンタルリバランス
- すべてのConsumerを停止せず、必要な部分のみリバランス
- Kafka 2.4以降

### リバランスプロトコル

#### Eager Rebalancing（従来）
```
[1] リバランストリガー（Consumer追加/削除）
    ↓
[2] すべてのConsumerがパーティション解放
    ↓
[3] Group Coordinator がパーティション再割り当て
    ↓
[4] すべてのConsumerが新パーティションを取得
    ↓
[5] 処理再開

問題: [2]〜[4]の間、全体が停止
```

#### Incremental Cooperative Rebalancing（推奨）
```
[1] リバランストリガー
    ↓
[2] 影響を受けるパーティションのみ解放
    ↓
[3] 他のConsumerは処理継続
    ↓
[4] 解放されたパーティションのみ再割り当て
    ↓
[5] 全Consumer処理中

利点: ダウンタイム最小化
```

### Group Coordinatorの役割

```
Broker (Group Coordinator)
    ↑
    | ハートビート
    |
Consumer Group
├── Consumer 1 → ハートビート送信（3秒ごと）
├── Consumer 2 → ハートビート送信
└── Consumer 3 → ハートビート送信

CoordinatorはConsumerの生存確認とリバランス調整
```

### 重要な設定パラメータ

```properties
# Consumer Group ID（必須）
group.id=my-consumer-group

# セッションタイムアウト（ハートビート途絶からリバランスまで）
session.timeout.ms=10000  # 10秒

# ハートビート間隔
heartbeat.interval.ms=3000  # 3秒（session.timeout.msの1/3推奨）

# poll()呼び出しの最大間隔（これを超えるとConsumer離脱）
max.poll.interval.ms=300000  # 5分

# パーティション割り当て戦略
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# リバランス時の動作
group.instance.id=consumer-1  # Static membership用（オプション）
```

## ユースケース

### 1. マイクロサービス並列処理
```
Order Topic
    ↓
Order Processing Service (Consumer Group)
├── Instance 1
├── Instance 2
└── Instance 3

各インスタンスが独立して注文を処理
```

### 2. 複数アプリケーションでの消費
```
Event Topic
    ├→ Analytics Group → ダッシュボード
    ├→ Archiving Group → S3保存
    └→ Notification Group → メール送信

同じイベントを異なる目的で処理
```

### 3. アクティブ/スタンバイ構成
```
Consumer Group: critical-processor (2 consumers)
Topic: alerts (1 partition)

Consumer 1: アクティブ（パーティション0処理）
Consumer 2: スタンバイ（待機、Consumer 1障害時に引き継ぎ）
```

### 4. データパイプライン
```
Raw Data Topic
    ↓
ETL Consumer Group (10 instances)
    ↓
Processed Data Topic

並列でデータ変換処理
```

### 5. A/Bテスト
```
User Events Topic
    ├→ Algorithm A Group → 推薦エンジンA
    └→ Algorithm B Group → 推薦エンジンB

同じデータで異なるアルゴリズムをテスト
```

## 実装例

### 基本的なConsumer Group

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupExample {
    public static void main(String[] args) {
        String groupId = args.length > 0 ? args[0] : "default-group";
        String consumerId = args.length > 1 ? args[1] : "consumer-" + System.currentTimeMillis();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            consumer.subscribe(Collections.singletonList("my-topic"));

            System.out.println(consumerId + " started in group: " + groupId);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[%s] Partition=%d, Offset=%d, Key=%s, Value=%s%n",
                        consumerId, record.partition(), record.offset(),
                        record.key(), record.value());
                }

                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}
```

### リバランスリスナー付きConsumer

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;

public class RebalanceListenerExample {
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rebalance-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // カスタムリバランスリスナー
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Partitions revoked: " + partitions);
                // リバランス前にオフセットをコミット
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Partitions assigned: " + partitions);
                for (TopicPartition partition : partitions) {
                    // 新しいパーティションの初期化処理
                    System.out.println("  Partition " + partition.partition() +
                        " assigned to this consumer");
                }
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                System.out.println("Partitions lost: " + partitions);
                // パーティションロスト時の処理
                currentOffsets.clear();
            }
        };

        consumer.subscribe(Collections.singletonList("my-topic"), listener);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Processing: partition=%d, offset=%d%n",
                        record.partition(), record.offset());

                    // 処理...

                    // 処理済みオフセットを記録
                    currentOffsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                    );
                }

                // 定期的にコミット
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            consumer.close();
        }
    }
}
```

### Static Group Membership

```java
public class StaticMemberConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "static-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Static Group Membership（Kafka 2.3+）
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "consumer-1");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);  // 長めに設定

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("my-topic"));

        // Static memberは再起動時にリバランスが発生しない
        // (session.timeout.ms内であれば)

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                // 処理...
            }
        } finally {
            consumer.close();
        }
    }
}
```

### Consumer Groupモニタリング

```java
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ConsumerGroupMonitoring {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {

            String groupId = "my-consumer-group";

            // Consumer Group一覧取得
            ListConsumerGroupsResult groups = adminClient.listConsumerGroups();
            groups.all().get().forEach(listing -> {
                System.out.println("Group: " + listing.groupId() + ", State: " + listing.state());
            });

            // グループ詳細取得
            DescribeConsumerGroupsResult description =
                adminClient.describeConsumerGroups(Collections.singletonList(groupId));

            ConsumerGroupDescription groupDescription = description.all().get().get(groupId);

            System.out.println("\nGroup: " + groupDescription.groupId());
            System.out.println("State: " + groupDescription.state());
            System.out.println("Coordinator: " + groupDescription.coordinator().id());
            System.out.println("Members: " + groupDescription.members().size());

            for (MemberDescription member : groupDescription.members()) {
                System.out.println("\n  Member ID: " + member.consumerId());
                System.out.println("  Client ID: " + member.clientId());
                System.out.println("  Host: " + member.host());
                System.out.println("  Partitions: " + member.assignment().topicPartitions());
            }

            // オフセット情報取得
            ListConsumerGroupOffsetsResult offsets =
                adminClient.listConsumerGroupOffsets(groupId);

            Map<TopicPartition, OffsetAndMetadata> offsetMap = offsets.partitionsToOffsetAndMetadata().get();

            System.out.println("\nOffsets:");
            offsetMap.forEach((partition, metadata) -> {
                System.out.printf("  %s: offset=%d%n", partition, metadata.offset());
            });

            // ラグ計算（簡易版）
            System.out.println("\nLag:");
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetMap.entrySet()) {
                TopicPartition partition = entry.getKey();
                long committedOffset = entry.getValue().offset();

                // エンドオフセット取得（別途Consumerが必要）
                // ここでは省略
                System.out.printf("  %s: committed=%d%n", partition, committedOffset);
            }
        }
    }
}
```

### CLIでのConsumer Group管理

```bash
# Consumer Group一覧表示
kafka-consumer-groups.sh --list \
  --bootstrap-server localhost:9092

# Consumer Group詳細表示
kafka-consumer-groups.sh --describe \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group

# オフセットリセット（最初から）
kafka-consumer-groups.sh --reset-offsets \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --topic my-topic \
  --to-earliest \
  --execute

# オフセットリセット（最新から）
kafka-consumer-groups.sh --reset-offsets \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --topic my-topic \
  --to-latest \
  --execute

# オフセットリセット（特定オフセット）
kafka-consumer-groups.sh --reset-offsets \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --topic my-topic:0 \
  --to-offset 1000 \
  --execute

# Consumer Group削除
kafka-consumer-groups.sh --delete \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group
```

## 実践演習

### 演習1: スケーリング動作確認

1. 4パーティションのトピック作成
2. Consumer 1台で処理開始
3. Consumer 2台目、3台目を追加
4. 各Consumerのパーティション割り当てを確認

### 演習2: 障害復旧

1. 3 Consumerで処理中
2. 1 Consumerを強制終了
3. リバランス動作を観察
4. 処理が継続することを確認

### 演習3: 異なるConsumer Group

1. 同じトピックに対して2つのConsumer Group作成
2. 各Groupで独立して消費
3. オフセットが独立していることを確認

## ベストプラクティス

### 1. 適切なConsumer数

```
最適Consumer数 = パーティション数

Consumer数 > パーティション数 → 遊休Consumer発生
Consumer数 < パーティション数 → 一部Consumerが複数パーティション処理
```

### 2. セッションタイムアウト設定

```properties
# 推奨設定
session.timeout.ms=10000  # 10秒
heartbeat.interval.ms=3000  # session.timeout.msの1/3
max.poll.interval.ms=300000  # 処理時間に応じて調整
```

### 3. Graceful Shutdown

```java
final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    System.out.println("Shutting down...");
    consumer.wakeup();
}));

try {
    consumer.subscribe(Collections.singletonList("my-topic"));
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        // 処理...
    }
} catch (WakeupException e) {
    System.out.println("Wakeup called");
} finally {
    consumer.close();  // オフセットコミット & グループ離脱
    System.out.println("Consumer closed");
}
```

### 4. Static Membership使用

```properties
# ローリングアップデート時のリバランス削減
group.instance.id=consumer-instance-1
session.timeout.ms=45000  # 通常より長く
```

## トラブルシューティング

### 問題1: 頻繁なリバランス

**原因**:
```
poll()間隔が長い → max.poll.interval.ms超過
処理に時間がかかる → タイムアウト
```

**解決策**:
```properties
max.poll.interval.ms=600000  # 延長
max.poll.records=100  # 削減
```

### 問題2: Consumer遊休

**症状**: Consumer数 > パーティション数

**解決策**:
```bash
# パーティション数を増やす
kafka-topics.sh --alter \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 10
```

### 問題3: ラグ増大

**診断**:
```bash
kafka-consumer-groups.sh --describe \
  --bootstrap-server localhost:9092 \
  --group my-group

# LAGカラムを確認
```

**解決策**:
- Consumer数を増やす
- 処理ロジックを最適化
- バッチサイズ調整

## まとめ

Consumer Groupsの重要ポイント:

1. **並列処理**でスケーラビリティ実現
2. **リバランス**の仕組みと対策を理解
3. **適切な設定**でパフォーマンス最適化
4. **モニタリング**でラグとリバランスを監視
5. **Static Membership**で不要なリバランス削減

次のレッスンでは、Replicationについて詳しく学びます。

---

**前へ**: [レッスン3: Topics & Partitions](./03-topics-partitions.md) | **次へ**: [レッスン5: Replication](./05-replication.md)
