# レッスン5: Replication (レプリケーション)

## 概要

Replicationは、Kafkaの耐障害性を実現する重要な機能です。各パーティションのデータを複数のブローカーに複製することで、ブローカー障害時でもデータロスを防ぎ、サービスを継続できます。

## メリット

### 1. 高可用性
- ブローカー障害時も自動フェイルオーバー
- データ損失なしでサービス継続
- 計画的メンテナンスでもダウンタイムなし

### 2. データ保護
- ハードウェア障害からデータを保護
- 複数コピーによる冗長性
- レプリカ数に応じた耐障害性

### 3. 読み取りスケーラビリティ
- Follower Fetchingで読み取り分散（Kafka 2.4+）
- リーダー負荷を軽減
- 地理的分散での低レイテンシ読み取り

### 4. 柔軟な信頼性設定
- トピックごとにレプリケーション係数を設定
- ACK設定で書き込み信頼性を調整
- min.insync.replicasで最小保証レプリカ数指定

### 5. データ整合性
- ISR（In-Sync Replicas）で同期状態管理
- リーダー選出で一貫性保証
- Unclean Leader Electionの制御

### 6. 障害検知と自動復旧
- Controller が障害を自動検知
- 新リーダー自動選出
- 復旧後の自動再同期

## デメリット

### 1. ストレージコスト
- レプリケーション係数分のディスク容量が必要
- RF=3の場合、3倍のストレージ
- 大規模クラスタでは大きなコスト

### 2. ネットワーク帯域幅
- レプリカ間でデータ複製
- ネットワークトラフィック増加
- クロスデータセンターでは特に顕著

### 3. 書き込みレイテンシ
- ACK=allの場合、すべてのISRからの確認待ち
- レプリケーション時間分のレイテンシ増加
- 地理的分散では更に増加

### 4. 運用複雑性
- ISR状態の監視が必要
- Under-replicated partitionsの対応
- Preferred Leader Electionの管理

### 5. リソース使用量
- CPU: レプリケーション処理
- メモリ: レプリカフェッチャースレッド
- I/O: 読み取り・書き込みの両方

## 技術的原理

### レプリケーションアーキテクチャ

```
Topic: orders, Partition 0, RF=3

Broker 1 (Leader)
    ↓ レプリケート
Broker 2 (Follower)
    ↓ レプリケート
Broker 3 (Follower)

Producer → Leader (書き込み)
Consumer → Leader (読み取り) ※デフォルト
```

### リーダーとフォロワー

#### リーダー (Leader)
- すべての読み取り・書き込みを処理
- フォロワーへのレプリケーションを調整
- ISRリストを管理

#### フォロワー (Follower)
- リーダーからデータを取得（Fetch）
- データを自分のログに書き込み
- リーダー障害時にリーダーに昇格可能

### ISR (In-Sync Replicas)

```
Partition 0:
  Leader: Broker 1 (offset: 10000)
  ISR: [Broker 1, Broker 2, Broker 3]

Follower状態:
  Broker 2: offset 9999 → ISR内（1メッセージ遅れ）
  Broker 3: offset 9500 → ISR外（500メッセージ遅れ）
```

ISR条件:
```properties
# フォロワーがISRから外れる条件
replica.lag.time.max.ms=10000  # 10秒以上遅れるとISR外

# ※replica.lag.max.messagesは廃止（Kafka 0.9以降）
```

### リーダー選出

#### 正常時のリーダー選出
```
1. Controller がブローカー障害を検知
2. ISR内のレプリカから新リーダーを選出
3. すべてのブローカーに新リーダー情報を通知
4. プロデューサー/コンシューマーが新リーダーに接続
```

#### Unclean Leader Election
```properties
unclean.leader.election.enable=false  # 推奨: 無効

false: ISR内からのみ選出（データロスなし、可用性低下）
true:  ISR外からも選出（可用性優先、データロス可能性）
```

### ACK設定とレプリケーション

#### acks=0
```
Producer → Leader
          ↓
        即座にACK（レプリケーション待たない）

リスク: Leader障害時にデータロス
```

#### acks=1
```
Producer → Leader → ローカルログに書き込み
                  ↓
                ACK（レプリケーション待たない）
                  ↓
              Followers → レプリケート

リスク: Leader障害前にレプリケーション完了していない場合ロス
```

#### acks=all (-1)
```
Producer → Leader → ローカルログに書き込み
                  ↓
              Followers (ISR) → レプリケート
                  ↓
              すべてのISRが確認
                  ↓
                ACK

最も安全だがレイテンシ増加
```

### min.insync.replicas

```properties
min.insync.replicas=2

ACK=all時、最低2つのレプリカが確認必要
ISR < min.insync.replicas → NotEnoughReplicasException

例:
RF=3, min.insync.replicas=2
ISRが1つだけ → 書き込みエラー（可用性より一貫性優先）
```

### Preferred Leader Election

```
各パーティションには「Preferred Leader」が定義されている
通常はレプリカリストの最初のブローカー

自動バランシング:
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=300

障害復旧後、Preferred Leaderに戻す
```

## ユースケース

### 1. 高信頼性システム（RF=3, min.insync=2）
```properties
replication.factor=3
min.insync.replicas=2
acks=all

例: 金融取引、決済システム
```

### 2. 高可用性重視（RF=3, min.insync=1）
```properties
replication.factor=3
min.insync.replicas=1
acks=all

例: ログ収集、メトリクス
```

### 3. コスト重視（RF=2）
```properties
replication.factor=2
min.insync.replicas=1
acks=1

例: 開発環境、一時データ
```

### 4. クリティカルデータ（RF=5）
```properties
replication.factor=5
min.insync.replicas=3
acks=all

例: 規制対象データ、監査ログ
```

### 5. 地理的分散（RF=6, 3データセンター）
```properties
replication.factor=6  # 各DC2レプリカ
min.insync.replicas=4  # 少なくとも2DCから確認

災害復旧対策
```

## 実装例

### トピック作成時のレプリケーション設定

```bash
# CLI
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic replicated-topic \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

```java
// Java Admin API
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import java.util.*;

public class ReplicatedTopicCreation {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            String topicName = "replicated-topic";
            int partitions = 3;
            short replicationFactor = 3;

            Map<String, String> configs = new HashMap<>();
            configs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");

            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor)
                .configs(configs);

            CreateTopicsResult result = adminClient.createTopics(
                Collections.singletonList(newTopic)
            );

            result.all().get();
            System.out.println("Replicated topic created successfully");
        }
    }
}
```

### 信頼性の高いProducer設定

```java
public class ReliableProducerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class.getName());

        // 信頼性設定
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // タイムアウト
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            ProducerRecord<String, String> record =
                new ProducerRecord<>("replicated-topic", "key1", "critical data");

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // NotEnoughReplicasException等のエラーハンドリング
                    System.err.println("Send failed: " + exception.getMessage());
                    if (exception instanceof org.apache.kafka.common.errors.NotEnoughReplicasException) {
                        System.err.println("Not enough in-sync replicas");
                    }
                } else {
                    System.out.printf("Sent successfully: partition=%d, offset=%d%n",
                        metadata.partition(), metadata.offset());
                }
            }).get();  // 同期待機

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
```

### レプリケーション状態の監視

```java
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import java.util.*;

public class ReplicationMonitoring {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            String topicName = "replicated-topic";

            // トピック詳細取得
            DescribeTopicsResult result = adminClient.describeTopics(
                Collections.singletonList(topicName)
            );

            TopicDescription description = result.all().get().get(topicName);

            System.out.println("Topic: " + description.name());
            System.out.println();

            for (TopicPartitionInfo partition : description.partitions()) {
                System.out.println("Partition " + partition.partition() + ":");
                System.out.println("  Leader: Broker " + partition.leader().id());

                System.out.print("  Replicas: ");
                partition.replicas().forEach(node ->
                    System.out.print("Broker " + node.id() + " ")
                );
                System.out.println();

                System.out.print("  ISR: ");
                partition.isr().forEach(node ->
                    System.out.print("Broker " + node.id() + " ")
                );
                System.out.println();

                // Under-replicated判定
                if (partition.isr().size() < partition.replicas().size()) {
                    System.out.println("  ⚠ WARNING: Under-replicated!");
                }

                System.out.println();
            }
        }
    }
}
```

### CLI監視コマンド

```bash
# トピック詳細表示（レプリケーション情報含む）
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic replicated-topic

# Under-replicated partitionsの確認
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --under-replicated-partitions

# Unavailable partitionsの確認
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --unavailable-partitions

# Preferred Leader Electionの実行
kafka-leader-election.sh --election-type preferred \
  --bootstrap-server localhost:9092 \
  --all-topic-partitions
```

### レプリケーション係数の変更

```bash
# レプリカ配置ファイル作成
cat > increase-replication-factor.json <<EOF
{
  "version": 1,
  "partitions": [
    {"topic": "my-topic", "partition": 0, "replicas": [1,2,3]},
    {"topic": "my-topic", "partition": 1, "replicas": [2,3,4]},
    {"topic": "my-topic", "partition": 2, "replicas": [3,4,1]}
  ]
}
EOF

# レプリケーション実行
kafka-reassign-partitions.sh --execute \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file increase-replication-factor.json

# 進捗確認
kafka-reassign-partitions.sh --verify \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file increase-replication-factor.json
```

### Rack Awareness設定

```properties
# server.properties（各ブローカー）
broker.rack=us-east-1a  # AZ or Rack ID

# レプリカを異なるRackに配置
# 同じRackの複数ブローカー障害に対応
```

```bash
# Rack考慮したトピック作成
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic rack-aware-topic \
  --partitions 3 \
  --replication-factor 3 \
  --replica-assignment 1:2:3,2:3:4,3:4:5
```

## 実践演習

### 演習1: レプリケーション動作確認

1. 3ブローカークラスタでRF=3のトピック作成
2. Producerでメッセージ送信
3. 各ブローカーでログファイルを確認
4. レプリケーションを確認

### 演習2: リーダー障害シミュレーション

1. リーダーブローカーを特定
2. リーダーブローカーを停止
3. 新リーダーの自動選出を確認
4. Producer/Consumerが継続動作することを確認

### 演習3: ISRと書き込み信頼性

1. RF=3, min.insync=2のトピック作成
2. ACK=allのProducerで送信
3. 1ブローカーを停止（ISR=2）
4. 送信成功を確認
5. もう1ブローカーを停止（ISR=1）
6. NotEnoughReplicasExceptionを確認

## ベストプラクティス

### 1. レプリケーション係数の選択

```
本番環境: RF=3（推奨）
  - 2つのブローカー障害まで耐えられる
  - ストレージとパフォーマンスのバランス

クリティカル: RF=5
  - 4つのブローカー障害まで
  - より高い信頼性

開発環境: RF=1 or 2
  - コスト削減
```

### 2. min.insync.replicasの設定

```
RF=3の場合:
  min.insync.replicas=2（推奨）

計算式:
  min.insync.replicas = (RF / 2) + 1

理由: 過半数が確認すれば一貫性保証
```

### 3. Producer設定

```properties
# 最も信頼性の高い設定
acks=all
enable.idempotence=true
retries=2147483647
max.in.flight.requests.per.connection=5
```

### 4. 監視項目

```
必須監視:
- Under-replicated partitions
- ISRサイズ
- Leader election rate
- Replica lag

アラート設定:
- Under-replicated > 0 が5分以上継続
- ISRが最小値を下回る
```

## トラブルシューティング

### 問題1: Under-replicated Partitions

**原因**:
- ブローカーダウン
- ディスクI/O遅延
- ネットワーク問題
- レプリカラグ大

**診断**:
```bash
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --under-replicated-partitions
```

**解決策**:
- ブローカー再起動
- ディスク容量・パフォーマンス確認
- replica.lag.time.max.ms調整

### 問題2: NotEnoughReplicasException

**原因**:
```
ISR数 < min.insync.replicas
```

**解決策**:
- ダウンしているブローカーを復旧
- 一時的にmin.insync.replicasを下げる（非推奨）
- unclean.leader.election.enable=trueを検討（データロスリスク）

### 問題3: リーダー偏り

**症状**: 特定ブローカーに多くのリーダーが集中

**診断**:
```bash
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

**解決策**:
```bash
# Preferred Leader Electionを実行
kafka-leader-election.sh --election-type preferred \
  --bootstrap-server localhost:9092 \
  --all-topic-partitions
```

## まとめ

Replicationの重要ポイント:

1. **レプリケーション係数**で耐障害性を決定
2. **ISR**で同期状態を管理
3. **ACK=all + min.insync.replicas**で信頼性保証
4. **Under-replicated監視**が運用の鍵
5. **Preferred Leader Election**で負荷分散

次のレッスンでは、Kafka Connectについて学びます。

---

**前へ**: [レッスン4: Consumer Groups](./04-consumer-groups.md) | **次へ**: [レッスン6: Kafka Connect](./06-kafka-connect.md)
