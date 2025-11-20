# レッスン10: Monitoring & Operations

## 概要

Kafkaクラスタの安定運用には、適切な監視と運用が不可欠です。このレッスンでは、重要なメトリクス、監視ツール、運用のベストプラクティスについて学びます。

## メリット

### 1. 問題の早期発見
- パフォーマンス低下の検知
- リソース不足の予測
- 障害の早期発見

### 2. キャパシティプランニング
- トレンド分析
- リソース使用状況の把握
- スケーリングタイミングの判断

### 3. SLA保証
- 可用性の監視
- レイテンシ追跡
- エラー率の監視

### 4. トラブルシューティング
- 問題の原因特定
- パフォーマンスボトルネックの発見
- 履歴データによる分析

### 5. セキュリティ
- 異常なアクセスパターン検知
- 認証失敗の監視
- データ漏洩の防止

## デメリット

### 1. 複雑性
- 多数のメトリクス
- 監視ツールの設定
- アラート調整の難しさ

### 2. リソースオーバーヘッド
- メトリクス収集のCPU/メモリ
- ストレージコスト
- ネットワーク帯域幅

### 3. アラート疲れ
- 誤検知によるノイズ
- 閾値設定の難しさ
- オンコール負荷

### 4. ツールコスト
- 商用監視ツールのライセンス
- インフラコスト
- 学習コスト

## 技術的原理

### Kafkaメトリクスアーキテクチャ

```
Kafka Broker
    ↓ JMX
JMX Exporter
    ↓ HTTP
Prometheus
    ↓
Grafana Dashboard

または

Kafka Broker
    ↓ JMX
Confluent Control Center / Datadog / New Relic
```

### メトリクスの種類

#### ブローカーメトリクス
```
- リソース使用率（CPU、メモリ、ディスク、ネットワーク）
- リクエストレート
- レプリケーション状態
- パーティション数
```

#### プロデューサーメトリクス
```
- 送信レート
- エラー率
- レイテンシ
- バッチサイズ
```

#### コンシューマーメトリクス
```
- コンシューマーラグ
- フェッチレート
- レコード処理レート
- リバランス回数
```

## 重要なメトリクス

### 1. Under-replicated Partitions

```
メトリクス: kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions

意味: レプリカが同期していないパーティション数
正常値: 0
アラート: > 0 が5分以上継続

原因:
- ブローカーダウン
- ネットワーク問題
- ディスクI/O遅延
```

### 2. ISR Shrink/Expand Rate

```
メトリクス:
- kafka.server:type=ReplicaManager,name=IsrShrinksPerSec
- kafka.server:type=ReplicaManager,name=IsrExpandsPerSec

意味: ISRからレプリカが離脱/復帰する頻度
正常値: 低頻度
アラート: 頻繁な変動

原因:
- ブローカー不安定
- ネットワーク問題
```

### 3. Request Latency

```
メトリクス:
- kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce
- kafka.network:type=RequestMetrics,name=TotalTimeMs,request=FetchConsumer

意味: リクエスト処理時間
正常値: < 100ms
アラート: > 500ms

原因:
- ディスクI/O遅延
- CPU高負荷
- ネットワーク遅延
```

### 4. Consumer Lag

```
メトリクス: kafka.consumer:type=consumer-fetch-manager-metrics,client-id={client-id}

意味: コンシューマーの処理遅延
正常値: 低く安定
アラート: 増加傾向

原因:
- コンシューマー処理が遅い
- プロデューサーの送信レートが高い
```

### 5. Disk Usage

```
メトリクス: ディスク使用率

正常値: < 80%
アラート: > 85%

原因:
- データ保持期間が長い
- ログコンパクションが動いていない
```

### 6. Network Throughput

```
メトリクス:
- kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec
- kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec

意味: ネットワークスループット
監視: トレンドと上限
```

### 7. Active Controller Count

```
メトリクス: kafka.controller:type=KafkaController,name=ActiveControllerCount

正常値: 1（クラスタ全体で）
アラート: 0 or > 1

原因:
- コントローラー障害
- Split-brain状態
```

## 監視ツール

### 1. Prometheus + Grafana

#### JMX Exporterセットアップ

```bash
# JMX Exporter JAR ダウンロード
wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.19.0/jmx_prometheus_javaagent-0.19.0.jar

# jmx_exporter_config.yml
---
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
- pattern: 'kafka.server<type=(.+), name=(.+)><>Value'
  name: kafka_server_$1_$2
- pattern: 'kafka.network<type=RequestMetrics, name=TotalTimeMs, request=(.+)><>Mean'
  name: kafka_network_request_total_time_ms
  labels:
    request: $1
```

```bash
# Kafka起動時にJMX Exporter設定
export KAFKA_OPTS="-javaagent:/path/to/jmx_prometheus_javaagent-0.19.0.jar=7071:/path/to/jmx_exporter_config.yml"
kafka-server-start.sh config/server.properties
```

#### Prometheus設定

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets:
        - 'broker1:7071'
        - 'broker2:7071'
        - 'broker3:7071'
```

#### Grafanaダッシュボード

```
人気のダッシュボード:
- Kafka Overview (ID: 7589)
- Kafka Exporter (ID: 7589)
- Strimzi Kafka (ID: 11962)

Grafana Labs:
https://grafana.com/grafana/dashboards/
```

### 2. Confluent Control Center

```yaml
# docker-compose.yml
control-center:
  image: confluentinc/cp-enterprise-control-center:latest
  ports:
    - "9021:9021"
  environment:
    CONTROL_CENTER_BOOTSTRAP_SERVERS: 'broker:9092'
    CONTROL_CENTER_REPLICATION_FACTOR: 3
    CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS: 1
    CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS: 1
    CONTROL_CENTER_STREAMS_NUM_STREAM_THREADS: 2
```

### 3. Kafka Manager (CMAK)

```bash
# Docker起動
docker run -d \
  -p 9000:9000 \
  -e ZK_HOSTS="zookeeper:2181" \
  hlebalbau/kafka-manager:latest
```

### 4. Burrow (Consumer Lag監視)

```yaml
# docker-compose.yml
burrow:
  image: linkedin/burrow:latest
  volumes:
    - ./burrow.toml:/etc/burrow/burrow.toml
  ports:
    - "8000:8000"
```

### 5. カスタムスクリプト

```bash
# コンシューマーラグチェック
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group my-group --describe

# Under-replicated partitions
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions

# ディスク使用量
du -sh /var/lib/kafka/data/*
```

## 実装例

### Java Admin APIで監視

```java
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaMonitoring {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient admin = AdminClient.create(props)) {

            // クラスタ情報
            DescribeClusterResult clusterResult = admin.describeCluster();
            System.out.println("Cluster ID: " + clusterResult.clusterId().get());
            System.out.println("Controller: " + clusterResult.controller().get());
            System.out.println("Nodes: " + clusterResult.nodes().get().size());

            // Under-replicated partitions
            checkUnderReplicatedPartitions(admin);

            // Consumer lag
            checkConsumerLag(admin);
        }
    }

    private static void checkUnderReplicatedPartitions(AdminClient admin)
            throws ExecutionException, InterruptedException {

        ListTopicsResult topics = admin.listTopics();
        DescribeTopicsResult descriptions = admin.describeTopics(topics.names().get());

        int underReplicatedCount = 0;

        for (Map.Entry<String, TopicDescription> entry : descriptions.all().get().entrySet()) {
            String topic = entry.getKey();
            TopicDescription description = entry.getValue();

            for (TopicPartitionInfo partition : description.partitions()) {
                int replicaCount = partition.replicas().size();
                int isrCount = partition.isr().size();

                if (isrCount < replicaCount) {
                    System.out.printf("Under-replicated: %s-%d (ISR: %d/%d)%n",
                        topic, partition.partition(), isrCount, replicaCount);
                    underReplicatedCount++;
                }
            }
        }

        System.out.println("Total under-replicated partitions: " + underReplicatedCount);
    }

    private static void checkConsumerLag(AdminClient admin)
            throws ExecutionException, InterruptedException {

        ListConsumerGroupsResult groups = admin.listConsumerGroups();

        for (ConsumerGroupListing group : groups.all().get()) {
            String groupId = group.groupId();

            ListConsumerGroupOffsetsResult offsets =
                admin.listConsumerGroupOffsets(groupId);

            Map<TopicPartition, OffsetAndMetadata> offsetMap =
                offsets.partitionsToOffsetAndMetadata().get();

            System.out.println("\nGroup: " + groupId);

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetMap.entrySet()) {
                TopicPartition partition = entry.getKey();
                long committedOffset = entry.getValue().offset();

                // エンドオフセット取得（簡略版）
                System.out.printf("  %s: committed=%d%n",
                    partition, committedOffset);
            }
        }
    }
}
```

### Consumer Lag監視スクリプト

```python
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
import time

def get_consumer_lag(bootstrap_servers, group_id):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False
    )

    # コミット済みオフセット取得
    committed = admin.list_consumer_group_offsets(group_id)

    # エンドオフセット取得
    partitions = list(committed.keys())
    end_offsets = consumer.end_offsets(partitions)

    # ラグ計算
    lag_info = {}
    total_lag = 0

    for partition in partitions:
        committed_offset = committed[partition].offset
        end_offset = end_offsets[partition]
        lag = end_offset - committed_offset

        lag_info[partition] = {
            'committed': committed_offset,
            'end': end_offset,
            'lag': lag
        }
        total_lag += lag

    return lag_info, total_lag

# 使用例
bootstrap_servers = ['localhost:9092']
group_id = 'my-consumer-group'

while True:
    lag_info, total_lag = get_consumer_lag(bootstrap_servers, group_id)

    print(f"\n=== Consumer Group: {group_id} ===")
    print(f"Total Lag: {total_lag}")

    for partition, info in lag_info.items():
        print(f"  {partition}: lag={info['lag']}")

    time.sleep(10)
```

### Prometheus Alertingルール

```yaml
# prometheus_alerts.yml
groups:
  - name: kafka_alerts
    interval: 30s
    rules:
      - alert: UnderReplicatedPartitions
        expr: kafka_server_ReplicaManager_UnderReplicatedPartitions > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Under-replicated partitions detected"
          description: "{{ $value }} partitions are under-replicated"

      - alert: HighConsumerLag
        expr: kafka_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag"
          description: "Consumer lag is {{ $value }}"

      - alert: HighDiskUsage
        expr: (node_filesystem_avail_bytes / node_filesystem_size_bytes) < 0.15
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Disk usage high"
          description: "Disk usage is above 85%"

      - alert: NoActiveController
        expr: kafka_controller_KafkaController_ActiveControllerCount == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "No active controller"
          description: "Kafka cluster has no active controller"
```

## 運用のベストプラクティス

### 1. ログ管理

```properties
# log4j.properties
log4j.rootLogger=INFO, kafkaAppender

log4j.appender.kafkaAppender=org.apache.log4j.RollingFileAppender
log4j.appender.kafkaAppender.File=/var/log/kafka/server.log
log4j.appender.kafkaAppender.MaxFileSize=100MB
log4j.appender.kafkaAppender.MaxBackupIndex=10
```

### 2. バックアップ

```bash
# トピック設定バックアップ
kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --describe > topics_config_backup.txt

# Consumer Group オフセットバックアップ
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --all-groups --describe > consumer_offsets_backup.txt
```

### 3. ローリングアップグレード

```bash
# 1. 1台ずつ実行
# 2. ブローカー停止
kafka-server-stop.sh

# 3. バイナリ更新
cp kafka_new_version/* /opt/kafka/

# 4. 設定確認
vi /opt/kafka/config/server.properties

# 5. ブローカー起動
kafka-server-start.sh -daemon config/server.properties

# 6. 起動確認
tail -f /var/log/kafka/server.log

# 7. Under-replicated partitions確認
kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --under-replicated-partitions

# 8. 次のブローカーへ
```

### 4. ディスククリーンアップ

```bash
# 古いログセグメント削除（自動）
log.retention.hours=168
log.retention.bytes=-1
log.segment.delete.delay.ms=60000

# 手動削除（注意）
# サービス停止して実行
rm -rf /var/lib/kafka/data/topic-name-0/
```

### 5. パフォーマンスチューニング

```properties
# OS設定
vm.swappiness=1
vm.max_map_count=262144

# Kafkaブローカー設定
num.network.threads=8
num.io.threads=16
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# ログ設定
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
```

## トラブルシューティングフロー

### 1. Under-replicated Partitions

```
[診断]
1. kafka-topics.sh --describe --under-replicated-partitions
2. ブローカーログ確認
3. ディスク使用率確認
4. ネットワーク確認

[対応]
- ブローカー再起動
- ディスク容量確保
- レプリカ手動追加
```

### 2. Consumer Lag増加

```
[診断]
1. kafka-consumer-groups.sh --describe --group <group>
2. コンシューマーログ確認
3. 処理時間測定

[対応]
- コンシューマー数増加
- パーティション数増加
- 処理ロジック最適化
```

### 3. High Latency

```
[診断]
1. JMX メトリクス確認
2. ディスクI/O確認 (iostat)
3. CPU使用率確認 (top)
4. ネットワーク確認 (iftop)

[対応]
- ディスク変更（SSD）
- CPUスケールアップ
- パーティション分散
```

## まとめ

Monitoring & Operationsの重要ポイント:

1. **重要メトリクス**を継続的に監視
2. **アラート**で問題を早期発見
3. **可視化ツール**で状況把握
4. **運用手順**の標準化
5. **定期メンテナンス**でクラスタ健全性維持

これでApache Kafkaカリキュラムのすべてのレッスンが完了しました！

---

**前へ**: [レッスン9: Schema Registry](./09-schema-registry.md) | **トップへ**: [カリキュラムTOP](../README.md)
