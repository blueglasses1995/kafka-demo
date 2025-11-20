# Apache Kafka カリキュラム - 第2章: 技術的原理

## Kafkaのアーキテクチャ概要

Kafkaは分散システムとして設計されており、水平スケーリング、耐障害性、高スループットを実現しています。

## 1. ストレージモデル

### コミットログの概念

Kafkaの中核は**コミットログ**（append-only log）です：

```
[0] [1] [2] [3] [4] [5] [6] [7] ...
 ↑                           ↑
古いメッセージ              新しいメッセージ
```

- メッセージは常にログの末尾に追加される
- 各メッセージには順序を示すオフセットが割り当てられる
- メッセージは削除されず、保持期間まで保存される

### パーティションとセグメント

**パーティション構造**:
```
Topic: user-events
├── Partition 0
│   ├── segment-0.log
│   ├── segment-1.log
│   └── segment-2.log
├── Partition 1
│   ├── segment-0.log
│   └── segment-1.log
└── Partition 2
    └── segment-0.log
```

- 各パーティションは複数のセグメントファイルに分割
- セグメントサイズ（デフォルト1GB）に達すると新しいセグメントを作成
- 古いセグメントは保持ポリシーに基づいて削除

### ディスクへの書き込み最適化

Kafkaが高速な理由：

1. **シーケンシャル書き込み**
   - ランダムアクセスではなく順次書き込み
   - HDDでも高速（300MB/s以上）

2. **ページキャッシュの活用**
   - OSのページキャッシュに依存
   - JVMヒープを使わずメモリ管理

3. **ゼロコピー転送**
   - データをアプリケーション層にコピーせず、カーネル空間で直接転送
   - sendfile()システムコールを使用

## 2. プロデューサーの動作原理

### メッセージ送信フロー

```
Producer → Serializer → Partitioner → Batch Buffer → Sender Thread → Broker
```

1. **シリアライゼーション**: オブジェクトをバイト配列に変換
2. **パーティショニング**: メッセージを送信先パーティションに振り分け
3. **バッチング**: 複数メッセージをまとめて送信
4. **圧縮**: バッチを圧縮（gzip, snappy, lz4, zstd）
5. **送信**: ブローカーに送信し、ACKを待つ

### パーティショニング戦略

```java
// キーベースパーティショニング
hash(key) % partition_count = target_partition

// ラウンドロビン（キーなし）
partition = (partition + 1) % partition_count
```

### ACK設定とデータの信頼性

- **acks=0**: 送信後すぐに成功とみなす（最速、データロスの可能性大）
- **acks=1**: リーダーが受信確認（バランス型）
- **acks=all**: すべてのレプリカが受信確認（最も安全、低速）

## 3. コンシューマーの動作原理

### オフセット管理

```
Partition: [0] [1] [2] [3] [4] [5] [6] [7] [8]
                        ↑           ↑
                 Committed Offset  Current Position
                 (last committed)  (next to read)
```

- **Current Position**: 次に読み取るオフセット
- **Committed Offset**: 最後に処理完了したオフセット
- オフセットは`__consumer_offsets`トピックに保存

### コンシューマーグループの調整

**パーティション割り当て**:
```
Consumer Group: my-app-group
Topic: orders (4 partitions)
Consumers: 3

Allocation:
Consumer 1 → Partition 0, 1
Consumer 2 → Partition 2
Consumer 3 → Partition 3
```

**リバランス**:
- コンシューマーの追加/削除時に発生
- パーティションの再割り当て
- リバランス中は処理が一時停止

### フェッチプロトコル

```
1. Consumer → Broker: FetchRequest (offset, max_bytes)
2. Broker → Consumer: FetchResponse (messages)
3. Consumer: Process messages
4. Consumer → Broker: CommitRequest (offset)
```

## 4. レプリケーションの仕組み

### リーダーとフォロワー

```
Partition 0 (Replication Factor = 3)

Broker 1 (Leader)    ← Write/Read
    ↓ replicate
Broker 2 (Follower)  ← Replicate only
    ↓ replicate
Broker 3 (Follower)  ← Replicate only
```

### ISR (In-Sync Replicas)

- リーダーに追いついているレプリカの集合
- レプリカラグが閾値以下のもの
- リーダー障害時、ISR内のレプリカのみがリーダーに昇格可能

### 最小ISR設定

```properties
min.insync.replicas=2
```

- ACK=allの場合、この数のレプリカが書き込み確認必要
- 可用性とデータ整合性のトレードオフ

## 5. ZooKeeperとKRaft

### ZooKeeperの役割（従来）

- クラスターメタデータの管理
- ブローカーのメンバーシップ管理
- パーティションリーダーの選出
- 設定の保存

### KRaft（Kafka Raft Metadata Mode）

Kafka 2.8以降の新しいコンセンサスプロトコル：

**メリット**:
- ZooKeeperへの依存を排除
- セットアップの簡素化
- スケーラビリティの向上（100万パーティション以上）
- リカバリ時間の短縮

**アーキテクチャ**:
```
Controller Quorum (Raft)
├── Controller 1 (Leader)
├── Controller 2 (Follower)
└── Controller 3 (Follower)

Metadata Log → すべてのクラスター変更を記録
```

## 6. データの順序保証

### 保証されるもの

1. **パーティション内の順序**
   - 同じパーティション内のメッセージは順序が保証される

2. **キーベースの順序**
   - 同じキーのメッセージは同じパーティションに送信される

### 保証されないもの

- **パーティション間の順序**
  - 異なるパーティション間の順序は保証されない

### 順序を保証する設定

```properties
# Producer設定
max.in.flight.requests.per.connection=1
enable.idempotence=true
```

## 7. メッセージ配信セマンティクス

### At-Most-Once（最大1回）
- メッセージは失われる可能性があるが、重複しない
- オフセットコミット後に処理

### At-Least-Once（最低1回）
- メッセージは重複する可能性があるが、失われない
- 処理後にオフセットコミット
- 最も一般的

### Exactly-Once（正確に1回）
- メッセージは失われず、重複もしない
- トランザクション機能を使用
- Kafkaストリーム処理でサポート

```java
// Exactly-Once設定
props.put("enable.idempotence", "true");
props.put("transactional.id", "my-transactional-id");
```

## 8. パフォーマンス最適化の原理

### バッチング

```
小さいメッセージ × 1000回送信 = 遅い
↓
大きいバッチ × 1回送信 = 速い
```

設定:
```properties
batch.size=16384          # バッチサイズ（バイト）
linger.ms=10              # バッチ待機時間
```

### 圧縮

```
圧縮なし: 1MB
↓ snappy圧縮
圧縮後: 200KB（80%削減）
```

トレードオフ:
- CPU使用量増加 vs ネットワーク帯域幅削減

### パーティション数の選択

**計算式**:
```
Partitions = max(T/P, T/C)
T = 目標スループット
P = パーティションあたりのProducerスループット
C = パーティションあたりのConsumerスループット
```

例: 目標100MB/s、Producer 10MB/s/partition、Consumer 20MB/s/partition
```
max(100/10, 100/20) = max(10, 5) = 10 partitions
```

## 9. データ保持とクリーンアップ

### 時間ベース保持

```properties
log.retention.hours=168    # 7日間保持
log.retention.bytes=-1     # 無制限
```

### ログコンパクション

```
Before Compaction:
key=A, value=1
key=B, value=2
key=A, value=3  ← 最新
key=C, value=4

After Compaction:
key=A, value=3  ← 各キーの最新値のみ保持
key=B, value=2
key=C, value=4
```

用途:
- CDC (Change Data Capture)
- 状態スナップショット
- キャッシュ更新

## まとめ

Kafkaの技術的原理を理解することで：

1. **パフォーマンスチューニング**が可能に
2. **信頼性設定**を適切に選択
3. **トラブルシューティング**が効率的に
4. **アーキテクチャ設計**の質が向上

次章からは、各機能を実際に使いながら学習していきます。

---

**前へ**: [第1章: 機能全体像](./01-overview.md) | **次へ**: [レッスン1: Producer](./lessons/01-producer.md)
