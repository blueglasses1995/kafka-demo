# レッスン6: Kafka Connect

## 概要

Kafka Connectは、外部システムとKafkaの間でデータを転送するためのフレームワークです。コードを書かずに設定ファイルだけで、データベース、ファイルシステム、クラウドストレージなどとKafkaを接続できます。

## メリット

### 1. コーディング不要
- 設定ファイル（JSON）のみでデータパイプライン構築
- 一般的なシステムに対応したコネクタが豊富
- カスタムコネクタも開発可能

### 2. スケーラビリティ
- 分散モードで複数ワーカーに負荷分散
- タスクの並列実行
- 動的なスケーリング

### 3. 耐障害性
- ワーカー障害時の自動タスク再配置
- オフセット管理による再開可能性
- リトライ機構

### 4. データ変換
- Single Message Transforms (SMT)で軽量な変換
- フィールド追加・削除・リネーム
- タイムスタンプ挿入など

### 5. 統合性
- 200以上のコネクタ（Confluent Hub）
- 主要データベース、クラウドサービス対応
- 標準化されたREST API

### 6. 運用管理
- REST APIで管理
- 監視メトリクス提供
- プラグインアーキテクチャ

## デメリット

### 1. 複雑な変換には不向き
- SMTは単純な変換のみ
- 複雑なビジネスロジックは実装困難
- その場合はKafka Streams使用を検討

### 2. デバッグの難しさ
- エラーメッセージが分かりにくい場合がある
- 分散環境でのトラブルシューティング
- ログが複数ワーカーに分散

### 3. バージョン互換性
- コネクタとKafka Connectのバージョン依存
- アップグレード時の注意が必要
- 一部コネクタは特定バージョンのみ対応

### 4. パフォーマンス制約
- 大量データには最適化が必要
- バッチサイズとタスク数の調整
- ネットワークとI/Oがボトルネックに

### 5. ライセンスとコスト
- 一部コネクタは商用ライセンス
- Confluent Platform必要な場合も
- エンタープライズ機能は有償

## 技術的原理

### アーキテクチャ

```
Source System → Source Connector → Kafka Topic
                     ↓
                Source Task × N (並列)

Kafka Topic → Sink Connector → Target System
                  ↓
            Sink Task × N (並列)
```

### コネクタとタスク

```
Connector
├── Task 1 → パーティション処理
├── Task 2 → パーティション処理
└── Task 3 → パーティション処理

Connectorが設定を管理
Taskが実際のデータ転送を実行
```

### 実行モード

#### 1. Standalone Mode
```
単一プロセスで実行
開発・テスト用
耐障害性なし
```

#### 2. Distributed Mode（推奨）
```
複数ワーカーで実行
本番環境用
高可用性・スケーラビリティ
REST APIで管理
```

### オフセット管理

#### Source Connector
```
外部システムの位置を追跡
例: データベースの最終行ID、ファイルの最終読み取り位置

オフセットトピック: connect-offsets
```

#### Sink Connector
```
Kafkaオフセットを追跡
Consumer Groupのオフセット管理と同様

停止・再開時に同じ位置から再開
```

### Single Message Transforms (SMT)

```
Message → Transform 1 → Transform 2 → Transform 3 → Output

例:
1. InsertField: タイムスタンプ追加
2. MaskField: 機密データマスク
3. ReplaceField: フィールドリネーム
```

## ユースケース

### 1. CDC (Change Data Capture)
```
MySQL → Debezium Source Connector → Kafka → Sink Connector → Elasticsearch

データベース変更をリアルタイムでストリーミング
```

### 2. ログ収集
```
Log Files → FileStream Source → Kafka → S3 Sink → S3

ログファイルをKafka経由でS3に保存
```

### 3. データウェアハウス連携
```
Kafka → JDBC Sink Connector → PostgreSQL/Snowflake

ストリームデータをDWHに保存
```

### 4. クラウド統合
```
S3 → S3 Source → Kafka → BigQuery Sink → BigQuery

クラウドストレージ間のデータ移動
```

### 5. メトリクス収集
```
Prometheus → HTTP Source → Kafka → InfluxDB Sink → InfluxDB

メトリクスの集約と保存
```

## 実装例

### Distributed Mode起動

```properties
# connect-distributed.properties
bootstrap.servers=localhost:9092
group.id=connect-cluster

# オフセットストレージ
offset.storage.topic=connect-offsets
offset.storage.replication.factor=3
offset.storage.partitions=25

# 設定ストレージ
config.storage.topic=connect-configs
config.storage.replication.factor=3

# ステータスストレージ
status.storage.topic=connect-status
status.storage.replication.factor=3
status.storage.partitions=5

# 変換設定
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# プラグインパス
plugin.path=/usr/local/share/kafka/plugins
```

```bash
# Kafka Connect起動
connect-distributed.sh config/connect-distributed.properties
```

### FileStream Source Connector

```json
{
  "name": "file-source",
  "config": {
    "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
    "tasks.max": "1",
    "file": "/var/log/application.log",
    "topic": "log-topic"
  }
}
```

```bash
# コネクタ作成
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @file-source-connector.json
```

### JDBC Source Connector

```json
{
  "name": "jdbc-source-users",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "3",
    "connection.url": "jdbc:mysql://localhost:3306/mydb",
    "connection.user": "kafka",
    "connection.password": "secret",
    "table.whitelist": "users",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "topic.prefix": "mysql-",
    "poll.interval.ms": "1000"
  }
}
```

### JDBC Sink Connector

```json
{
  "name": "jdbc-sink-orders",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "3",
    "topics": "orders",
    "connection.url": "jdbc:postgresql://localhost:5432/warehouse",
    "connection.user": "kafka",
    "connection.password": "secret",
    "auto.create": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "order_id",
    "table.name.format": "orders"
  }
}
```

### Elasticsearch Sink Connector

```json
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "3",
    "topics": "logs",
    "connection.url": "http://localhost:9200",
    "type.name": "_doc",
    "key.ignore": "true",
    "schema.ignore": "true"
  }
}
```

### S3 Sink Connector

```json
{
  "name": "s3-sink",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "tasks.max": "3",
    "topics": "events",
    "s3.bucket.name": "my-kafka-backup",
    "s3.region": "us-east-1",
    "flush.size": "1000",
    "rotate.interval.ms": "60000",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
    "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
    "partition.duration.ms": "3600000",
    "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
    "timestamp.extractor": "Record"
  }
}
```

### Debezium MySQL CDC Connector

```json
{
  "name": "debezium-mysql-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "localhost",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "secret",
    "database.server.id": "184054",
    "database.server.name": "mysql-server",
    "database.include.list": "mydb",
    "table.include.list": "mydb.users,mydb.orders",
    "database.history.kafka.bootstrap.servers": "localhost:9092",
    "database.history.kafka.topic": "schema-changes.mydb"
  }
}
```

### SMT使用例

```json
{
  "name": "jdbc-source-with-transforms",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/mydb",
    "table.whitelist": "users",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "topic.prefix": "mysql-",

    "transforms": "InsertTimestamp,MaskSSN,RenameField",

    "transforms.InsertTimestamp.type": "org.apache.kafka.connect.transforms.InsertField$Value",
    "transforms.InsertTimestamp.timestamp.field": "processed_at",

    "transforms.MaskSSN.type": "org.apache.kafka.connect.transforms.MaskField$Value",
    "transforms.MaskSSN.fields": "ssn",
    "transforms.MaskSSN.replacement": "***-**-****",

    "transforms.RenameField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.RenameField.renames": "old_name:new_name"
  }
}
```

### REST API操作

```bash
# コネクタ一覧取得
curl http://localhost:8083/connectors

# コネクタ詳細取得
curl http://localhost:8083/connectors/jdbc-source-users

# コネクタステータス取得
curl http://localhost:8083/connectors/jdbc-source-users/status

# コネクタ作成
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connector-config.json

# コネクタ設定更新
curl -X PUT http://localhost:8083/connectors/jdbc-source-users/config \
  -H "Content-Type: application/json" \
  -d @new-config.json

# コネクタ一時停止
curl -X PUT http://localhost:8083/connectors/jdbc-source-users/pause

# コネクタ再開
curl -X PUT http://localhost:8083/connectors/jdbc-source-users/resume

# コネクタ再起動
curl -X POST http://localhost:8083/connectors/jdbc-source-users/restart

# コネクタ削除
curl -X DELETE http://localhost:8083/connectors/jdbc-source-users

# タスク再起動
curl -X POST http://localhost:8083/connectors/jdbc-source-users/tasks/0/restart

# プラグイン一覧取得
curl http://localhost:8083/connector-plugins
```

### Javaでのカスタムコネクタ開発

```java
// Source Connector
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceTask;
import java.util.*;

public class MySourceConnector extends SourceConnector {

    private Map<String, String> config;

    @Override
    public void start(Map<String, String> props) {
        this.config = props;
    }

    @Override
    public Class<? extends SourceTask> taskClass() {
        return MySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void stop() {
        // クリーンアップ
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define("my.config", ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "My configuration");
    }
}

// Source Task
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import java.util.*;

public class MySourceTask extends SourceTask {

    @Override
    public void start(Map<String, String> props) {
        // 初期化
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();

        // 外部システムからデータ取得
        // SourceRecordを作成してリストに追加

        return records;
    }

    @Override
    public void stop() {
        // クリーンアップ
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
```

## 実践演習

### 演習1: FileStream Connector

1. ログファイルを作成
2. FileStream Source Connectorでトピックに送信
3. ConsoleConsumerで確認
4. FileStream Sink Connectorで別ファイルに出力

### 演習2: JDBC Connector

1. MySQLデータベース準備
2. JDBC Source Connectorでテーブルデータを取得
3. Kafkaトピックで確認
4. JDBC Sink Connectorで別DBに書き込み

### 演習3: REST API操作

1. コネクタをREST APIで作成
2. ステータス確認
3. 一時停止・再開
4. 削除

## ベストプラクティス

### 1. タスク数の設定

```json
{
  "tasks.max": "3"
}

推奨:
- Source: 外部システムのパーティション/テーブル数
- Sink: Kafkaトピックのパーティション数
```

### 2. エラーハンドリング

```json
{
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "dlq-topic",
  "errors.deadletterqueue.topic.replication.factor": "3"
}
```

### 3. パフォーマンスチューニング

```json
{
  "batch.size": "1000",
  "linger.ms": "100",
  "buffer.memory": "67108864",
  "max.request.size": "1048576"
}
```

### 4. 監視

```
重要メトリクス:
- connector-total-task-count
- connector-running-task-count
- connector-failed-task-count
- sink-record-send-rate
- source-record-poll-rate
```

## トラブルシューティング

### 問題1: タスクが起動しない

**診断**:
```bash
curl http://localhost:8083/connectors/my-connector/status
```

**解決策**:
- 設定エラーをログで確認
- 依存ライブラリ（JDBCドライバー等）確認
- plugin.pathに配置されているか確認

### 問題2: データが転送されない

**診断**:
```bash
# タスク詳細確認
curl http://localhost:8083/connectors/my-connector/tasks/0/status

# ログ確認
tail -f logs/connect.log
```

**解決策**:
- オフセット確認
- ネットワーク接続確認
- 外部システムの権限確認

### 問題3: パフォーマンス低下

**解決策**:
```json
{
  "tasks.max": "5",  // タスク数増加
  "batch.size": "2000",  // バッチサイズ増加
  "poll.interval.ms": "500"  // ポーリング間隔短縮
}
```

## まとめ

Kafka Connectの重要ポイント:

1. **設定のみ**で外部システムと連携
2. **分散モード**で本番運用
3. **REST API**で管理
4. **SMT**で軽量な変換
5. **豊富なコネクタ**でエコシステム活用

次のレッスンでは、Kafka Streamsについて学びます。

---

**前へ**: [レッスン5: Replication](./05-replication.md) | **次へ**: [レッスン7: Kafka Streams](./07-kafka-streams.md)
