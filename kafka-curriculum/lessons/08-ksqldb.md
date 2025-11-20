# レッスン8: ksqlDB

## 概要

ksqlDBは、Kafka上で動作するイベントストリーミングデータベースです。SQLライクな構文でストリーム処理を記述でき、プログラミングなしでリアルタイムアプリケーションを構築できます。

## メリット

### 1. SQL構文
- 馴染みのあるSQL構文でストリーム処理
- プログラミング知識不要
- データアナリストも利用可能

### 2. リアルタイムクエリ
- ストリーミングデータに対するSQLクエリ
- 継続的なクエリ（Pull/Push Query）
- マテリアライズドビュー

### 3. 開発速度
- コンパイル不要
- 即座にデプロイ
- インタラクティブな開発

### 4. 高度な機能
- ウィンドウ処理
- ストリーム/テーブル結合
- UDF（User-Defined Functions）
- コネクタ統合

### 5. スケーラビリティ
- Kafka Streamsベース
- 水平スケーリング
- 分散処理

### 6. データガバナンス
- Schema Registry統合
- データ型強制
- バージョン管理

## デメリット

### 1. 制約された表現力
- SQLで表現できない複雑なロジック
- Kafka Streamsより柔軟性が低い
- カスタムロジックはUDF開発が必要

### 2. パフォーマンス
- Java実装より遅い場合がある
- 複雑なクエリはオーバーヘッド
- リソース消費が大きい

### 3. 学習コスト
- ストリーム処理特有の概念
- SQL標準との違い
- ウィンドウ処理の理解

### 4. デバッグの難しさ
- エラーメッセージが分かりにくい場合がある
- クエリ最適化が難しい
- 実行計画の確認が限定的

### 5. 商用ライセンス
- Confluent Community License
- 一部機能は制限
- サポートは有償

## 技術的原理

### アーキテクチャ

```
ksqlDB Server
├── REST API (ポート8088)
├── Query Engine
├── Kafka Streams Runtime
└── State Stores

ksqlDB CLI → REST API → ksqlDB Server → Kafka
```

### ストリームとテーブル

#### STREAM
```sql
-- イベントの無限シーケンス
CREATE STREAM clickstream (
  user_id VARCHAR,
  page VARCHAR,
  timestamp BIGINT
) WITH (
  KAFKA_TOPIC='clicks',
  VALUE_FORMAT='JSON'
);

-- すべてのイベントが保持される
```

#### TABLE
```sql
-- 最新の状態
CREATE TABLE users (
  user_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  age INT
) WITH (
  KAFKA_TOPIC='users',
  VALUE_FORMAT='JSON'
);

-- キーごとに最新の値のみ
```

### クエリタイプ

#### Push Query
```sql
-- 継続的にデータを配信
SELECT * FROM clickstream EMIT CHANGES;

-- 新しいイベントが到着すると即座にクライアントに送信
```

#### Pull Query
```sql
-- 特定時点の状態を取得
SELECT * FROM user_stats WHERE user_id = 'user123';

-- テーブルに対してのみ実行可能
```

### マテリアライゼーション

```
STREAM → Processing → MATERIALIZED TABLE
                            ↓
                     State Store (RocksDB)
                            ↓
                     Pull Query可能
```

## ユースケース

### 1. リアルタイムダッシュボード
```sql
ストリームデータをSQL集計
→ ダッシュボードに表示
```

### 2. イベントフィルタリング
```sql
全イベントから特定条件を抽出
→ 別トピックに出力
```

### 3. データエンリッチメント
```sql
ストリーム + マスターデータ結合
→ エンリッチされたイベント生成
```

### 4. 異常検知
```sql
ウィンドウ集計で異常パターン検出
→ アラート送信
```

### 5. データ変換パイプライン
```sql
生データ → 変換 → 集計 → 出力
```

## 実装例

### ksqlDB起動

```bash
# Docker Compose
version: '3'
services:
  ksqldb-server:
    image: confluentinc/ksqldb-server:latest
    ports:
      - "8088:8088"
    environment:
      KSQL_BOOTSTRAP_SERVERS: kafka:9092
      KSQL_LISTENERS: http://0.0.0.0:8088

  ksqldb-cli:
    image: confluentinc/ksqldb-cli:latest
    depends_on:
      - ksqldb-server
    entrypoint: /bin/sh
    tty: true
```

```bash
# CLIアクセス
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

### ストリーム作成

```sql
-- ストリーム作成
CREATE STREAM pageviews (
  viewtime BIGINT,
  user_id VARCHAR,
  page_id VARCHAR
) WITH (
  KAFKA_TOPIC='pageviews',
  VALUE_FORMAT='JSON',
  TIMESTAMP='viewtime'
);

-- データ確認
SELECT * FROM pageviews EMIT CHANGES LIMIT 10;

-- データ投入（テスト用）
INSERT INTO pageviews (viewtime, user_id, page_id)
VALUES (UNIX_TIMESTAMP(), 'user1', 'page1');
```

### テーブル作成

```sql
-- テーブル作成
CREATE TABLE users (
  user_id VARCHAR PRIMARY KEY,
  name VARCHAR,
  country VARCHAR,
  gender VARCHAR
) WITH (
  KAFKA_TOPIC='users',
  VALUE_FORMAT='JSON'
);

-- Pull Query
SELECT * FROM users WHERE user_id = 'user1';
```

### フィルタリング

```sql
-- 特定ページのみ抽出
CREATE STREAM page1_views AS
  SELECT *
  FROM pageviews
  WHERE page_id = 'page1'
  EMIT CHANGES;
```

### 集計

```sql
-- ユーザーごとのページビュー数
CREATE TABLE pageview_counts AS
  SELECT user_id,
         COUNT(*) AS view_count
  FROM pageviews
  GROUP BY user_id
  EMIT CHANGES;

-- Pull Query
SELECT * FROM pageview_counts WHERE user_id = 'user1';
```

### ウィンドウ集計

```sql
-- Tumbling Window: 5分ごとの集計
CREATE TABLE pageviews_per_window AS
  SELECT user_id,
         WINDOWSTART AS window_start,
         WINDOWEND AS window_end,
         COUNT(*) AS view_count
  FROM pageviews
  WINDOW TUMBLING (SIZE 5 MINUTES)
  GROUP BY user_id
  EMIT CHANGES;

-- Hopping Window
CREATE TABLE pageviews_hopping AS
  SELECT user_id,
         COUNT(*) AS view_count
  FROM pageviews
  WINDOW HOPPING (SIZE 10 MINUTES, ADVANCE BY 5 MINUTES)
  GROUP BY user_id
  EMIT CHANGES;

-- Session Window
CREATE TABLE user_sessions AS
  SELECT user_id,
         COUNT(*) AS event_count,
         WINDOWSTART AS session_start,
         WINDOWEND AS session_end
  FROM pageviews
  WINDOW SESSION (30 MINUTES)
  GROUP BY user_id
  EMIT CHANGES;
```

### ストリーム結合

```sql
-- Stream-Stream Join
CREATE STREAM pageview_with_ad_impressions AS
  SELECT pv.user_id,
         pv.page_id,
         ad.ad_id
  FROM pageviews pv
  INNER JOIN ad_impressions ad
    WITHIN 1 MINUTES
    ON pv.user_id = ad.user_id
  EMIT CHANGES;
```

### ストリーム-テーブル結合

```sql
-- エンリッチメント
CREATE STREAM enriched_pageviews AS
  SELECT pv.user_id,
         pv.page_id,
         u.name,
         u.country
  FROM pageviews pv
  LEFT JOIN users u
    ON pv.user_id = u.user_id
  EMIT CHANGES;
```

### 複雑な集計

```sql
-- ユーザーごと、国ごとのページビュー
CREATE TABLE pageviews_by_country AS
  SELECT u.country,
         COUNT(*) AS view_count,
         COUNT_DISTINCT(pv.user_id) AS unique_users
  FROM pageviews pv
  LEFT JOIN users u ON pv.user_id = u.user_id
  GROUP BY u.country
  EMIT CHANGES;
```

### UDF（User-Defined Function）

```java
// Java UDF
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "mask_email", description = "Mask email addresses")
public class MaskEmailUdf {
    @Udf(description = "Mask the local part of an email")
    public String maskEmail(String email) {
        if (email == null || !email.contains("@")) {
            return email;
        }
        String[] parts = email.split("@");
        return "***@" + parts[1];
    }
}
```

```sql
-- UDF使用
SELECT user_id,
       mask_email(email) AS masked_email
FROM users
EMIT CHANGES;
```

### Connector統合

```sql
-- Source Connector作成
CREATE SOURCE CONNECTOR jdbc_source WITH (
  'connector.class' = 'io.confluent.connect.jdbc.JdbcSourceConnector',
  'connection.url' = 'jdbc:mysql://localhost:3306/mydb',
  'mode' = 'incrementing',
  'incrementing.column.name' = 'id',
  'topic.prefix' = 'mysql-'
);

-- Sink Connector作成
CREATE SINK CONNECTOR elasticsearch_sink WITH (
  'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'topics' = 'enriched_pageviews',
  'connection.url' = 'http://localhost:9200'
);
```

### Schema Registry統合

```sql
-- Avro形式でストリーム作成
CREATE STREAM orders (
  order_id INT,
  product_id INT,
  quantity INT,
  price DOUBLE
) WITH (
  KAFKA_TOPIC='orders',
  VALUE_FORMAT='AVRO'
);

-- スキーマは自動的にSchema Registryに登録される
```

### 複雑なクエリ例

```sql
-- リアルタイム異常検知
CREATE TABLE abnormal_behavior AS
  SELECT user_id,
         COUNT(*) AS request_count,
         WINDOWSTART AS window_start
  FROM api_requests
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY user_id
  HAVING COUNT(*) > 100
  EMIT CHANGES;

-- 売上集計
CREATE TABLE sales_summary AS
  SELECT p.category,
         SUM(o.quantity * o.price) AS total_sales,
         COUNT(DISTINCT o.user_id) AS unique_customers
  FROM orders o
  LEFT JOIN products p ON o.product_id = p.product_id
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY p.category
  EMIT CHANGES;
```

### CLI操作

```sql
-- トピック一覧
SHOW TOPICS;

-- ストリーム一覧
SHOW STREAMS;

-- テーブル一覧
SHOW TABLES;

-- クエリ一覧
SHOW QUERIES;

-- ストリーム詳細
DESCRIBE pageviews;

-- 拡張情報
DESCRIBE EXTENDED pageviews;

-- クエリ説明
EXPLAIN <query_id>;

-- クエリ停止
TERMINATE <query_id>;

-- ストリーム削除
DROP STREAM pageviews DELETE TOPIC;

-- テーブル削除
DROP TABLE users;
```

### REST API操作

```bash
# クエリ実行
curl -X POST http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{
    "ksql": "SELECT * FROM pageviews EMIT CHANGES;",
    "streamsProperties": {}
  }'

# ステータス確認
curl http://localhost:8088/info

# クエリ一覧
curl http://localhost:8088/ksql \
  -H "Content-Type: application/vnd.ksql.v1+json" \
  -d '{"ksql": "SHOW QUERIES;"}'
```

## 実践演習

### 演習1: 基本的なストリーム処理

1. ストリーム作成
2. フィルタリング
3. 別トピックに出力

### 演習2: ウィンドウ集計

1. イベントストリーム作成
2. 5分ウィンドウで集計
3. 結果をテーブルとして保存

### 演習3: エンリッチメント

1. イベントストリームとマスターテーブル作成
2. 結合してエンリッチ
3. 結果を出力

## ベストプラクティス

### 1. 適切なデータ型選択

```sql
-- タイムスタンプを正しく設定
CREATE STREAM events (
  event_time BIGINT,
  ...
) WITH (
  KAFKA_TOPIC='events',
  TIMESTAMP='event_time'
);
```

### 2. パーティショニング

```sql
-- 適切なPARTITION BY
CREATE STREAM repartitioned AS
  SELECT *
  FROM source_stream
  PARTITION BY user_id
  EMIT CHANGES;
```

### 3. エラーハンドリング

```sql
-- エラートピック設定
SET 'processing.guarantee' = 'exactly_once';
SET 'ksql.streams.processing.exception.handler' = 'log_and_fail';
```

### 4. リソース管理

```sql
-- クエリ制限
SET 'ksql.query.pull.max.allowed.offset.lag' = '10000';
SET 'ksql.query.push.v2.max.catchup.consumers' = '5';
```

## トラブルシューティング

### 問題1: クエリが遅い

**診断**:
```sql
EXPLAIN <query_id>;
```

**解決策**:
- パーティショニングを最適化
- ウィンドウサイズを調整
- インデックス（テーブルのKEY）を適切に設定

### 問題2: メモリ不足

**解決策**:
```properties
# ksqldb-server.properties
ksql.streams.cache.max.bytes.buffering=10485760
```

### 問題3: スキーマエラー

**診断**:
```sql
DESCRIBE EXTENDED stream_name;
```

**解決策**:
- Schema Registry確認
- VALUE_FORMAT確認
- データ型の一致確認

## まとめ

ksqlDBの重要ポイント:

1. **SQL構文**で簡単にストリーム処理
2. **ストリーム/テーブル**の概念理解
3. **ウィンドウ処理**でタイムベース集計
4. **結合**でデータエンリッチメント
5. **Connector統合**で外部システム連携

次のレッスンでは、Schema Registryについて学びます。

---

**前へ**: [レッスン7: Kafka Streams](./07-kafka-streams.md) | **次へ**: [レッスン9: Schema Registry](./09-schema-registry.md)
