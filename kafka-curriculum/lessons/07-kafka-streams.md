# レッスン7: Kafka Streams

## 概要

Kafka Streamsは、Kafkaに組み込まれたストリーム処理ライブラリです。リアルタイムでデータの変換、フィルタリング、集計、結合などの処理を、標準的なJavaアプリケーションとして実装できます。

## メリット

### 1. シンプルな開発
- 標準的なJavaライブラリとして利用
- 外部フレームワーク不要（Spark、Flink不要）
- マイクロサービスに組み込み可能

### 2. スケーラビリティ
- パーティション単位で並列処理
- アプリケーションインスタンス追加で水平スケーリング
- 状態ストアによる効率的な状態管理

### 3. 耐障害性
- 状態ストアの自動バックアップ（changelog topic）
- 障害時の自動復旧
- Exactly-once処理保証

### 4. リアルタイム処理
- イベント駆動の低レイテンシ処理
- ウィンドウ処理でタイムベース集計
- ストリーム/テーブルの結合

### 5. 状態管理
- ローカル状態ストア（RocksDB）
- 分散状態の自動管理
- 高速な状態アクセス

### 6. 開発効率
- 高レベルDSL API
- 低レベルProcessor API
- Interactive Queriesで状態照会

## デメリット

### 1. Java/Kotlin限定
- Java/Kotlinのみサポート
- 他言語は別ツール（ksqlDB等）が必要
- Scalaは非公式サポート

### 2. 学習曲線
- ストリーム処理の概念理解が必要
- KTable vs KStreamの違い
- ウィンドウ処理の複雑さ

### 3. 状態ストアの管理
- ディスク容量の管理が必要
- 状態復旧に時間がかかる場合がある
- ローカルディスクの依存性

### 4. デバッグの難しさ
- 非同期処理のデバッグ
- 分散環境でのトラブルシューティング
- トポロジーの複雑化

### 5. リソース要件
- メモリ（状態ストア用）
- ディスク（RocksDB）
- ネットワーク（リバランス、状態復旧）

## 技術的原理

### ストリーム処理トポロジー

```
Source Topology:
Input Topic → Stream Processor → Output Topic

例:
orders-topic → filter → map → aggregate → results-topic
```

### KStream vs KTable

#### KStream
```
イベントストリーム（INSERT-only）

例: クリックストリーム
user1 clicked page1
user1 clicked page2
user2 clicked page1

すべてのイベントが保持される
```

#### KTable
```
変更ログ（UPDATE/DELETE）

例: ユーザープロファイル
user1: {name: "Alice", age: 30}
user1: {name: "Alice", age: 31}  ← 更新

最新の状態のみが重要
```

#### GlobalKTable
```
すべてのパーティションのデータを全インスタンスに複製
結合処理で使用
```

### ウィンドウ処理

#### Tumbling Window
```
固定サイズ、重複なし

[0-5min] [5-10min] [10-15min]

例: 5分ごとの集計
```

#### Hopping Window
```
固定サイズ、重複あり

[0-5min]
   [2-7min]
      [4-9min]

例: 5分ウィンドウ、2分ごとに更新
```

#### Sliding Window
```
イベントベース、動的サイズ

イベント到着時に計算
時間差が閾値以内のイベントをグループ化
```

#### Session Window
```
アクティビティベース

非アクティブ期間で区切る
例: ユーザーセッション分析
```

### 状態ストア

```
Kafka Streams App
├── Local State Store (RocksDB)
│   └── Partition 0 state
└── Changelog Topic (Kafka)
    └── State backup

障害時: Changelog Topicから復旧
```

### リバランスと状態復旧

```
[1] 新インスタンス追加
    ↓
[2] リバランス発生
    ↓
[3] パーティション再割り当て
    ↓
[4] 状態ストア復旧（Changelogから）
    ↓
[5] 処理開始
```

## ユースケース

### 1. リアルタイム分析
```
Clickstream → Kafka Streams → ダッシュボード

ページビュー、コンバージョン率の計算
```

### 2. イベント駆動マイクロサービス
```
Order Created → Kafka Streams → Inventory Update

注文処理、在庫更新、通知送信
```

### 3. 不正検知
```
Transaction Stream → Kafka Streams → Alert Topic

異常パターン検出、リアルタイムアラート
```

### 4. データエンリッチメント
```
Event Stream + Reference Data → Enriched Stream

ユーザー情報、商品情報の付加
```

### 5. IoTデータ処理
```
Sensor Data → Kafka Streams → Aggregated Metrics

センサーデータの集計、異常検知
```

## 実装例

### 基本的なKafka Streamsアプリケーション

```java
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class BasicStreamsApp {
    public static void main(String[] args) {
        // 1. 設定
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            org.apache.kafka.common.serialization.Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            org.apache.kafka.common.serialization.Serdes.String().getClass());

        // 2. トポロジー構築
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("input-topic");

        source
            .filter((key, value) -> value.length() > 5)
            .mapValues(value -> value.toUpperCase())
            .to("output-topic");

        // 3. Streamsアプリケーション起動
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 4. Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### WordCountの例

```java
import org.apache.kafka.streams.kstream.*;
import java.util.Arrays;

public class WordCountApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("text-input");

        KTable<String, Long> wordCounts = textLines
            .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
            .groupBy((key, word) -> word)
            .count();

        wordCounts.toStream().to("word-count-output",
            Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### ウィンドウ集計

```java
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;

public class WindowedAggregationApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-agg-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Long> events = builder.stream("events",
            Consumed.with(Serdes.String(), Serdes.Long()));

        // Tumbling Window: 5分ごとの集計
        KTable<Windowed<String>, Long> windowedCounts = events
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
            .count();

        windowedCounts.toStream()
            .map((key, value) -> KeyValue.pair(
                key.key() + "@" + key.window().start(),
                value
            ))
            .to("windowed-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
```

### ストリーム結合

```java
public class StreamJoinApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // 2つのストリーム
        KStream<String, String> orders = builder.stream("orders");
        KStream<String, String> shipments = builder.stream("shipments");

        // ストリーム結合（5分以内に到着したイベントを結合）
        KStream<String, String> joined = orders.join(
            shipments,
            (orderValue, shipmentValue) -> "Order: " + orderValue + ", Shipment: " + shipmentValue,
            JoinWindows.of(Duration.ofMinutes(5))
        );

        joined.to("order-shipment-joined");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
```

### ストリーム/テーブル結合

```java
public class StreamTableJoinApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // ストリーム: クリックイベント
        KStream<String, String> clicks = builder.stream("user-clicks");

        // テーブル: ユーザープロファイル
        KTable<String, String> users = builder.table("user-profiles");

        // エンリッチメント
        KStream<String, String> enrichedClicks = clicks.join(
            users,
            (clickValue, userProfile) -> clickValue + " | User: " + userProfile
        );

        enrichedClicks.to("enriched-clicks");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
```

### カスタム状態ストア

```java
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.*;

public class StatefulProcessorApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-processor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // 状態ストア定義
        StoreBuilder<KeyValueStore<String, Long>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("counts-store"),
                Serdes.String(),
                Serdes.Long()
            );

        builder.addStateStore(storeBuilder);

        // Processor定義
        builder.stream("input")
            .process(() -> new Processor<String, String, String, Long>() {
                private KeyValueStore<String, Long> store;

                @Override
                public void init(ProcessorContext<String, Long> context) {
                    store = context.getStateStore("counts-store");
                }

                @Override
                public void process(Record<String, String> record) {
                    Long oldValue = store.get(record.key());
                    Long newValue = (oldValue == null) ? 1L : oldValue + 1;
                    store.put(record.key(), newValue);

                    context().forward(new Record<>(record.key(), newValue, record.timestamp()));
                }
            }, "counts-store");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
```

### Interactive Queries

```java
import org.apache.kafka.streams.state.*;

public class InteractiveQueriesApp {
    private KafkaStreams streams;

    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "iq-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7070");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Long> wordCounts = builder.stream("text-input")
            .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
            .groupBy((key, word) -> word)
            .count(Materialized.as("word-counts-store"));

        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public Long getCount(String word) {
        ReadOnlyKeyValueStore<String, Long> store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    "word-counts-store",
                    QueryableStoreTypes.keyValueStore()
                )
            );

        return store.get(word);
    }
}
```

### Exactly-Once設定

```java
public class ExactlyOnceApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "exactly-once-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Exactly-Once Semantics有効化
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
            StreamsConfig.EXACTLY_ONCE_V2);

        // トランザクション設定
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("input")
            .mapValues(value -> processValue(value))
            .to("output");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static String processValue(String value) {
        // 処理ロジック（重複なく正確に1回実行される）
        return value.toUpperCase();
    }
}
```

## 実践演習

### 演習1: 基本的なフィルタリング

1. 入力トピックからメッセージを読み取る
2. 特定条件でフィルタリング
3. 変換して出力トピックに書き込む

### 演習2: リアルタイム集計

1. イベントストリームを読み取る
2. 5分ウィンドウで集計
3. 結果を出力

### 演習3: ストリーム結合

1. 2つのストリームを作成
2. 時間ウィンドウで結合
3. エンリッチされたデータを出力

## ベストプラクティス

### 1. 適切なSerde選択

```java
// JSON Serde
final Serde<MyObject> mySerde = new JsonSerde<>(MyObject.class);

props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, mySerde.getClass());
```

### 2. 状態ストアの最適化

```java
// RocksDB設定
props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);

// キャッシュサイズ
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
```

### 3. スレッド数の調整

```java
// パーティション数に応じて調整
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
```

### 4. エラーハンドリング

```java
streams.setUncaughtExceptionHandler((thread, throwable) -> {
    System.err.println("Uncaught exception: " + throwable.getMessage());
    return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
});
```

## トラブルシューティング

### 問題1: 状態復旧が遅い

**原因**: Changelogトピックからの復旧

**解決策**:
```java
// スタンバイレプリカ設定
props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
```

### 問題2: メモリ不足

**原因**: 状態ストアやキャッシュ

**解決策**:
```java
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024L);
// JVM_OPTS="-Xmx2G"
```

### 問題3: リバランスが頻繁

**解決策**:
```java
props.put(StreamsConfig.REBALANCE_TIMEOUT_MS_CONFIG, 300000);
```

## まとめ

Kafka Streamsの重要ポイント:

1. **軽量ライブラリ**で簡単にストリーム処理
2. **KStream/KTable**でストリーム/テーブル処理
3. **ウィンドウ処理**でタイムベース集計
4. **状態管理**で複雑な処理を実現
5. **Exactly-Once**で高信頼性

次のレッスンでは、ksqlDBについて学びます。

---

**前へ**: [レッスン6: Kafka Connect](./06-kafka-connect.md) | **次へ**: [レッスン8: ksqlDB](./08-ksqldb.md)
