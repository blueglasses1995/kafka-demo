# レッスン9: Schema Registry

## 概要

Schema Registryは、Kafkaメッセージのスキーマを一元管理するサービスです。Avro、Protobuf、JSON Schemaなどのスキーマを管理し、バージョニング、互換性チェック、スキーマの進化を支援します。

## メリット

### 1. データ整合性
- スキーマ定義による型安全性
- 不正なデータの拒否
- データ品質の保証

### 2. スキーマ進化
- バージョン管理
- 後方互換性チェック
- 前方互換性チェック
- プロデューサー/コンシューマーの独立したデプロイ

### 3. ストレージ効率
- スキーマIDのみをメッセージに含める
- スキーマ本体は共有
- ペイロードサイズ削減

### 4. ドキュメント化
- スキーマがドキュメントとして機能
- データ構造の明確化
- チーム間のコミュニケーション向上

### 5. ツール連携
- Kafka Connect自動スキーマ変換
- ksqlDB統合
- Avro/Protobuf IDE サポート

### 6. ガバナンス
- スキーマ変更の追跡
- 承認ワークフロー（エンタープライズ版）
- アクセス制御

## デメリット

### 1. インフラの追加
- Schema Registryサーバーの運用
- 高可用性構成が必要
- 追加のメンテナンス負荷

### 2. 開発の複雑さ
- スキーマ定義が必要
- バージョン管理の理解
- 互換性ルールの学習

### 3. パフォーマンスオーバーヘッド
- スキーマ取得のネットワークコール
- シリアライゼーション/デシリアライゼーション
- キャッシュ設定が重要

### 4. 制約
- スキーマ形式の選択（Avro/Protobuf/JSON Schema）
- 一度登録したスキーマの削除制限
- 互換性モードによる制約

### 5. ライセンス
- Confluent Community License
- エンタープライズ機能は有償
- オープンソース代替は限定的

## 技術的原理

### アーキテクチャ

```
Producer
    ↓ 1. Get/Register Schema
Schema Registry (REST API)
    ↓ 2. Return Schema ID
    ↓
Producer → Kafka (Schema ID + Data)
    ↓
Consumer
    ↓ 3. Get Schema by ID
Schema Registry
    ↓ 4. Return Schema
Consumer → Deserialize
```

### スキーマバージョニング

```
Topic: users

Version 1:
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"}
  ]
}

Version 2: フィールド追加（後方互換）
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

### 互換性モード

#### BACKWARD（デフォルト）
```
新しいコンシューマーが古いデータを読める
用途: フィールド削除、デフォルト付きフィールド追加
```

#### FORWARD
```
古いコンシューマーが新しいデータを読める
用途: フィールド追加のみ
```

#### FULL
```
BACKWARD + FORWARD
双方向互換性
```

#### NONE
```
互換性チェックなし
注意して使用
```

### スキーマ形式

#### Avro
```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

#### Protobuf
```protobuf
syntax = "proto3";

message User {
  int32 id = 1;
  string name = 2;
  string email = 3;
}
```

#### JSON Schema
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "name": {"type": "string"},
    "email": {"type": "string"}
  },
  "required": ["id", "name"]
}
```

## ユースケース

### 1. マイクロサービス連携
```
サービス間のデータ契約を定義
スキーマ変更の影響を最小化
```

### 2. データレイク構築
```
すべてのイベントスキーマを管理
データカタログとして機能
```

### 3. CDC (Change Data Capture)
```
データベーススキーマをKafkaスキーマに変換
スキーマ変更を自動追跡
```

### 4. API Gateway
```
イベントのバリデーション
スキーマベースのルーティング
```

### 5. データガバナンス
```
データ定義の一元管理
スキーマ変更履歴の追跡
```

## 実装例

### Schema Registry起動

```yaml
# docker-compose.yml
version: '3'
services:
  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
```

### Avro Producer（Java）

```java
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class AvroProducerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        // Avroスキーマ定義
        String schemaString = """
            {
              "type": "record",
              "name": "User",
              "namespace": "com.example",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": null}
              ]
            }
            """;

        Schema schema = new Schema.Parser().parse(schemaString);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            // レコード作成
            GenericRecord user = new GenericData.Record(schema);
            user.put("id", 1);
            user.put("name", "Alice");
            user.put("email", "alice@example.com");

            ProducerRecord<String, GenericRecord> record =
                new ProducerRecord<>("users-avro", "user1", user);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent: partition=%d, offset=%d%n",
                        metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
    }
}
```

### Avro Consumer（Java）

```java
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            KafkaAvroDeserializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("users-avro"));

            while (true) {
                ConsumerRecords<String, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord user = record.value();
                    System.out.printf("User: id=%s, name=%s, email=%s%n",
                        user.get("id"),
                        user.get("name"),
                        user.get("email"));
                }
            }
        }
    }
}
```

### Avro Maven依存関係

```xml
<dependencies>
    <!-- Kafka Clients -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>3.5.0</version>
    </dependency>

    <!-- Avro -->
    <dependency>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro</artifactId>
        <version>1.11.1</version>
    </dependency>

    <!-- Schema Registry Client -->
    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-avro-serializer</artifactId>
        <version>7.4.0</version>
    </dependency>
</dependencies>

<repositories>
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>
```

### スキーマ進化の例

```java
// Version 1
String schemaV1 = """
    {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"}
      ]
    }
    """;

// Version 2: フィールド追加（後方互換）
String schemaV2 = """
    {
      "type": "record",
      "name": "User",
      "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "age", "type": ["null", "int"], "default": null}
      ]
    }
    """;

// 古いコンシューマー（V1スキーマ）が新しいデータ（V2）を読める
// ageフィールドは無視される
```

### REST API操作

```bash
# スキーマ一覧取得
curl http://localhost:8081/subjects

# 特定サブジェクトのバージョン一覧
curl http://localhost:8081/subjects/users-value/versions

# 最新スキーマ取得
curl http://localhost:8081/subjects/users-value/versions/latest

# スキーマ登録
curl -X POST http://localhost:8081/subjects/users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}"
  }'

# 互換性チェック
curl -X POST http://localhost:8081/compatibility/subjects/users-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }'

# 互換性モード取得
curl http://localhost:8081/config/users-value

# 互換性モード設定
curl -X PUT http://localhost:8081/config/users-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FULL"}'

# スキーマ削除（ソフト削除）
curl -X DELETE http://localhost:8081/subjects/users-value/versions/1

# 完全削除
curl -X DELETE http://localhost:8081/subjects/users-value/versions/1?permanent=true
```

### Protobuf使用例

```java
// .proto ファイル
syntax = "proto3";

option java_package = "com.example.proto";
option java_outer_classname = "UserProto";

message User {
  int32 id = 1;
  string name = 2;
  string email = 3;
}
```

```java
// Producer
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import com.example.proto.UserProto.User;

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
props.put("schema.registry.url", "http://localhost:8081");

KafkaProducer<String, User> producer = new KafkaProducer<>(props);

User user = User.newBuilder()
    .setId(1)
    .setName("Alice")
    .setEmail("alice@example.com")
    .build();

producer.send(new ProducerRecord<>("users-proto", "user1", user));
```

### Kafka Connect with Schema Registry

```json
{
  "name": "jdbc-source-with-schema",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/mydb",
    "table.whitelist": "users",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "topic.prefix": "mysql-",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://localhost:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://localhost:8081"
  }
}
```

### スキーマバリデーション

```java
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;

public class SchemaValidator {
    public static void main(String[] args) throws Exception {
        SchemaRegistryClient client =
            new CachedSchemaRegistryClient("http://localhost:8081", 100);

        String subject = "users-value";

        // 最新スキーマ取得
        Schema latestSchema = new Schema.Parser().parse(
            client.getLatestSchemaMetadata(subject).getSchema()
        );

        System.out.println("Latest schema: " + latestSchema);

        // 互換性テスト
        String newSchemaString = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "phone", "type": ["null", "string"], "default": null}
              ]
            }
            """;

        Schema newSchema = new Schema.Parser().parse(newSchemaString);

        boolean isCompatible = client.testCompatibility(subject, newSchema);
        System.out.println("Is compatible: " + isCompatible);
    }
}
```

## 実践演習

### 演習1: Avroスキーマ使用

1. Schema Registry起動
2. Avroスキーマ定義
3. ProducerでAvroメッセージ送信
4. ConsumerでAvroメッセージ受信

### 演習2: スキーマ進化

1. V1スキーマでデータ送信
2. V2スキーマ（フィールド追加）を登録
3. 互換性確認
4. 古いConsumerが新しいデータを読めることを確認

### 演習3: REST APIでスキーマ管理

1. curlでスキーマ登録
2. バージョン一覧取得
3. 互換性チェック

## ベストプラクティス

### 1. 命名規則

```
Subject名:
<topic-name>-key    # キーのスキーマ
<topic-name>-value  # 値のスキーマ

例: users-value, orders-key
```

### 2. デフォルト値の使用

```json
{
  "name": "email",
  "type": ["null", "string"],
  "default": null
}
```

### 3. 互換性モードの選択

```
一般的な選択:
BACKWARD: コンシューマーファースト
FORWARD: プロデューサーファースト
FULL: 厳格な互換性
```

### 4. キャッシュ設定

```java
props.put("schema.registry.cache.capacity", 100);
props.put("auto.register.schemas", false);  // 本番では無効推奨
```

## トラブルシューティング

### 問題1: 互換性エラー

```
Error: Schema being registered is incompatible with an earlier schema
```

**解決策**:
- スキーマ変更を見直す
- 互換性モードを確認
- 必要に応じてモード変更

### 問題2: Schema Registryに接続できない

**診断**:
```bash
curl http://localhost:8081/subjects
```

**解決策**:
- Schema Registryが起動しているか確認
- ネットワーク設定確認
- URLが正しいか確認

### 問題3: シリアライゼーションエラー

**解決策**:
- スキーマ定義を確認
- データ型の一致を確認
- Schema Registry URLを確認

## まとめ

Schema Registryの重要ポイント:

1. **スキーマ管理**でデータ整合性を保証
2. **バージョニング**でスキーマ進化をサポート
3. **互換性チェック**で安全なデプロイ
4. **Avro/Protobuf**で効率的なシリアライゼーション
5. **ツール統合**でエコシステム活用

次のレッスンでは、MonitoringとOperationsについて学びます。

---

**前へ**: [レッスン8: ksqlDB](./08-ksqldb.md) | **次へ**: [レッスン10: Monitoring & Operations](./10-monitoring.md)
