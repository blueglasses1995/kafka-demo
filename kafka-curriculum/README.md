# Apache Kafka 完全カリキュラム

このカリキュラムは、Apache Kafkaを基礎から実践的なレベルまで、ステップバイステップで学習できるように設計されています。

## 📚 カリキュラム構成

### 基礎編

#### [第1章: 機能全体像](./01-overview.md)
Apache Kafkaの全体像を把握します。
- Kafkaとは何か
- 主要コンポーネント
- エコシステム
- 特徴とユースケース

#### [第2章: 技術的原理](./02-technical-principles.md)
Kafkaの内部動作を理解します。
- ストレージモデル
- プロデューサー/コンシューマーの動作原理
- レプリケーションの仕組み
- パフォーマンス最適化の原理

### 実践編：コア機能

#### [レッスン1: Producer](./lessons/01-producer.md)
データをKafkaに送信する方法を学びます。
- **メリット**: 高スループット、非同期処理、柔軟な信頼性設定
- **デメリット**: 設定の複雑さ、メモリ管理
- **技術的原理**: メッセージ送信フロー、シリアライゼーション、パーティショニング
- **ユースケース**: ログ収集、メトリクス送信、イベント駆動アーキテクチャ
- **実装例**: 基本的なProducer、非同期送信、信頼性の高い設定

#### [レッスン2: Consumer](./lessons/02-consumer.md)
Kafkaからデータを読み取る方法を学びます。
- **メリット**: スケーラブルな並列処理、オフセット管理、耐障害性
- **デメリット**: リバランスのオーバーヘッド、重複処理のリスク
- **技術的原理**: オフセット管理、ポーリングループ、Consumer Group調整
- **ユースケース**: リアルタイム処理、ETLパイプライン、マイクロサービス連携
- **実装例**: 基本的なConsumer、手動オフセットコミット、リバランスリスナー

#### [レッスン3: Topics & Partitions](./lessons/03-topics-partitions.md)
Kafkaのデータ構造を理解します。
- **メリット**: スケーラビリティ、並列処理、順序保証
- **デメリット**: パーティション数の選択の難しさ、順序保証の制約
- **技術的原理**: パーティショニング戦略、セグメント管理
- **ユースケース**: イベントストリーム、トランザクションログ、CDC
- **実装例**: トピック作成、パーティション管理、Admin API

#### [レッスン4: Consumer Groups](./lessons/04-consumer-groups.md)
複数Consumerでの処理分散を学びます。
- **メリット**: スケーラブルな並列処理、高可用性、負荷分散
- **デメリット**: リバランスのオーバーヘッド、パーティション制約
- **技術的原理**: パーティション割り当て戦略、リバランスプロトコル
- **ユースケース**: マイクロサービス並列処理、データパイプライン
- **実装例**: Consumer Group、リバランスリスナー、Static Membership

#### [レッスン5: Replication](./lessons/05-replication.md)
データの冗長化と耐障害性を学びます。
- **メリット**: 高可用性、データ保護、読み取りスケーラビリティ
- **デメリット**: ストレージコスト、ネットワーク帯域幅、書き込みレイテンシ
- **技術的原理**: リーダー/フォロワー、ISR、リーダー選出
- **ユースケース**: 高信頼性システム、災害復旧対策
- **実装例**: レプリケーション設定、信頼性の高いProducer、監視

### 実践編：エコシステム

#### [レッスン6: Kafka Connect](./lessons/06-kafka-connect.md)
外部システムとの連携を学びます。
- **メリット**: コーディング不要、スケーラビリティ、耐障害性
- **デメリット**: 複雑な変換には不向き、デバッグの難しさ
- **技術的原理**: Source/Sink Connector、タスク管理、SMT
- **ユースケース**: CDC、ログ収集、データウェアハウス連携
- **実装例**: JDBC Connector、Debezium、Elasticsearch Sink

#### [レッスン7: Kafka Streams](./lessons/07-kafka-streams.md)
ストリーム処理ライブラリを学びます。
- **メリット**: シンプルな開発、スケーラビリティ、Exactly-once保証
- **デメリット**: Java/Kotlin限定、学習曲線
- **技術的原理**: KStream/KTable、ウィンドウ処理、状態管理
- **ユースケース**: リアルタイム分析、イベント駆動マイクロサービス、不正検知
- **実装例**: WordCount、ウィンドウ集計、ストリーム結合

#### [レッスン8: ksqlDB](./lessons/08-ksqldb.md)
SQLでストリーム処理を学びます。
- **メリット**: SQL構文、開発速度、リアルタイムクエリ
- **デメリット**: 制約された表現力、パフォーマンス
- **技術的原理**: Stream/Table、Push/Pull Query、マテリアライゼーション
- **ユースケース**: リアルタイムダッシュボード、データエンリッチメント
- **実装例**: ストリーム作成、ウィンドウ集計、結合処理

#### [レッスン9: Schema Registry](./lessons/09-schema-registry.md)
スキーマ管理を学びます。
- **メリット**: データ整合性、スキーマ進化、ストレージ効率
- **デメリット**: インフラの追加、開発の複雑さ
- **技術的原理**: スキーマバージョニング、互換性チェック
- **ユースケース**: マイクロサービス連携、データガバナンス、CDC
- **実装例**: Avro Producer/Consumer、スキーマ進化、REST API

#### [レッスン10: Monitoring & Operations](./lessons/10-monitoring.md)
監視と運用を学びます。
- **メリット**: 問題の早期発見、キャパシティプランニング、SLA保証
- **デメリット**: 複雑性、リソースオーバーヘッド
- **技術的原理**: JMXメトリクス、重要な監視項目
- **ユースケース**: クラスタ監視、パフォーマンスチューニング
- **実装例**: Prometheus + Grafana、Consumer Lag監視、アラート設定

## 🎯 学習の進め方

### 推奨学習順序

1. **基礎理解**（1-2週間）
   - 第1章: 機能全体像
   - 第2章: 技術的原理

2. **コア機能習得**（2-3週間）
   - レッスン1: Producer
   - レッスン2: Consumer
   - レッスン3: Topics & Partitions
   - レッスン4: Consumer Groups
   - レッスン5: Replication

3. **エコシステム習得**（3-4週間）
   - レッスン6: Kafka Connect
   - レッスン7: Kafka Streams
   - レッスン8: ksqlDB
   - レッスン9: Schema Registry

4. **運用スキル**（1-2週間）
   - レッスン10: Monitoring & Operations

### 各レッスンの学習方法

1. **理論学習**（30分）
   - メリット・デメリットを理解
   - 技術的原理を把握
   - ユースケースを確認

2. **実装演習**（1-2時間）
   - 実装例を実際に動かす
   - 実践演習に取り組む
   - 設定を変えて動作を確認

3. **復習とまとめ**（30分）
   - 重要ポイントを確認
   - ベストプラクティスを整理
   - トラブルシューティングを理解

## 🛠️ 環境構築

### 最小構成（ローカル開発）

```bash
# Docker Composeで起動
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

### 推奨構成（本番相当）

- ブローカー: 3台以上
- ZooKeeper: 3台以上（またはKRaftモード）
- Schema Registry: 2台以上
- Kafka Connect: 2台以上
- 監視: Prometheus + Grafana

## 📖 追加リソース

### 公式ドキュメント
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Documentation](https://docs.confluent.io/)

### 書籍
- "Kafka: The Definitive Guide" by Neha Narkhede, Gwen Shapira, Todd Palino
- "Kafka Streams in Action" by Bill Bejeck

### オンラインコース
- Confluent Developer Skills
- Udemy Kafka コース

### コミュニティ
- [Kafka Users Mailing List](https://kafka.apache.org/contact)
- [Confluent Community Forum](https://forum.confluent.io/)
- [Stack Overflow - Apache Kafka](https://stackoverflow.com/questions/tagged/apache-kafka)

## 🎓 認定資格

### Confluent認定
- Confluent Certified Developer for Apache Kafka (CCDAK)
- Confluent Certified Administrator for Apache Kafka (CCAAK)

## 💡 ベストプラクティスまとめ

### プロデューサー
- べき等性を有効化（`enable.idempotence=true`）
- 適切なACK設定（本番: `acks=all`）
- バッチングと圧縮を活用

### コンシューマー
- 手動オフセットコミット検討
- べき等処理の実装
- Graceful Shutdownの実装

### トピック設計
- 適切なパーティション数の計算
- レプリケーション係数: 3（推奨）
- 命名規則の統一

### 運用
- 重要メトリクスの監視
- 定期的なバックアップ
- ローリングアップグレード手順の確立

## 🚀 次のステップ

カリキュラム完了後の学習:

1. **実践プロジェクト**
   - リアルタイムダッシュボード構築
   - マイクロサービス連携実装
   - ストリーム処理パイプライン構築

2. **高度なトピック**
   - Kafkaの内部実装
   - カスタムシリアライザー開発
   - パフォーマンスチューニング

3. **他のストリーミング技術**
   - Apache Flink
   - Apache Pulsar
   - AWS Kinesis

## 📝 ライセンス

このカリキュラムはMITライセンスの下で公開されています。

---

**Happy Learning! 🎉**

Apache Kafkaをマスターして、スケーラブルなリアルタイムシステムを構築しましょう！
