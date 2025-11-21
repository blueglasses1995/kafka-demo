#!/bin/bash

echo "🚀 Kafka環境を起動します..."
echo ""

# Docker Composeでサービス起動
docker-compose up -d

echo ""
echo "⏳ サービスの起動を待っています..."
sleep 10

# 起動確認
echo ""
echo "📊 サービス状態:"
docker-compose ps

echo ""
echo "✅ Kafka環境の起動が完了しました！"
echo ""
echo "利用可能なサービス:"
echo "  - Kafka Broker: localhost:9092"
echo "  - Schema Registry: http://localhost:8081"
echo "  - Kafka UI: http://localhost:8080"
echo ""
echo "次のステップ:"
echo "  1. Pythonパッケージをインストール: pip install -r requirements.txt"
echo "  2. Consumerを起動: python python-app/consumer/basic_consumer.py"
echo "  3. Producerを起動: python python-app/producer/basic_producer.py"
echo ""
