#!/bin/bash

echo "🛑 Kafka環境を停止します..."
echo ""

# Docker Composeでサービス停止
docker-compose down

echo ""
echo "✅ Kafka環境を停止しました"
echo ""
echo "データを完全に削除する場合:"
echo "  docker-compose down -v"
echo ""
