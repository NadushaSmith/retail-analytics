#!/bin/bash
echo "========================================="
echo "��� ПРОВЕРКА ВСЕХ СЕРВИСОВ"
echo "$(date)"
echo "========================================="

# 1. Проверка Docker контейнеров
echo -e "\n��� DOCKER КОНТЕЙНЕРЫ:"
for service in mongodb kafka clickhouse minio postgres redis; do
    if docker ps | grep -q "pikcha-$service"; then
        echo "   ✅ $service: запущен"
    else
        echo "   ❌ $service: НЕ запущен"
    fi
done

# 2. Проверка MongoDB
echo -e "\n��� MONGODB:"
docker exec pikcha-mongodb mongosh -u admin -p admin123 --eval "
db = db.getSiblingDB('pikcha_raw');
print('   Коллекции: ' + db.getCollectionNames().length);
print('   Магазины: ' + db.stores.countDocuments());
print('   Товары: ' + db.products.countDocuments());
print('   Покупатели: ' + db.customers.countDocuments());
print('   Покупки: ' + db.purchases.countDocuments());
" 2>/dev/null || echo "   ❌ MongoDB недоступен"

# 3. Проверка Kafka
echo -e "\n��� KAFKA:"
docker exec pikcha-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null || echo "   ❌ Kafka недоступен"

# 4. Проверка ClickHouse
echo -e "\n���️ CLICKHOUSE:"
echo "   RAW таблицы:"
docker exec pikcha-clickhouse clickhouse-client --user default --password strong_password_123 --query "
SELECT 'stores_raw' as table, count() FROM pikcha_raw.stores_raw
UNION ALL SELECT 'products_raw', count() FROM pikcha_raw.products_raw
UNION ALL SELECT 'customers_raw', count() FROM pikcha_raw.customers_raw
UNION ALL SELECT 'purchases_raw', count() FROM pikcha_raw.purchases_raw
FORMAT PrettyCompact
" 2>/dev/null || echo "   ❌ ClickHouse недоступен"

# 5. Проверка MinIO
echo -e "\n☁️ MINIO:"
curl -I http://localhost:9002/minio/health/live 2>/dev/null | head -1 || echo "   ❌ MinIO недоступен"
docker exec pikcha-minio-client mc ls myminio/pikcha-analytics/ 2>/dev/null | tail -5 || echo "   ��� Bucket пуст"

echo -e "\n========================================="
