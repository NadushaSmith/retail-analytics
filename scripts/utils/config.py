# config.py - Загрузка настроек из .env
import os
from dotenv import load_dotenv

# Загружаем .env файл (ищем в корне проекта)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
load_dotenv(env_path)


class Config:
    # MongoDB
    MONGO_HOST = os.getenv('MONGO_HOST', 'mongodb')
    MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
    MONGO_USER = os.getenv('MONGO_USER', 'admin')
    MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', 'admin123')
    MONGO_DB = os.getenv('MONGO_DB', 'pikcha_raw')

    # Kafka
    KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka')
    KAFKA_PORT = int(os.getenv('KAFKA_PORT', 9092))

    # ClickHouse
    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
    CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'strong_password_123')
    CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'pikcha_raw')

    # Redis (опционально)
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'redis_strong_password')
    REDIS_DB = int(os.getenv('REDIS_DB', 0))
    USE_REDIS = os.getenv('USE_REDIS', 'true').lower() == 'true'

    # Шифрование
    ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
    if not ENCRYPTION_KEY:
        raise ValueError("ENCRYPTION_KEY не найден в .env файле!")

    # Настройки
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')