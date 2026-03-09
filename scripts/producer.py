# Чтение из MongoDB, шифрование, отправка в Kafka
import json
from pymongo import MongoClient
from kafka import KafkaProducer
from datetime import datetime

from utils.config import Config
from utils.phone_normalizer import normalize_phone
from utils.encryption import Encryptor


class MongoKafkaProducer:
    def __init__(self):
        print("🚀 Инициализация Producer...")

        # Инициализация шифрования
        self.encryptor = Encryptor(Config.ENCRYPTION_KEY)

        # Подключение к MongoDB
        mongo_uri = f"mongodb://{Config.MONGO_USER}:{Config.MONGO_PASSWORD}@{Config.MONGO_HOST}:{Config.MONGO_PORT}/"
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client[Config.MONGO_DB]
        print(f"✅ MongoDB: {Config.MONGO_HOST}:{Config.MONGO_PORT}")

        # Подключение к Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=[f"{Config.KAFKA_HOST}:{Config.KAFKA_PORT}"],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            compression_type='gzip'
        )
        print(f"✅ Kafka: {Config.KAFKA_HOST}:{Config.KAFKA_PORT}")

        self.stats = {'stores': 0, 'products': 0, 'customers': 0, 'purchases': 0}

    def process_customer(self, doc):
        """Обработка покупателя: нормализация и шифрование"""
        doc.pop('_id', None)

        # Обработка телефона
        raw_phone = doc.get('phone')
        norm_phone = normalize_phone(raw_phone)
        if norm_phone:
            doc['phone_encrypted'] = self.encryptor.encrypt(norm_phone)
            doc['phone_hash'] = self.encryptor.hash_text(norm_phone)
            doc['phone'] = '[ENCRYPTED]'
        else:
            doc['phone_encrypted'] = ''
            doc['phone_hash'] = ''
            doc['phone'] = ''

        # Обработка email
        raw_email = doc.get('email', '')
        if raw_email:
            norm_email = raw_email.lower().strip()
            doc['email_encrypted'] = self.encryptor.encrypt(norm_email)
            doc['email_hash'] = self.encryptor.hash_text(norm_email)
            doc['email'] = '[ENCRYPTED]'
        else:
            doc['email_encrypted'] = ''
            doc['email_hash'] = ''

        doc['_processed_at'] = datetime.now().isoformat()
        return doc

    def process_purchase(self, doc):
        """Обработка покупки: шифрование данных покупателя"""
        doc.pop('_id', None)

        if 'customer' in doc and isinstance(doc['customer'], dict):
            cust = doc['customer']

            if 'phone' in cust:
                norm_phone = normalize_phone(cust['phone'])
                if norm_phone:
                    cust['phone_encrypted'] = self.encryptor.encrypt(norm_phone)
                    cust['phone'] = '[ENCRYPTED]'

            if 'email' in cust:
                norm_email = cust['email'].lower().strip()
                cust['email_encrypted'] = self.encryptor.encrypt(norm_email)
                cust['email'] = '[ENCRYPTED]'

        doc['_processed_at'] = datetime.now().isoformat()
        return doc

    def process_collection(self, collection_name, topic, process_func=None):
        """Обрабатывает коллекцию и отправляет в Kafka"""
        collection = self.db[collection_name]
        count = 0

        for doc in collection.find():
            if process_func:
                doc = process_func(doc)
            else:
                doc.pop('_id', None)
                doc['_processed_at'] = datetime.now().isoformat()

            self.producer.send(topic, doc)
            count += 1

            if count % 50 == 0:
                print(f"   Отправлено {count} документов в {topic}")

        return count

    def run(self):
        """Запускает producer"""
        print("\n📤 Отправка данных в Kafka...")

        # Магазины (без шифрования)
        print("\n🏪 Магазины...")
        self.stats['stores'] = self.process_collection('stores', 'pikcha_stores')

        # Товары (без шифрования)
        print("\n📦 Товары...")
        self.stats['products'] = self.process_collection('products', 'pikcha_products')

        # Покупатели (с шифрованием)
        print("\n👥 Покупатели...")
        self.stats['customers'] = self.process_collection('customers', 'pikcha_customers', self.process_customer)

        # Покупки (с шифрованием)
        print("\n🛒 Покупки...")
        self.stats['purchases'] = self.process_collection('purchases', 'pikcha_purchases', self.process_purchase)

        # Отправляем все накопленные сообщения
        self.producer.flush()

        print("\n" + "=" * 50)
        print("📊 ИТОГИ:")
        print(f"  🏪 Магазины:    {self.stats['stores']}")
        print(f"  📦 Товары:      {self.stats['products']}")
        print(f"  👥 Покупатели:  {self.stats['customers']}")
        print(f"  🛒 Покупки:     {self.stats['purchases']}")
        print("=" * 50)

        return self.stats

    def close(self):
        """Закрывает соединения"""
        self.mongo_client.close()
        self.producer.close()


if __name__ == "__main__":
    producer = MongoKafkaProducer()
    try:
        producer.run()
    finally:
        producer.close()