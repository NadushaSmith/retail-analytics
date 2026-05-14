#Чтение из Kafka и запись в ClickHouse
import json
import sys
import traceback
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from clickhouse_driver import Client, errors as clickhouse_errors

from utils.config import Config

# чтобы логи были сразу видны
# sys.stdout.reconfigure(line_buffering=True)
# sys.stderr.reconfigure(line_buffering=True)


class ClickHouseConsumer:
    def __init__(self):
        print("🚀 ИНИЦИАЛИЗАЦИЯ CONSUMER", flush=True)
        print("=" * 50, flush=True)

        # Подключение к Kafka
        kafka_host = f"{Config.KAFKA_HOST}:{Config.KAFKA_PORT}"
        print(f"📨 Подключение к Kafka: {kafka_host}", flush=True)

        try:
            self.consumer = KafkaConsumer(
                'pikcha_stores',
                'pikcha_products',
                'pikcha_customers',
                'pikcha_purchases',
                bootstrap_servers=[kafka_host],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='pikcha_consumer_group',
                consumer_timeout_ms=60000,
                max_poll_records=500,
                max_poll_interval_ms=300000
            )

            # ========== ДИАГНОСТИКА ==========
            print("\n🔍 ДИАГНОСТИКА KAFKA", flush=True)
            print("-" * 30, flush=True)

            # 1. Проверяем все доступные топики
            all_topics = self.consumer.topics()
            print(f"📋 Все топики в Kafka: {sorted(all_topics)}", flush=True)

            # 2. Проверяем подписку
            subscription = self.consumer.subscription()
            print(f"📋 Подписка consumer: {subscription}", flush=True)

            # 3. Проверяем каждый топик отдельно
            test_topics = ['pikcha_stores', 'pikcha_products', 'pikcha_customers', 'pikcha_purchases']
            print("\n🔎 Проверка каждого топика:", flush=True)

            for topic in test_topics:
                print(f"\n--- {topic} ---", flush=True)

                # Проверяем, есть ли топик в Kafka
                if topic not in all_topics:
                    print(f"❌ Топик {topic} НЕ СУЩЕСТВУЕТ в Kafka!", flush=True)
                    continue

                # Получаем партиции
                partitions = self.consumer.partitions_for_topic(topic)
                print(f"✅ Партиции: {partitions}", flush=True)


                try:

                    test_consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers=[kafka_host],
                        auto_offset_reset='earliest',
                        enable_auto_commit=False,
                        consumer_timeout_ms=5000
                    )


                    test_messages = test_consumer.poll(timeout_ms=5000, max_records=1)

                    if test_messages:
                        for tp, msgs in test_messages.items():
                            print(f"✅ Топик {topic} содержит {len(msgs)} сообщений", flush=True)
                            for msg in msgs:
                                print(f"   Первое сообщение: {msg.value[:100]}...", flush=True)
                                break
                    else:
                        print(f"⚠️ Топик {topic} не содержит сообщений", flush=True)

                    test_consumer.close()

                except Exception as e:
                    print(f"❌ Ошибка при проверке топика {topic}: {e}", flush=True)

            print("\n" + "=" * 50, flush=True)
            # ========== КОНЕЦ ДИАГНОСТИКИ ==========

            # Проверка наличия топиков
            required_topics = {'pikcha_stores', 'pikcha_products', 'pikcha_customers', 'pikcha_purchases'}
            if not required_topics.issubset(all_topics):
                missing = required_topics - all_topics
                print(f"⚠️ Предупреждение: отсутствуют топики {missing}", flush=True)

            print(f"✅ Kafka подключен: {kafka_host}", flush=True)

        except KafkaError as e:
            print(f"❌ Ошибка подключения к Kafka: {e}", flush=True)
            traceback.print_exc()
            raise
        except Exception as e:
            print(f"❌ Неизвестная ошибка Kafka: {e}", flush=True)
            traceback.print_exc()
            raise

        # Подключение к ClickHouse
        print(f"🗄️ Подключение к ClickHouse: {Config.CLICKHOUSE_HOST}:{Config.CLICKHOUSE_PORT}", flush=True)

        try:
            self.ch = Client(
                host=Config.CLICKHOUSE_HOST,
                port=Config.CLICKHOUSE_PORT,
                user=Config.CLICKHOUSE_USER,
                password=Config.CLICKHOUSE_PASSWORD,
                database=Config.CLICKHOUSE_DB,
                connect_timeout=10,
                send_receive_timeout=30,
                sync_request_timeout=30
            )

            result = self.ch.execute('SELECT 1')
            print(f"✅ ClickHouse подключен. Тест: {result[0][0]}", flush=True)

        except clickhouse_errors.Error as e:
            print(f"❌ Ошибка подключения к ClickHouse: {e}", flush=True)
            traceback.print_exc()
            raise
        except Exception as e:
            print(f"❌ Неизвестная ошибка ClickHouse: {e}", flush=True)
            traceback.print_exc()
            raise

        self.stats = {
            'pikcha_stores': 0,
            'pikcha_products': 0,
            'pikcha_customers': 0,
            'pikcha_purchases': 0
        }

        self.batch_size = 100
        self.batch_buffer = {topic: [] for topic in self.stats.keys()}

    def flush_batch(self, topic):
        """Вставка накопленных данных батчем"""
        if not self.batch_buffer[topic]:
            return

        try:
            now = datetime.now()

            if topic == 'pikcha_stores':
                self.ch.execute(
                    "INSERT INTO pikcha_raw.stores_raw (store_id, data, version, inserted_at) VALUES",
                    [(sid, data, ver, now) for sid, data, ver in self.batch_buffer[topic]]
                )
            elif topic == 'pikcha_products':
                self.ch.execute(
                    "INSERT INTO pikcha_raw.products_raw (product_id, data, version, inserted_at) VALUES",
                    [(pid, data, ver, now) for pid, data, ver in self.batch_buffer[topic]]
                )
            elif topic == 'pikcha_customers':
                self.ch.execute(
                    """INSERT INTO pikcha_raw.customers_raw 
                       (customer_id, data, phone_encrypted, email_encrypted, version, inserted_at) VALUES""",
                    [(cid, data, phone, email, ver, now) for cid, data, phone, email, ver in self.batch_buffer[topic]]
                )
            elif topic == 'pikcha_purchases':
                self.ch.execute(
                    "INSERT INTO pikcha_raw.purchases_raw (purchase_id, data, version, inserted_at) VALUES",
                    [(pid, data, ver, now) for pid, data, ver in self.batch_buffer[topic]]
                )

            self.stats[topic] += len(self.batch_buffer[topic])
            print(f"✅ Вставлено {len(self.batch_buffer[topic])} записей в {topic}", flush=True)
            self.batch_buffer[topic] = []

        except Exception as e:
            print(f"❌ Ошибка при вставке батча {topic}: {e}", flush=True)
            print(f"📄 Данные: {self.batch_buffer[topic][:2]}", flush=True)
            traceback.print_exc()
            raise

    def process_message(self, topic, data, version):
        """Обрабатывает одно сообщение из Kafka"""
        try:
            if topic == 'pikcha_stores':
                store_id = data.get('store_id') or data.get('id')
                if store_id:
                    self.batch_buffer[topic].append(
                        (store_id, json.dumps(data, ensure_ascii=False), version)
                    )

            elif topic == 'pikcha_products':
                product_id = data.get('id')
                if product_id:
                    self.batch_buffer[topic].append(
                        (product_id, json.dumps(data, ensure_ascii=False), version)
                    )

            elif topic == 'pikcha_customers':
                customer_id = data.get('customer_id')
                if customer_id:
                    self.batch_buffer[topic].append(
                        (customer_id,
                         json.dumps(data, ensure_ascii=False),
                         data.get('phone_encrypted', ''),
                         data.get('email_encrypted', ''),
                         version)
                    )

            elif topic == 'pikcha_purchases':
                purchase_id = data.get('purchase_id')
                if purchase_id:
                    self.batch_buffer[topic].append(
                        (purchase_id, json.dumps(data, ensure_ascii=False), version)
                    )

            # Если батч набрался - вставляем
            if len(self.batch_buffer[topic]) >= self.batch_size:
                self.flush_batch(topic)

        except Exception as e:
            print(f"❌ Ошибка при обработке сообщения из {topic}: {e}", flush=True)
            print(f"📄 Данные: {data}", flush=True)
            traceback.print_exc()

    def run(self):
        """Запускает consumer"""
        print("\n📥 НАЧАЛО ПОТРЕБЛЕНИЯ СООБЩЕНИЙ", flush=True)
        print("=" * 50, flush=True)

        message_count = 0
        processed_count = 0

        try:
            for message in self.consumer:
                message_count += 1

                if message_count % 100 == 0:
                    print(f"📊 Получено сообщений: {message_count}", flush=True)

                self.process_message(message.topic, message.value, message.timestamp)
                processed_count += 1

                # Периодический commit
                if message_count % 500 == 0:
                    # Сначала вставляем остатки
                    for topic in self.batch_buffer:
                        if self.batch_buffer[topic]:
                            self.flush_batch(topic)
                    self.consumer.commit()
                    print(f"✅ Commit после {message_count} сообщений", flush=True)

        except KeyboardInterrupt:
            print("\n⏹️ Получен сигнал остановки", flush=True)
        except KafkaError as e:
            print(f"\n❌ Ошибка Kafka: {e}", flush=True)
            traceback.print_exc()
        except Exception as e:
            print(f"\n❌ Критическая ошибка: {e}", flush=True)
            traceback.print_exc()
        finally:
            # Вставляем остатки
            print("\n🔄 Завершение работы...", flush=True)
            for topic in self.batch_buffer:
                if self.batch_buffer[topic]:
                    self.flush_batch(topic)

            # Финальный commit
            try:
                self.consumer.commit()
                self.consumer.close()
                print("✅ Kafka consumer закрыт", flush=True)
            except Exception as e:
                print(f"❌ Ошибка при закрытии Kafka: {e}", flush=True)

            print("\n" + "=" * 50, flush=True)
            print("📊 ИТОГОВАЯ СТАТИСТИКА", flush=True)
            print("=" * 50, flush=True)
            print(f"  🏪 Магазины:    {self.stats['pikcha_stores']}", flush=True)
            print(f"  📦 Товары:      {self.stats['pikcha_products']}", flush=True)
            print(f"  👥 Покупатели:  {self.stats['pikcha_customers']}", flush=True)
            print(f"  🛒 Покупки:     {self.stats['pikcha_purchases']}", flush=True)
            print(f"  📨 Всего сообщений: {message_count}", flush=True)
            print("=" * 50, flush=True)

    def close(self):
        """Закрывает соединения"""
        try:
            self.ch.disconnect()
            print("✅ ClickHouse соединение закрыто", flush=True)
        except Exception as e:
            print(f"❌ Ошибка при закрытии ClickHouse: {e}", flush=True)


if __name__ == "__main__":
    consumer = ClickHouseConsumer()
    try:
        consumer.run()
    except Exception as e:
        print(f"❌ Фатальная ошибка: {e}", flush=True)
        traceback.print_exc()
    finally:
        consumer.close()