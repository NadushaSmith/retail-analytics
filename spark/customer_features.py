# customer_features.py
# ETL для построения признаков покупателей
# Читает данные из ClickHouse MART, считает 15 признаков,
# сохраняет результат в CSV и загружает в MinIO

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import os
from dotenv import load_dotenv

# ==================== ЗАГРУЗКА НАСТРОЕК ИЗ .ENV ====================
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

# Параметры подключения к ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_HTTP_PORT = os.getenv('CLICKHOUSE_HTTP_PORT', '8123')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_MART_DB = os.getenv('CLICKHOUSE_MART_DB', 'pikcha_mart')

# ==================== НАЧАЛО РАБОТЫ ====================
print("=" * 70)
print("🚀 ЗАПУСК SPARK ETL ДЛЯ ПОСТРОЕНИЯ ПРИЗНАКОВ ПОКУПАТЕЛЕЙ")
print("=" * 70)
print(f"📁 Читаем из: {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_MART_DB}")

# 1. СОЗДАЁМ SPARK СЕССИЮ
spark = SparkSession.builder \
    .appName("Pikcha Customer Features") \
    .master("local[*]") \
    .config("spark.jars", "jars/clickhouse-jdbc-0.4.6.jar") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Функция для чтения таблиц из ClickHouse
def read_clickhouse(table_name):
    """Читает таблицу из ClickHouse MART"""
    url = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_MART_DB}"
    return spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("user", CLICKHOUSE_USER) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("query", f"SELECT * FROM {table_name}") \
        .load()


print("\n📥 ШАГ 1: ЧТЕНИЕ ДАННЫХ ИЗ CLICKHOUSE")

# 2. ЧИТАЕМ ДАННЫЕ
purchases_df = read_clickhouse("purchases_clean")
customers_df = read_clickhouse("customers_clean")

print(f"✅ Загружено покупок: {purchases_df.count()}")
print(f"✅ Загружено покупателей: {customers_df.count()}")

# Текущая дата для расчётов
current_date = datetime.now().date()

print("\n📊 ШАГ 2: АГРЕГАЦИЯ ДАННЫХ ПО ПОКУПАТЕЛЯМ")

# 3. АГРЕГИРУЕМ ПОКУПКИ ПО КАЖДОМУ ПОКУПАТЕЛЮ
customer_agg = purchases_df.groupBy("customer_id").agg(
    count("purchase_id").alias("total_purchases"),
    sum("total_amount").alias("total_spent"),
    avg("total_amount").alias("avg_basket"),
    avg("products_count").alias("avg_products_count"),
    countDistinct("store_id").alias("stores_visited"),
    sum("is_delivery").alias("delivery_count"),
    sum("has_organic").alias("organic_count"),
    max("purchase_date").alias("last_purchase_date"),
    min("purchase_date").alias("first_purchase_date")
)

# Заполняем NULL для покупателей без покупок
customer_agg = customer_agg.fillna({
    "total_purchases": 0,
    "total_spent": 0,
    "avg_basket": 0,
    "avg_products_count": 0,
    "stores_visited": 0,
    "delivery_count": 0,
    "organic_count": 0
})

print("✅ Агрегация завершена")

print("\n🔧 ШАГ 3: ПОДГОТОВКА ПРОМЕЖУТОЧНЫХ СПИСКОВ")

# 4. СОБИРАЕМ СПИСКИ ID ДЛЯ РАЗНЫХ УСЛОВИЙ

# 4.1. Покупатели за последние 30 дней
recent_buyers = purchases_df.filter(
    datediff(lit(current_date), col("purchase_date")) <= 30
).select("customer_id").distinct().collect()
recent_buyer_ids = [r.customer_id for r in recent_buyers]
print(f"✅ Покупателей за 30 дней: {len(recent_buyer_ids)}")

# 4.2. Часто покупающие (>2 покупок за 30 дней)
frequent_recent = purchases_df.filter(
    datediff(lit(current_date), col("purchase_date")) <= 30
).groupBy("customer_id").count().filter("count > 2").select("customer_id").collect()
frequent_ids = [r.customer_id for r in frequent_recent]
print(f"✅ Часто покупающих: {len(frequent_ids)}")

# 4.3. Ночные покупатели (после 20:00)
night_shoppers = purchases_df.filter(
    hour("purchase_datetime") >= 20
).select("customer_id").distinct().collect()
night_ids = [r.customer_id for r in night_shoppers]
print(f"✅ Ночных покупателей: {len(night_ids)}")

# 4.4. Утренние покупатели (до 10:00)
morning_shoppers = purchases_df.filter(
    hour("purchase_datetime") <= 10
).select("customer_id").distinct().collect()
morning_ids = [r.customer_id for r in morning_shoppers]
print(f"✅ Утренних покупателей: {len(morning_ids)}")

# 4.5. Покупатели с картой (≥70%)
card_ratios = purchases_df.groupBy("customer_id").agg(
    avg(when(col("payment_method") == "card", 1).otherwise(0)).alias("card_ratio")
).filter("card_ratio >= 0.7").select("customer_id").collect()
card_ids = [r.customer_id for r in card_ratios]
print(f"✅ Предпочитающих карту: {len(card_ids)}")

# 4.6. Покупатели с наличными (≥70%)
cash_ratios = purchases_df.groupBy("customer_id").agg(
    avg(when(col("payment_method") == "cash", 1).otherwise(0)).alias("cash_ratio")
).filter("cash_ratio >= 0.7").select("customer_id").collect()
cash_ids = [r.customer_id for r in cash_ratios]
print(f"✅ Предпочитающих наличные: {len(cash_ids)}")

# 4.7. Покупатели с одним товаром (≥50% покупок)
single_items = purchases_df.withColumn(
    "is_single", when(col("products_count") == 1, 1).otherwise(0)
).groupBy("customer_id").agg(
    avg("is_single").alias("single_ratio")
).filter("single_ratio >= 0.5").select("customer_id").collect()
single_ids = [r.customer_id for r in single_items]
print(f"✅ Покупающих один товар: {len(single_ids)}")

# 4.8. Покупатели с покупками в выходные (≥60%)
weekend_shoppers = purchases_df.withColumn(
    "is_weekend", when(dayofweek("purchase_date").isin(1, 7), 1).otherwise(0)
).groupBy("customer_id").agg(
    avg("is_weekend").alias("weekend_ratio")
).filter("weekend_ratio >= 0.6").select("customer_id").collect()
weekend_ids = [r.customer_id for r in weekend_shoppers]
print(f"✅ Покупающих в выходные: {len(weekend_ids)}")

print("✅ Все промежуточные списки готовы")

print("\n🔧 ШАГ 4: ОБЪЕДИНЕНИЕ ДАННЫХ И ГЕНЕРАЦИЯ ПРИЗНАКОВ")

# 5. ОБЪЕДИНЯЕМ ДАННЫЕ
print("🔄 Объединение данных...")

# Переименовываем колонку в customer_agg, чтобы избежать конфликта
customer_agg = customer_agg.withColumnRenamed("customer_id", "agg_customer_id")

# Выполняем join
customer_features = customers_df.join(
    customer_agg,
    customers_df.customer_id == customer_agg.agg_customer_id,
    "left"
).select(
    customers_df.customer_id.alias("customer_id"),
    customers_df.is_loyalty_member,
    customers_df.city,
    customer_agg["total_purchases"],
    customer_agg["total_spent"],
    customer_agg["avg_basket"],
    customer_agg["avg_products_count"],
    customer_agg["stores_visited"],
    customer_agg["delivery_count"],
    customer_agg["organic_count"],
    customer_agg["last_purchase_date"],
    customer_agg["first_purchase_date"]
)

print(f"✅ Объединено записей: {customer_features.count()}")

# Заполняем NULL для покупателей без покупок
customer_features = customer_features.fillna({
    "total_purchases": 0,
    "total_spent": 0,
    "avg_basket": 0,
    "avg_products_count": 0,
    "stores_visited": 0,
    "delivery_count": 0,
    "organic_count": 0
})

# 6. ВЫЧИСЛЯЕМ ПРИЗНАКИ
print("📊 Генерация признаков...")

features_df = customer_features.select(
    col("customer_id"),

    # 1. bought_milk_last_30d
    when(col("customer_id").isin(recent_buyer_ids), 1).otherwise(0).alias("bought_milk_last_30d"),

    # 2. recurrent_buyer
    when(col("customer_id").isin(frequent_ids), 1).otherwise(0).alias("recurrent_buyer"),

    # 3. inactive_14_30
    when(
        (datediff(lit(current_date), col("last_purchase_date")).between(14, 30)) &
        (col("total_purchases") > 0), 1
    ).otherwise(0).alias("inactive_14_30"),

    # 4. delivery_user
    when(col("delivery_count") > 0, 1).otherwise(0).alias("delivery_user"),

    # 5. organic_preference
    when(col("organic_count") > 0, 1).otherwise(0).alias("organic_preference"),

    # 6. bulk_buyer
    when(col("avg_basket") > 1000, 1).otherwise(0).alias("bulk_buyer"),

    # 7. low_cost_buyer
    when((col("avg_basket") < 200) & (col("avg_basket") > 0), 1).otherwise(0).alias("low_cost_buyer"),

    # 8. loyal_customer
    when((col("is_loyalty_member") == 1) & (col("total_purchases") >= 3), 1).otherwise(0).alias("loyal_customer"),

    # 9. night_shopper
    when(col("customer_id").isin(night_ids), 1).otherwise(0).alias("night_shopper"),

    # 10. morning_shopper
    when(col("customer_id").isin(morning_ids), 1).otherwise(0).alias("morning_shopper"),

    # 11. prefers_card
    when(col("customer_id").isin(card_ids), 1).otherwise(0).alias("prefers_card"),

    # 12. prefers_cash
    when(col("customer_id").isin(cash_ids), 1).otherwise(0).alias("prefers_cash"),

    # 13. weekend_shopper
    when(col("customer_id").isin(weekend_ids), 1).otherwise(0).alias("weekend_shopper"),

    # 14. single_item_buyer
    when(col("customer_id").isin(single_ids), 1).otherwise(0).alias("single_item_buyer"),

    # 15. family_shopper
    when(col("avg_products_count") >= 4, 1).otherwise(0).alias("family_shopper")
)

print("✅ Все 15 признаков сгенерированы")
print(f"👥 Всего покупателей с признаками: {features_df.count()}")
print(f"📊 Количество признаков: {len(features_df.columns)}")

print("\n💾 ШАГ 5: СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")

# 7. СОХРАНЯЕМ РЕЗУЛЬТАТ В CSV (в папку проекта)
output_dir = "/opt/airflow/spark/output"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now().strftime("%Y_%m_%d")
output_file = os.path.join(output_dir, f"analytic_result_{today}.csv")

print(f"📊 Конвертация DataFrame в Pandas ({features_df.count()} строк)...")
pandas_df = features_df.toPandas()

# Сохраняем файл
pandas_df.to_csv(output_file, index=False, encoding='utf-8')

print(f"✅ Результат сохранён в: {output_file}")

# Проверяем, что файл создался
if os.path.exists(output_file):
    file_size = os.path.getsize(output_file)
    print(f"📁 Размер файла: {file_size} байт ({file_size / 1024:.2f} КБ)")

    # Показываем первые 5 строк файла
    print("\n📁 Первые 5 строк файла:")
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 5:
                print(line.strip())
            else:
                break
else:
    print("❌ Ошибка: файл не создался!")

# ==================== ЗАГРУЗКА В MINIO ====================
print("\n☁️ ШАГ 6: ЗАГРУЗКА РЕЗУЛЬТАТА В MINIO")

try:
    import boto3
    from botocore.client import Config

    # Настройки MinIO
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minio"
    MINIO_SECRET_KEY = "minio_strong_password"
    MINIO_BUCKET = "pikcha-analytics"

    print(f"📡 Подключение к MinIO: {MINIO_ENDPOINT}")

    # Создаем клиент
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )

    # Проверяем подключение
    buckets = s3.list_buckets()
    print("📋 Доступные buckets:")
    for bucket in buckets['Buckets']:
        print(f"   - {bucket['Name']}")

    # Загружаем файл
    filename = f"analytic_result_{today}.csv"

    if os.path.exists(output_file):
        print(f"📤 Загрузка {filename} в MinIO...")

        s3.upload_file(
            output_file,
            MINIO_BUCKET,
            filename
        )
        print(f"✅ Файл успешно загружен: {MINIO_BUCKET}/{filename}")

        # Проверяем загрузку
        objects = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=filename)
        if 'Contents' in objects:
            print(f"📁 Размер в MinIO: {objects['Contents'][0]['Size']} байт")
    else:
        print(f"❌ Файл не найден: {output_file}")

except ImportError:
    print("⚠️ boto3 не установлен, устанавливаю...")
    import subprocess

    subprocess.check_call(["pip", "install", "boto3"])
    print("✅ boto3 установлен. Запустите скрипт снова!")

except Exception as e:
    print(f"❌ Ошибка загрузки в MinIO: {e}")
    import traceback

    traceback.print_exc()

print("\n🔍 ШАГ 7: ПРИМЕР РЕЗУЛЬТАТА ИЗ DATAFRAME")

# 8. ПОКАЗЫВАЕМ ПРИМЕР ИЗ SPARK
print("\nПервые 5 строк с признаками (из Spark DataFrame):")
features_df.show(5, truncate=False)

# 9. СТАТИСТИКА ПО ПРИЗНАКАМ
print("\n" + "=" * 70)
print("📊 СТАТИСТИКА ПО ПРИЗНАКАМ")
print("=" * 70)

total_customers = features_df.count()
for i, col_name in enumerate(features_df.columns[1:], 1):
    count_1 = features_df.filter(col(col_name) == 1).count()
    percent = (count_1 / total_customers) * 100 if total_customers > 0 else 0
    print(f"{i:2d}. {col_name:25s}: {count_1:3d} покупателей ({percent:5.1f}%)")

print("=" * 70)
print(f"✅ SPARK ETL УСПЕШНО ЗАВЕРШЁН!")
print(f"📁 Результат сохранён в: {output_file}")
print(f"☁️ Результат загружен в MinIO: {MINIO_BUCKET}/{filename}")
print("=" * 70)

spark.stop()