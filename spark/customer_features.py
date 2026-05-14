# customer_features.py
# ETL для построения признаков покупателей
# Читает данные из ClickHouse MART, считает 30 признаков,
# сохраняет результат в CSV и загружает в MinIO

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date, lit
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


print("\n📥 ЧТЕНИЕ ДАННЫХ ИЗ CLICKHOUSE")

purchases_df = read_clickhouse("purchases_clean")
customers_df = read_clickhouse("customers_clean")
items_df = read_clickhouse("purchase_items_clean")

# ✅ Кэшируем
purchases_df.cache()


сount_purchases = purchases_df.count()
count_customers = customers_df.count()
count_items = items_df.count()

print(f"✅ Загружено покупок: {сount_purchases}")
print(f"✅ Загружено покупателей: {count_customers}")
print(f"✅ Загружено позиций: {count_items}")

#========================= JOIN Items + Purchases =========================
print("\n📊 JOIN items + purchases для расчета 7 признаков")

items_with_purchases = items_df.alias("i") \
    .join(
        purchases_df.select("purchase_id", "customer_id", "purchase_date").alias("p"),
        col("i.purchase_id") == col("p.purchase_id"),
        "inner"
    ) \
    .select(
        col("p.customer_id"),
        col("p.purchase_date"),
        col("i.category"),
        col("i.purchase_id")
    ) \
    .cache()


print(f"✅ items_with_purchases: {items_with_purchases.count()} записей")

# ===================== Расчитываем категорийные признаки покупателей ==================
print("\n📊 РАСЧЁТ КАТЕГОРИЙНЫХ ПРИЗНАКОВ")

# 1. Молочные продукты за 30 дней
milk_recent_buyers_df = items_with_purchases \
    .filter(col("category") == "Молочные продукты") \
    .filter(datediff(current_date(), col("purchase_date")) <= 30) \
    .select("customer_id").distinct() \
    .withColumn("bought_milk_last_30d", lit(1))

# 2. Фрукты за 14 дней
fruit_recent_buyers_df = items_with_purchases \
    .filter(col("category") == "Фрукты и ягоды") \
    .filter(datediff(current_date(), col("purchase_date")) <= 14) \
    .select("customer_id").distinct() \
    .withColumn("bought_fruits_last_14d", lit(1))

# 3. Овощи за 14 дней
veggies_recent_buyers_df = items_with_purchases \
    .filter(col("category") == "Овощи и зелень") \
    .filter(datediff(current_date(), col("purchase_date")) <= 14) \
    .select("customer_id").distinct() \
    .withColumn("bought_veggies_14d", lit(1))

# 11. Хлеб (хотя бы раз)
bakery_buyers_df = items_with_purchases \
    .filter(col("category") == "Зерновые и хлебобулочные изделия") \
    .select("customer_id").distinct() \
    .withColumn("buys_bakery", lit(1))

# 14. Мясо за неделю
meat_last_week_df = items_with_purchases \
    .filter(col("category") == "Мясо, рыба, яйца и бобовые") \
    .filter(datediff(current_date(), col("purchase_date")) <= 7) \
    .select("customer_id").distinct() \
    .withColumn("bought_meat_last_week", lit(1))

# 29. Любитель фруктов (3+ покупок за 30 дней)
fruit_lover_df = items_with_purchases \
    .filter(col("category") == "Фрукты и ягоды") \
    .filter(datediff(current_date(), col("purchase_date")) <= 30) \
    .groupBy("customer_id") \
    .agg(countDistinct("purchase_id").alias("fruit_purchases")) \
    .filter(col("fruit_purchases") >= 3) \
    .select("customer_id").withColumn("fruit_lover", lit(1))

# 30. Вегетарианцы (не покупали мясо 90 дней)
MEAT_CATEGORIES = ["мясо", "рыба", "мясо, рыба, яйца и бобовые"]

meat_buyers_90d = items_with_purchases \
    .filter(lower(col("category")).isin(MEAT_CATEGORIES)) \
    .filter(datediff(current_date(), col("purchase_date")) <= 90) \
    .select("customer_id").distinct()


print("✅ Категорийные признаки посчитаны")

#======================== Агрегация по покупателям ====================
print("\n📊 АГРЕГАЦИЯ ДАННЫХ ПО ПОКУПАТЕЛЯМ")

#  Агрегируем покупки по каждому покупателю
customer_agg = purchases_df.groupBy("customer_id").agg(
    count("purchase_id").alias("total_purchases"),
    sum("total_amount").alias("total_spent"),
    avg("total_amount").alias("avg_basket"),
    avg("products_count").alias("avg_products_count"), # для family_shopper
    countDistinct("store_id").alias("stores_visited"), # для store_loyal / switching_store
    sum("is_delivery").alias("delivery_count"),
    sum("has_organic").alias("organic_count"),
    max("purchase_date").alias("last_purchase_date"),
    min("purchase_date").alias("first_purchase_date"),
    countDistinct("delivery_city").alias("cities_visited"),  # для multicity_buyer
    sum(when(col("products_count") == 1, 1).otherwise(0)).alias("single_item_count"),
    sum(when(col("payment_method") == "cash", 1).otherwise(0)).alias("cash_count"),
    sum(when(col("payment_method") == "card", 1).otherwise(0)).alias("card_count"),
    sum(when(dayofweek(col("purchase_date")).isin(1, 7), 1).otherwise(0)).alias("weekend_count"),
    sum(when(hour(col("purchase_datetime")).between(12, 15), 1).otherwise(0)).alias("early_bird_count"),
)

print(f"✅ customer_agg: {customer_agg.count()} записей")

# Заполняем NULL для покупателей без покупок
customer_agg = customer_agg.fillna({
    "total_purchases": 0,
    "total_spent": 0,
    "avg_basket": 0,
    "avg_products_count": 0,
    "stores_visited": 0,
    "delivery_count": 0,
    "organic_count": 0,
    "cities_visited": 0,
    "single_item_count": 0,
    "cash_count": 0,
    "card_count": 0,
    "weekend_count": 0,
    "early_bird_count": 0,
})

print(f"✅ customer_agg: {customer_agg.count()} записей")
print("✅ Агрегация завершена")

#======================================== Расчет признаков из Purchases =======================
print("\n📊 ШАГ 4: ПРИЗНАКИ ИЗ PURCHASES")

# 4. Recurrent buyer (3+ покупок за 30 дней)
recurrent_recent_df = purchases_df \
    .filter(datediff(current_date(), col("purchase_date")) <= 30) \
    .groupBy("customer_id").count() \
    .filter(col("count") > 2) \
    .select("customer_id").withColumn("recurrent_buyer", lit(1))

# 15-16. Ночные/утренние
night_shoppers_df = purchases_df \
    .filter(hour(col("purchase_datetime")) >= 20) \
    .select("customer_id").distinct() \
    .withColumn("night_shopper", lit(1))

morning_shoppers_df = purchases_df \
    .filter(hour(col("purchase_datetime")) <= 10) \
    .select("customer_id").distinct() \
    .withColumn("morning_shopper", lit(1))

# 22. Varied shopper (≥4 категорий)
varied_shopper_df = purchases_df \
    .filter(col("categories_count") >= 4) \
    .select("customer_id").distinct() \
    .withColumn("varied_shopper", lit(1))

# 28. Recent high spender (>2000 за 7 дней)
recent_high_spender_df = purchases_df \
    .filter(datediff(current_date(), col("purchase_date")) <= 7) \
    .groupBy("customer_id").agg(sum("total_amount").alias("recent_spent_7d")) \
    .filter(col("recent_spent_7d") > 2000) \
    .select("customer_id").withColumn("recent_high_spender", lit(1))

print("✅ Признаки из purchases посчитаны")

#===================================== Сборка customer_features ========================

print("\n🔧 СБОРКА таблицы customer_features")

customer_features = customers_df.join(
    customer_agg,
    customers_df.customer_id == customer_agg.customer_id,
    "left"
).select(
    customers_df.customer_id.alias("customer_id"),
    customers_df.is_loyalty_member,
    customers_df.city,
    customers_df.registration_date,
    customer_agg["total_purchases"],
    customer_agg["total_spent"],
    customer_agg["avg_basket"],
    customer_agg["avg_products_count"],
    customer_agg["stores_visited"],
    customer_agg["delivery_count"],
    customer_agg["organic_count"],
    customer_agg["last_purchase_date"],
    customer_agg["first_purchase_date"],
    customer_agg["cities_visited"],
    customer_agg["single_item_count"],
    customer_agg["cash_count"],
    customer_agg["card_count"],
    customer_agg["weekend_count"],
    customer_agg["early_bird_count"]
).cache()


print(f"✅ customer_features готов: {customer_features.count()} записей")

#=====================================Признаки на customer_features ===========================================
print("\n📊 СПИСОЧНЫЕ ПРИЗНАКИ customer_features")


# 5. inactive_14_30
customer_features = customer_features.withColumn(
    "inactive_14_30",
    when((datediff(current_date(), col("last_purchase_date")).between(14, 30)) &
         (col("total_purchases") > 0), 1).otherwise(0)
)

# 6. new_customer
customer_features = customer_features.withColumn(
    "new_customer",
    when(datediff(current_date(), col("registration_date")) < 30, 1).otherwise(0)
)

# 7-8. Базовые флаги
for field, count_field in [
    ("delivery_user", "delivery_count"),
    ("organic_preference", "organic_count"),
]:
    customer_features = customer_features.withColumn(
        field, when(col(count_field) > 0, 1).otherwise(0)
    )

# 9-10. Корзина
customer_features = customer_features.withColumn(
    "bulk_buyer", when(col("avg_basket") > 1000, 1).otherwise(0)
)
customer_features = customer_features.withColumn(
    "low_cost_buyer", when((col("avg_basket") > 0) & (col("avg_basket") < 200), 1).otherwise(0)
)

# 12-13. Магазины/города
customer_features = customer_features.withColumn(
    "multicity_buyer", when(col("cities_visited") > 1, 1).otherwise(0)
)
customer_features = customer_features.withColumn(
    "loyal_customer", when((col("is_loyalty_member") == 1) & (col("total_purchases") >= 3), 1).otherwise(0)
)

# 17-18. Оплата
customer_features = customer_features.withColumn(
    "prefers_cash",
    when((col("cash_count") / col("total_purchases") >= 0.7) & (col("total_purchases") > 0), 1).otherwise(0)
)
customer_features = customer_features.withColumn(
    "prefers_card",
    when((col("card_count") / col("total_purchases") >= 0.7) & (col("total_purchases") > 0), 1).otherwise(0)
)

# 19. Weekend shopper
customer_features = customer_features.withColumn(
    "weekend_shopper",
    when((col("weekend_count") / col("total_purchases") >= 0.6) & (col("total_purchases") > 0), 1).otherwise(0)
)

# 21. Single item buyer
customer_features = customer_features.withColumn(
    "single_item_buyer",
    when((col("single_item_count") / col("total_purchases") >= 0.5) & (col("total_purchases") > 0), 1).otherwise(0)
)

# 23-24. Store loyalty
customer_features = customer_features.withColumn(
    "store_loyal", when(col("stores_visited") == 1, 1).otherwise(0)
)
customer_features = customer_features.withColumn(
    "switching_store", when(col("stores_visited") > 1, 1).otherwise(0)
)

# 25. Family shopper
customer_features = customer_features.withColumn(
    "family_shopper", when(col("avg_products_count") >= 4, 1).otherwise(0)
)

# 26. Early bird
customer_features = customer_features.withColumn(
    "early_bird", when(col("early_bird_count") > 0, 1).otherwise(0)
)

# 27. No purchases
customer_features = customer_features.withColumn(
    "no_purchases", when(col("total_purchases") == 0, 1).otherwise(0)
)

print("✅ Все признаки на customer_features посчитаны")

#===================================== Vegans ==================

print("\n📊 ШАГ 7: Расчет vegans")

all_customers = customer_features.select("customer_id").distinct()

vegetarian_profile_df = all_customers.join(
    meat_buyers_90d,
    on="customer_id",
    how="left_anti"
).withColumn("vegetarian_profile", lit(1))

print(f"✅ Вегетарианцев: {vegetarian_profile_df.count()}")

#===================== Финальная сборка витрины ========================

print("\n📊 СБОРКА ИТОГОВОЙ ВИТРИНЫ (30+ признаков)")

features_df = customer_features \
    .join(night_shoppers_df,        on="customer_id", how="left") \
    .join(morning_shoppers_df,      on="customer_id", how="left") \
    .join(milk_recent_buyers_df,    on="customer_id", how="left") \
    .join(fruit_recent_buyers_df,   on="customer_id", how="left") \
    .join(veggies_recent_buyers_df, on="customer_id", how="left") \
    .join(recurrent_recent_df,      on="customer_id", how="left") \
    .join(bakery_buyers_df,         on="customer_id", how="left") \
    .join(meat_last_week_df,        on="customer_id", how="left") \
    .join(varied_shopper_df,        on="customer_id", how="left") \
    .join(recent_high_spender_df,   on="customer_id", how="left") \
    .join(fruit_lover_df,           on="customer_id", how="left") \
    .join(vegetarian_profile_df,    on="customer_id", how="left") \
    .fillna({
        "bought_milk_last_30d": 0,
        "bought_fruits_last_14d": 0,
        "bought_veggies_14d": 0,
        "recurrent_buyer": 0,
        "night_shopper": 0,
        "morning_shopper": 0,
        "bought_meat_last_week": 0,
        "recent_high_spender": 0,
        "fruit_lover": 0,
        "vegetarian_profile": 0,
        "buys_bakery": 0,
        "varied_shopper": 0,
    })

# Дополнительный признак: не покупал овощи
features_df = features_df.withColumn(
    "not_bought_veggies_14d",
    when(col("bought_veggies_14d") == 0, 1).otherwise(0)

)
#=============================== 📊 ФИНАЛЬНЫЙ SELECT со всеми признаками ====================================

print("\n📊 ФИНАЛЬНЫЙ SELECT (все признаки)")

features_df = features_df.select(
    col("customer_id"),
    # Базовые признаки (19 шт):
    "inactive_14_30", "new_customer", "delivery_user", "organic_preference",
    "bulk_buyer", "low_cost_buyer", "buys_bakery", "loyal_customer", "multicity_buyer",
    "prefers_cash", "prefers_card", "weekend_shopper",
    "single_item_buyer", "store_loyal", "switching_store", "family_shopper",
    "varied_shopper", "early_bird", "no_purchases",
    # Списочные признаки (11 шт):
    "bought_milk_last_30d", "bought_fruits_last_14d", "bought_veggies_14d",
    "not_bought_veggies_14d", "recurrent_buyer", "bought_meat_last_week",
    "night_shopper", "morning_shopper", "recent_high_spender",
    "fruit_lover", "vegetarian_profile"
)

print(f"✅ Признаков: {len(features_df.columns) - 1}")
print(f"👥 Покупателей: {features_df.count()}")


# Показываем структуру
features_df.printSchema()
features_df.show(5, truncate=False)

print("✅ Все 30 признаков сгенерированы")
print(f"👥 Всего покупателей с признаками: {features_df.count()}")
print(f"📊 Количество признаков: {len(features_df.columns)}")

#==================================== СОХРАНЕНИЕ РЕЗУЛЬТАТОВ В MinIO ================================================

print("\n💾 СОХРАНЕНИЕ РЕЗУЛЬТАТОВ В MinIO")

output_dir = "/opt/airflow/spark/output"
os.makedirs(output_dir, exist_ok=True)

today = datetime.now().strftime("%Y_%m_%d")
file_prefix = f"analytic_result_{today}.csv"

# Временная папка для выгрузки Spark
spark_output_dir = os.path.join(output_dir, file_prefix)

print(f" Сохраняем витрины в CSV через Spark в директорию {spark_output_dir} ...")

# Пишем партицию в CSV через Spark с заголовками
features_df.coalesce(1).write.mode("overwrite").option("header", True).csv(spark_output_dir)

# Находим созданный файл CSV, созданный Spark
csv_file = None
for fname in os.listdir(spark_output_dir):
    if fname.endswith(".csv"):
        csv_file = os.path.join(spark_output_dir, fname)
        break
if csv_file is None:
    raise RuntimeError(f" Не удалось найти CSV-файл в {spark_output_dir}")

# Копируем/переименовываем в нужное имя analytic_result_YYYY_MM_DD.csv
output_file = os.path.join(output_dir, f"{file_prefix}.csv")
os.replace(csv_file, output_file)

print(f"Результат сохранен в: {output_file}")

# Проверяем, что файл создался
if os.path.exists(output_file):
    file_size = os.path.getsize(output_file)
    print(f"📁 Размер файла: {file_size} байт ({file_size / 1024:.2f} КБ)")

    # Показываем первые 5 строк файла
    print("📁 Первые 5 строк файла:")
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 5:
                print(line.strip())
            else:
                break
else:
    print("❌ Ошибка: файл не создался!")

# ==================== ЗАГРУЗКА В MINIO ====================
print("\n☁️ Загрузка результатов в MINIO")

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
    print("❌ boto3 не установлен, устанавливаю...")
    import subprocess

    subprocess.check_call(["pip", "install", "boto3"])
    print("✅ boto3 установлен. Запустите скрипт снова!")

except Exception as e:
    print(f"❌ Ошибка загрузки в MinIO: {e}")
    import traceback

    traceback.print_exc()

print("\n🔍 ШАГ 7: ПРИМЕР РЕЗУЛЬТАТА ИЗ DATAFRAME")

# 8. Показываем пример в Spark
print("\nПервые 5 строк с признаками (из Spark DataFrame):")
features_df.show(5, truncate=False)

# ======================== СТАТИСТИКА ПО ПРИЗНАКАМ ===============================
# 📊 СТАТИСТИКА ПО ПРИЗНАКАМ

print("\n📊 СТАТИСТИКА ПО ПРИЗНАКАМ")
total_customers = features_df.count()
for col_name in features_df.columns[1:]:
    count_1 = features_df.filter(col(col_name) == 1).count()
    percent = (count_1 / total_customers * 100) if total_customers > 0 else 0
    print(f"{col_name:25s}: {count_1:4d} ({percent:5.1f}%)")

print("\n✅ ETL ЗАВЕРШЁН! 30+ признаков готовы")

# ==================== ОЧИСТКА ====================

print("\n🧹 Очистка кэша...")

purchases_df.unpersist()
items_with_purchases.unpersist()
customer_features.unpersist()

spark.stop()

