from pyspark.sql import SparkSession
import os

# Текущая директория (С:\retail_analytics_pikcha\spark)
current_dir = os.path.dirname(os.path.abspath(__file__))
jar_path = os.path.join(current_dir, "jars", "clickhouse-jdbc-0.4.6.jar")

print(f"📁 Текущая директория: {current_dir}")
print(f"📁 Путь к драйверу: {jar_path}")
print(f"📁 Файл существует: {os.path.exists(jar_path)}")

if not os.path.exists(jar_path):
    print(" Драйвер не найден!")
    # Попробуем найти драйвер в других местах
    alt_path = os.path.join(os.path.dirname(current_dir), "spark", "jars", "clickhouse-jdbc-0.4.6.jar")
    print(f"📁 Альтернативный путь: {alt_path}")
    print(f"📁 Альтернативный файл существует: {os.path.exists(alt_path)}")
    exit(1)

spark = SparkSession.builder \
    .appName("ClickHouse Test") \
    .master("local[*]") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

print("✅ Spark сессия создана")

try:
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://localhost:8123/pikcha_mart") \
        .option("user", "default") \
        .option("password", "strong_password_123") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("query", "SELECT count(*) as cnt FROM customers_clean") \
        .load()

    print("✅ Подключение к ClickHouse успешно!")
    df.show()

except Exception as e:
    print(f" Ошибка: {e}")

spark.stop()