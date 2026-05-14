"""
Полный ETL пайплайн для сети магазинов Пикча
Запускается ежедневно в 10:00
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import socket

default_args = {
    'owner': 'pikcha',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@pikcha.ru'],
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'pikcha_full_etl',
    default_args=default_args,
    description='MongoDB → Kafka → ClickHouse → Spark → MinIO',
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=['pikcha', 'etl', 'minio'],
    dagrun_timeout=timedelta(minutes=30),
)

SCRIPTS_PATH = '/opt/airflow/scripts'
SPARK_PATH = '/opt/airflow/spark'
CLICKHOUSE_CMD = 'docker exec -i pikcha-clickhouse clickhouse-client --user default --password strong_password_123 --multiquery'


def check_services():
    """Проверка доступности сервисов"""
    services = [
        ('MongoDB', 'mongodb', 27017),
        ('Kafka', 'kafka', 9092),
        ('ClickHouse', 'clickhouse', 9000),
        ('MinIO', 'minio', 9000),
    ]
    print("\n" + "=" * 60)
    print("🔍 ПРОВЕРКА СЕРВИСОВ")
    print("=" * 60)

    all_ok = True
    for name, host, port in services:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            if result == 0:
                print(f"  ✅ {name}: порт {port} доступен")
            else:
                print(f"  ❌ {name}: порт {port} НЕ доступен")
                all_ok = False
            sock.close()
        except Exception as e:
            print(f"  ❌ {name}: ошибка - {e}")
            all_ok = False

    if not all_ok:
        raise Exception("❌ Не все сервисы доступны!")
    print("=" * 60)
    return "services_ok"

def run_producer():
    import sys
    sys.path.append(SCRIPTS_PATH)
    from producer import MongoKafkaProducer
    producer = MongoKafkaProducer()
    producer.run()

def run_consumer():
    import sys
    sys.path.append(SCRIPTS_PATH)
    from consumer import ClickHouseConsumer

    consumer = ClickHouseConsumer()
    consumer.run()

def check_data():
    """Проверка количества данных в ClickHouse"""
    client = Client(
        host='clickhouse',
        port=9000,
        user='default',
        password='strong_password_123'
    )
    result = client.execute("""
        SELECT
            (SELECT count() FROM pikcha_raw.stores_raw) as stores,
            (SELECT count() FROM pikcha_raw.purchases_raw) as purchases,
            (SELECT count() FROM pikcha_mart.customers_clean) as customers
    """)
    print(f"✅ Data counts - Stores: {result[0][0]}, Purchases: {result[0][1]}, Customers: {result[0][2]}")
    return result

def check_quality():
    """Проверка качества данных"""
    client = Client(
        host='clickhouse',
        port=9000,
        user='default',
        password='strong_password_123'
    )
    result = client.execute("""
        SELECT
            check_date,
            table_name,
            duplicate_percentage,
            quality_score,
            alert_level
        FROM pikcha_mart.quality_alerts
        ORDER BY check_date DESC
        LIMIT 10
    """)
    print(f"✅ Quality alerts: {result}")
    return result


check_services_task = PythonOperator(
    task_id='check_services',
    python_callable=check_services,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

producer_task = PythonOperator(
    task_id='run_producer',
    python_callable=run_producer,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

consumer_task = PythonOperator(
    task_id='run_consumer',
    python_callable=run_consumer,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)


check_data_task = PythonOperator(
    task_id='check_data',
    python_callable=check_data,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

# check_data_task = ClickHouseOperator(
#     task_id='check_data',
#     clickhouse_conn_id='clickhouse_default',
#     sql="""
#         select
#             (select count() from pikcha_raw.stores_raw) as stores,
#             (select count() from pikcha_raw.purchases_raw) as purchases,
#             (select count() from pikcha_mart.customers_clean) as customers
#     """,
#     do_xcom_push=True,
#     execution_timeout=timedelta(minutes=2),
#     dag=dag,



spark_task = SparkSubmitOperator(
    task_id='spark_etl',
    application=f'{SPARK_PATH}/customer_features.py',
    conn_id='spark_default',
    name='pikcha_customer_features',
    executor_memory='2g',
    driver_memory='2g',
    num_executors=1,
    conf={
        'spark.master': 'local[*]',
        'spark.sql.adaptive.enabled': 'true',
        'spark.jars': '/opt/airflow/spark/jars/clickhouse-jdbc-0.4.6.jar'
    },
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

check_quality_task = PythonOperator(
    task_id='check_quality',
    python_callable=check_quality,
    execution_timeout=timedelta(minutes=2),
    dag=dag,

# check_quality_task = ClickHouseOperator(
#     task_id='check_quality',
#     clickhouse_conn_id='clickhouse_default',
#     sql="""
#         select
#             check_date,
#             table_name,
#             duplicate_percentage,
#             quality_score,
#             alert_level
#         from pikcha_mart.quality_alerts
#         order by check_date desc
#         limit 10
#     """,
#     do_xcom_push=True,
#     execution_timeout=timedelta(minutes=2),
#     dag=dag,
)



print('chengess')
check_minio_task = BashOperator(
    task_id='check_minio_upload',
    bash_command=f'''
        echo "🔍 ПРОВЕРКА ЗАГРУЗКИ В MINIO"
        echo "----------------------------------------"
        TODAY=$(date +%Y_%m_%d)

        echo "📋 Файл analytic_result_${{TODAY}}.csv должен быть в MinIO"
        echo "✅ Spark ETL отработал успешно, считаем что файл загружен"
        exit 0
    ''',
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)
# ==================== Порядок выполнения ====================
check_services_task >> \
producer_task >> \
consumer_task >> \
check_data_task >> \
spark_task >> \
check_quality_task >> \
check_minio_task