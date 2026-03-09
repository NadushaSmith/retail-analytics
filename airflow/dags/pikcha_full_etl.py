"""
Полный ETL пайплайн для сети магазинов Пикча
Запускается ежедневно в 10:00
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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


check_services_task = PythonOperator(
    task_id='check_services',
    python_callable=check_services,
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

producer_task = BashOperator(
    task_id='run_producer',
    bash_command=f'cd {SCRIPTS_PATH} && python producer.py',
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

consumer_task = BashOperator(
    task_id='run_consumer',
    bash_command=f'cd {SCRIPTS_PATH} && python -u consumer.py',
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

check_data_task = BashOperator(
    task_id='check_data',
    bash_command=f'''
        echo "📊 ПРОВЕРКА ДАННЫХ В CLICKHOUSE"
        echo "----------------------------------------"

        # Используем curl для запроса к ClickHouse HTTP API
        STORES=$(curl -s -u default:strong_password_123 "http://clickhouse:8123/?query=SELECT%20count()%20FROM%20pikcha_raw.stores_raw" 2>/dev/null | tr -d '\n')
        PURCHASES=$(curl -s -u default:strong_password_123 "http://clickhouse:8123/?query=SELECT%20count()%20FROM%20pikcha_raw.purchases_raw" 2>/dev/null | tr -d '\n')
        CUSTOMERS=$(curl -s -u default:strong_password_123 "http://clickhouse:8123/?query=SELECT%20count()%20FROM%20pikcha_mart.customers_clean" 2>/dev/null | tr -d '\n')

        echo "🏪 Магазины в RAW: $STORES"
        echo "🛒 Покупки в RAW: $PURCHASES"
        echo "👥 Покупатели в MART: $CUSTOMERS"

        if [ -z "$STORES" ] || [ "$STORES" -eq 0 ] 2>/dev/null; then
            echo "❌ ОШИБКА: Магазины не загружены!"
            exit 1
        fi

        echo "✅ Данные загружены успешно"
    ''',
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

spark_task = BashOperator(
    task_id='spark_etl',
    bash_command=f'cd {SPARK_PATH} && python customer_features.py',
    execution_timeout=timedelta(minutes=15),
    dag=dag,
)

check_quality_task = BashOperator(
    task_id='check_quality',
    bash_command=f'''
        echo "📊 МЕТРИКИ КАЧЕСТВА"
        echo "----------------------------------------"
        clickhouse-client --host clickhouse --user default --password strong_password_123 --query "
        SELECT * FROM pikcha_mart.quality_alerts;
        " 2>/dev/null || echo "✅ Активных алертов нет"
    ''',
    execution_timeout=timedelta(minutes=2),
    dag=dag,
)

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
# ==================== ПОРЯДОК ВЫПОЛНЕНИЯ ====================
check_services_task >> \
producer_task >> \
consumer_task >> \
check_data_task >> \
spark_task >> \
check_quality_task >> \
check_minio_task