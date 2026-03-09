import os
import json
import random
from faker import Faker
from datetime import datetime, timedelta

stores_path = r'C:\retail_analytics_pikcha\data\stores'
os.makedirs(stores_path, exist_ok=True)

def generate_stores():
    fake = Faker('ru_RU')

    categories = [
        "Зерновые и хлебобулочные изделия",
        "Мясо, рыба, яйца и бобовые",
        "Молочные продукты",
        "Фрукты и ягоды",
        "Овощи и зелень"
    ]
    cities = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Казань', 'Нижний Новгород', 'Самара',
              'Краснодар', 'Воронеж']


    print(f"📁 Сохраняем в: {stores_path}")
    print("🏪 Начинаем генерацию магазинов...")

    # 30 больших магазинов
    for i in range(1, 31):

        store_data = {
            "store_id": f"store-{i:03d}",
            "name": f"Большая Пикча - {fake.street_name()}",
            "store_network": "Большая Пикча",  # ДОБАВЛЕНО
            "store_type_description": "Супермаркет площадью более 200 кв.м. Входит в федеральную сеть из 30 магазинов.",
            "type": "offline",
            "categories": categories,
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email()
            },
            "location": {
                "country": "Россия",
                "city": random.choice(cities),
                "street": f"ул. {fake.street_name()}",
                "house": str(random.randint(1, 100)),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())
                }
            },
            "opening_hours": {
                "mon_fri": "09:00-21:00",
                "sat": "10:00-20:00",
                "sun": "10:00-18:00"
            },

            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }

        # путь к файлу
        filename = os.path.join(stores_path, f"store-{i:03d}.json")
        print(f" Сохраняем: {filename}")

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(store_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Создан: {filename}")



    print(f"    Создано 30 больших магазинов")

    # 15 маленьких магазинов
    for i in range(31, 46):

        store_data = {
            "store_id": f"store-{i:03d}",
            "name": f"Маленькая Пикча - {fake.street_name()}",
            "store_network": "Маленькая Пикча",
            "store_type_description": "Магазин у дома площадью до 100 кв.м. Входит в локальную сеть из 15 магазинов.",
            "type": "offline",  # ДОБАВЛЕНО
            "categories": categories,
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email()
            },
            "location": {
                "country": "Россия",
                "city": random.choice(cities),
                "street": f"ул. {fake.street_name()}",
                "house": str(random.randint(1, 100)),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())
                }
            },
            "opening_hours": {
                "mon_fri": "08:00-22:00",
                "sat": "09:00-21:00",
                "sun": "09:00-20:00"
            },

            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }

        filename = os.path.join(stores_path, f"store-{i:03d}.json")
        print(f"🔄 Сохраняю: {filename}")

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(store_data, f, ensure_ascii=False, indent=2)



    print(f"    Создано 15 маленьких магазинов")
    print(f"\n Готово! Создано 45 файлов магазинов в {stores_path}")


if __name__ == "__main__":
    generate_stores()