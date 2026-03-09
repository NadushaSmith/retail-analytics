import json
from pymongo import MongoClient
import random
from datetime import datetime, timedelta


def main():
    print("=" * 50)
    print("🚀 ЗАГРУЗКА ДАННЫХ В MONGODB")
    print("=" * 50)

    # Подключение к MongoDB
    try:
        client = MongoClient('mongodb://admin:admin123@localhost:27018/')
        db = client['pikcha_raw']
        print("✅ Подключено к MongoDB")
    except Exception as e:
        print(f"❌ Ошибка подключения к MongoDB: {e}")
        return

    # Очищаем коллекции
    print("\n🧹 Очистка старых данных...")
    db.stores.delete_many({})
    db.products.delete_many({})
    db.customers.delete_many({})
    db.purchases.delete_many({})
    print("✅ Коллекции очищены")

    # ============================================
    # 1. Магазины (45 штук)
    # ============================================
    print("\n🏪 СОЗДАНИЕ МАГАЗИНОВ...")

    cities = ["Москва", "Санкт-Петербург", "Казань", "Екатеринбург", "Новосибирск"]
    streets = ["Ленина", "Пушкина", "Гагарина", "Советская", "Мира"]

    for i in range(1, 46):
        store_id = f"store-{i:03d}"
        network = "Большая Пикча" if i <= 30 else "Маленькая Пикча"

        store = {
            "store_id": store_id,
            "store_name": f"{network} — Магазин №{i}",
            "store_network": network,
            "location": {
                "city": random.choice(cities),
                "street": random.choice(streets),
                "house": str(random.randint(1, 100)),
                "postal_code": random.randint(100000, 999999)
            },
            "manager": {
                "name": f"Менеджер {i}",
                "phone": f"+7-{random.randint(900, 999)}-{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}"
            },
            "accepts_online_orders": random.choice([True, False]),
            "delivery_available": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }

        db.stores.insert_one(store)

        if i % 10 == 0:
            print(f"   Создано {i} магазинов")

    stores_count = db.stores.count_documents({})
    print(f"✅ Создано магазинов: {stores_count}")

    # ============================================
    # 2. Товары (20+ штук)
    # ============================================
    print("\n📦 СОЗДАНИЕ ТОВАРОВ...")

    categories = [
        "🥖 Зерновые и хлебобулочные",
        "🥩 Мясо, рыба, яйца",
        "🥛 Молочные продукты",
        "🍏 Фрукты и ягоды",
        "🥦 Овощи и зелень"
    ]

    product_names = [
        "Хлеб ржаной", "Батон", "Говядина", "Курица", "Молоко",
        "Творог", "Сметана", "Кефир", "Йогурт", "Сыр",
        "Яблоки", "Бананы", "Апельсины", "Картофель", "Морковь",
        "Капуста", "Лук", "Чеснок", "Свинина", "Рыба"
    ]

    for i in range(1, 21):
        product_id = f"prd-{1000 + i}"
        category = random.choice(categories)

        product = {
            "id": product_id,
            "name": random.choice(product_names) + f" {i}",
            "group": category,
            "description": f"Описание товара {i}",
            "price": round(random.uniform(30, 500), 2),
            "unit": random.choice(["шт", "кг", "упаковка"]),
            "is_organic": random.choice([True, False]),
            "expiry_days": random.randint(5, 30),
            "manufacturer": {
                "name": f"Производитель {random.randint(1, 10)}",
                "country": "Россия"
            }
        }

        db.products.insert_one(product)

        if i % 5 == 0:
            print(f"   Создано {i} товаров")

    products_count = db.products.count_documents({})
    print(f"✅ Создано товаров: {products_count}")

    # ============================================
    # 3. Покупатели (50 штук)
    # ============================================
    print("\n👥 СОЗДАНИЕ ПОКУПАТЕЛЕЙ...")

    first_names = ["Александр", "Елена", "Дмитрий", "Ольга", "Сергей", "Анна", "Андрей", "Наталья", "Михаил", "Татьяна"]
    last_names = ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Попов", "Васильев", "Михайлов", "Федоров",
                  "Морозов"]

    for i in range(1, 51):
        customer_id = f"cus-{1000 + i}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        customer = {
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
            "phone": f"+7-{random.randint(900, 999)}-{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
            "birth_date": f"{random.randint(1970, 2000)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "gender": random.choice(["male", "female"]),
            "is_loyalty_member": random.choice([True, False]),
            "loyalty_card_number": f"LOYAL-{random.randint(10000000, 99999999)}" if random.choice(
                [True, False]) else None
        }

        db.customers.insert_one(customer)

        if i % 10 == 0:
            print(f"   Создано {i} покупателей")

    customers_count = db.customers.count_documents({})
    print(f"✅ Создано покупателей: {customers_count}")

    # ============================================
    # 4. Покупки (200+ штук)
    # ============================================
    print("\n🛒 СОЗДАНИЕ ПОКУПОК...")

    # Получаем списки ID для ссылок
    store_ids = [s["store_id"] for s in db.stores.find({}, {"store_id": 1})]
    customer_ids = [c["customer_id"] for c in db.customers.find({}, {"customer_id": 1})]
    products = list(db.products.find({}, {"id": 1, "price": 1, "name": 1}))

    payment_methods = ["card", "cash", "online"]

    for i in range(1, 201):
        purchase_id = f"ord-{i:05d}"

        # Выбираем случайного покупателя и магазин
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)

        # Создаем случайный набор товаров (1-4 товара)
        items = []
        total_amount = 0
        num_items = random.randint(1, 4)
        selected_products = random.sample(products, min(num_items, len(products)))

        for product in selected_products:
            quantity = random.randint(1, 3)
            price = product["price"]
            item_total = round(price * quantity, 2)
            total_amount += item_total

            items.append({
                "product_id": product["id"],
                "name": product["name"],
                "quantity": quantity,
                "price_per_unit": price,
                "total_price": item_total
            })

        purchase = {
            "purchase_id": purchase_id,
            "customer": {
                "customer_id": customer_id
            },
            "store": {
                "store_id": store_id
            },
            "items": items,
            "total_amount": round(total_amount, 2),
            "payment_method": random.choice(payment_methods),
            "is_delivery": random.choice([True, False]),
            "purchase_datetime": (datetime.now() - timedelta(days=random.randint(0, 90))).isoformat() + "Z"
        }

        db.purchases.insert_one(purchase)

        if i % 50 == 0:
            print(f"   Создано {i} покупок")

    purchases_count = db.purchases.count_documents({})
    print(f"✅ Создано покупок: {purchases_count}")

    # ============================================
    # ИТОГИ
    # ============================================
    print("\n" + "=" * 50)
    print("📊 ИТОГИ ЗАГРУЗКИ:")
    print(f"  🏪 Магазины:    {stores_count}")
    print(f"  📦 Товары:      {products_count}")
    print(f"  👥 Покупатели:  {customers_count}")
    print(f"  🛒 Покупки:     {purchases_count}")
    print("=" * 50)

    client.close()
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()