import json
import random
import glob
from faker import Faker
import os

fake = Faker('ru_RU')
os.makedirs('data/purchases', exist_ok=True)

def load_json_files(folder_path):
    """Загружает все JSON файлы из папки"""
    files = glob.glob(f'{folder_path}/*.json')
    data = []
    for file_path in files:
        with open(file_path, 'r', encoding='utf-8') as f:
            data.append(json.load(f))
    return data

def create_cart_item(product, quantity):
    """Создает товар для корзины"""
    return {
        "product_id": product['id'],
        "name": product['name'],
        "category": product['group'],
        "quantity": quantity,
        "unit": product['unit'],
        "price_per_unit": product['price'],
        "total_price": round(product['price'] * quantity, 2),
        "kbju": product['kbju'],
        "manufacturer": product['manufacturer']
    }

def get_store_description(store_network):
    """Возвращает описание магазина по его сети"""
    if store_network == "Большая Пикча":
        return "Супермаркет площадью более 200 кв.м. Входит в федеральную сеть из 30 магазинов."
    else:
        return "Магазин у дома площадью менее 100 кв.м. Входит в федеральную сеть из 15 магазинов."

def generate_purchase(purchase_id, products, customers):
    """Генерирует одну покупку"""
    customer = random.choice(customers)
    
    # Выбираем случайные товары
    num_items = random.randint(1, 5)
    selected_products = random.sample(products, num_items)
    
    # Создаем корзину
    items = []
    total_amount = 0
    
    for product in selected_products:
        quantity = random.randint(1, 3)
        item = create_cart_item(product, quantity)
        items.append(item)
        total_amount += item['total_price']
    
    # Определяем параметры покупки
    is_delivery = random.choice([True, False, False])  # 33% доставка
    payment_method = random.choice(["card", "cash", "online"])
    purchase_date = fake.date_time_between(start_date='-90d', end_date='now')
    
    # Формируем данные магазина
    store_network = customer['purchase_location']['store_network']
    
    purchase = {
        "purchase_id": f"ord-{purchase_date.strftime('%Y%m%d')}-{purchase_id:03d}",
        "customer": {
            "customer_id": customer['customer_id'],
            "first_name": customer['first_name'],
            "last_name": customer['last_name'],
            "email": customer['email'],
            "phone": customer['phone'],
            "is_loyalty_member": customer['is_loyalty_member'],
            "loyalty_card_number": customer.get('loyalty_card_number')
        },
        "store": {
            "store_id": customer['purchase_location']['store_id'],
            "store_name": customer['purchase_location']['store_name'],
            "store_network": store_network,
            "store_type_description": get_store_description(store_network),
            "location": {
                "city": customer['purchase_location']['city'],
                "street": customer['purchase_location']['street'],
                "house": customer['purchase_location']['house'],
                "postal_code": customer['purchase_location']['postal_code']
            }
        },
        "items": items,
        "total_amount": round(total_amount, 2),
        "payment_method": payment_method,
        "is_delivery": is_delivery,
        "delivery_address": customer['delivery_address'] if is_delivery else None,
        "purchase_datetime": purchase_date.strftime('%Y-%m-%dT%H:%M:%S')
    }
    
    return purchase

def generate_all_purchases(num_purchases=200):
    """Генерирует все покупки"""
    print("Загрузка данных...")
    
    # Загружаем существующие данные
    products = load_json_files('data/products')
    customers = load_json_files('data/customers')
    
    print(f"Загружено {len(products)} товаров и {len(customers)} покупателей")
    
    # Генерируем покупки
    for i in range(1, num_purchases + 1):
        purchase = generate_purchase(i, products, customers)
        
        # Сохраняем в файл
        filename = f"data/purchases/{purchase['purchase_id']}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(purchase, f, ensure_ascii=False, indent=2)
        
        # Прогресс каждые 20 покупок
        if i % 20 == 0:
            print(f"Создано: {i} покупок...")
    
    print(f"\n✅ Сгенерировано {num_purchases} покупок в папке data/purchases/")

if __name__ == "__main__":
    generate_all_purchases(200)