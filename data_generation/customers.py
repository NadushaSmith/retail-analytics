from faker import Faker
import json
import random
from datetime import datetime, timedelta
import os

fake = Faker('ru_RU')

postalcode_moscow = ["101000", "119021", "121096", "125009", "127006", "129090"]
stories = [
    {
        'store_id': 'store-001',
        'store_name': 'Большая Пикча – Магазин на Тверской',
        'city': 'Москва',
        'street': 'ул. Тверская',
        'house': '15',
        'postal_code': random.choice(postalcode_moscow)
     },
     {
        "store_id": "store-002", 
        "store_name": "Большая Пикча – Магазин на Арбате",
        "city": "Москва",
        "street": "ул. Арбат",
        "house": "25", 
        "postal_code": random.choice(postalcode_moscow)
     },
     {
       "store_id": "store-003",
        "store_name": "Маленькая Пикча – Магазин в Отрадном",
        "city": "Москва", 
        "street": "ул. Декабристов",
        "house": "8",
        "postal_code": random.choice(postalcode_moscow)  
     }
]

def generate_customer(customer_id):
    store = random.choice(stories)

    gender = random.choice(['male', 'female'])
    if gender == 'male':
        first_name = fake.first_name_male()
        last_name = fake.last_name_male()
    else:
        first_name = fake.first_name_female()
        last_name = fake.last_name_female()

    email = f"{first_name.lower()}.{last_name.lower()}@example.com"

    is_loyalty_member = random.choice([True, False])

    customer = {
        'customer_id': f"cus-{customer_id:06d}",
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'phone': fake.phone_number(),
        'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        'gender': gender,
        'registration_date': fake.date_time_between(start_date='-2y', end_date='now').isoformat() + 'Z',
        'is_loyalty_member': is_loyalty_member,
        'loyalty_card_number': f"LOYAL-{random.randint(100000000, 999999999)}" if is_loyalty_member else None,
        'purchase_location': {
            'store_id': store['store_id'],
            'store_name': store['store_name'],
            'store_network': "Большая Пикча" if "Большая" in store["store_name"] else "Маленькая Пикча",
            'country': "Россия",
            'city': store['city'],
            'street': store['street'],
            'house': store['house'],
            'postal_code': fake.postcode()
        },
        'delivery_address': {  # ← ОБЯЗАТЕЛЬНО ДОБАВИТЬ ЭТОТ БЛОК
            "country": "Россия",
            "city": store["city"],
            "street": fake.street_name(),
            "house": str(random.randint(1, 100)),
            "apartment": str(random.randint(1, 200)),
            "postal_code": random.choice(postalcode_moscow)
        },
        'preferences': {
            "preferred_language": "ru",
            "preferred_payment_method": random.choice(["card", "cash", "online"]),
            "receive_promotions": random.choice([True, False])
        }
    }

    return customer


#функция генерации всех покупателей
def generate_all_customers(num_customers=50):
    save_path = r'C:\retail_analytics_pikcha\data\customers'
    os.makedirs(save_path, exist_ok=True)

    for i in range(1, num_customers + 1):
        customer = generate_customer(i)

        filename = f"{save_path}\\{customer['customer_id']}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(customer, f, ensure_ascii=False, indent=2)

            print(f"✅ Создан: {customer['customer_id']}.json - {customer['first_name']} {customer['last_name']}")

    print(f"\n✅ Сгенерировано {num_customers} покупателей в папке data/customers/")


if __name__ == "__main__":
    generate_all_customers(50)   
    







