from faker import Faker
import json
import random
from datetime import date, datetime
import os
fake = Faker('ru_RU')

os.makedirs('data/products', exist_ok=True)

Categories = {
    'Молочные продукты':[
        {'product_name': "Tворог", 'variations': ['5%', '9%', 'Обезжиренный', 'Зерненый', 'Творожная масса с изюмом']},
        {'product_name': "Молоко", 'variations': ['2.5%', '3.2%', '1.5%', 'Безлактозное', 'Топленое']},
        {'product_name': "Сметана", 'variations': ['10%', '15%', '20%', 'Домашняя', 'Из топленого молока']},
        {'product_name': "Йогурты", 'variations': ['Греческий', 'Фруктовый', 'Питьевой', 'Безлактозный', 'Биойогурт']},
        {'product_name': "Кефир", 'variations': ['1%', '2.5%', '3.2%', '4%', 'Биокефир']},
        {'product_name': "Сливки", 'variations': ['10%', '20%', '33%', 'Для кофе', 'Для взбивания']},
        {'product_name': "Ряженка", 'variations': ['2.5%', '3.2%', '4%', 'От бабушки', 'Нежная']},
        {'product_name': "Сыр", 'variations': ['Российский', 'Голандский', 'Тильзер', 'Моцарелла', 'Адыгейский']}
],
    'Овощи и зелень': [
        {'product_name': "Картофель", 'variations': ['Молодой', 'Египетский', 'Воронежский', 'Красный', 'Белый мытый']},
        {'product_name': "Помидоры", 'variations': ['Желтые', 'Розовые', 'Черри', 'Очень вкусные', 'Сливовидные']},
        {'product_name': "Огурцы", 'variations': ['Длинные гладкие', 'Пупырчатые', 'Грунтовые', 'Малосольные', 'Луховицкие']},
        {'product_name': "Капуста", 'variations': ['Белокачанная', 'Молодая', 'Цветная', 'Брокколи', 'Пекинская']},
        {'product_name': "Перец", 'variations': ['Чили', 'Красный', 'Желтый', 'Зеленый', 'Ромиро']},
        {'product_name': "Зелень", 'variations': ['Петрушка', 'Зеленый лук', 'Укроп', 'Кинза', 'Базилик']},
        {'product_name': "лук", 'variations': ['Репчатый', 'Красный', 'Белый', 'Порей', 'Шалот']}
    ],
    'Фрукты и ягоды': [
        {'product_name': "Яблоки", 'variations': ['Зеленые', 'Антоновка', 'Красные', 'Фуджи', 'Годен']},
        {'product_name': "Груши", 'variations': ['Конференц', 'Китайские', 'Вильямс', 'Дюшес', 'Кармен']},
        {'product_name': "Виноград", 'variations': ['Кишмиш', 'Красный', 'Дамские пальчики', 'Мускат', 'Черный кишмиш']},
        {'product_name': "Мандарины", 'variations': ['Сладкие', 'Мараканские', 'Муркотт', 'Новый урожай', 'Азия']},
        {'product_name': "Сливы", 'variations': ['Иранская', 'Молдавская', 'Кубанская', 'Чернослив', 'Розовая']},
        {'product_name': "Персики", 'variations': ['Армения', 'Азербайджан', 'Плоские', 'Крупные', 'Узбекистан']},
        {'product_name': "Бананы", 'variations': ['Аргентинские', 'Мини', 'Красные', 'Зеленый', 'Сушеные']},
        {'product_name': "Клубника", 'variations': ['Домашняя', 'Египетская', 'Фермерская', 'Садовая', 'Викторя']}
    ],
    'Мясо, рыба, яйца и бобовые': [
        {'product_name': "Курица", 'variations': ['Грудка на кости', 'Кириное филе', 'Крылья', 'Голень', 'Тушка куриная']},
        {'product_name': "Говядина", 'variations': ['Стейк', 'Фарш', 'Ребра', 'Лопатка', 'Рубленная']},
        {'product_name': "Свинина", 'variations': ['Шейка', 'Корейка', 'Окорок', 'Рулька', 'Антрекот']},
        {'product_name': "Яйца", 'variations': ['Отборные', '1 категории', '2 категории', 'Деревенские', 'Перепелиные']},
        {'product_name': "Лосось", 'variations': ['Филе', 'Слабосоленый', 'Для ухи', 'Целая', 'Радужный']},
        {'product_name': "Креветки", 'variations': ['Корлевские', 'Тигровые', 'Белоногие', 'Коктельный', 'Северные']},
        {'product_name': "Фасоль", 'variations': ['Красная', 'Белая', 'Стручковая', 'Бланш', 'Свежезамороженная']}
   ],
   'Зерновые и хлебобулочные изделия': [
       {'product_name': "Хлеб", 'variations': ['Зерновой', 'Бородинский', 'Белый', 'Дарницкий', 'Тостовый']},
       {'product_name': "Выпечка", 'variations': ['Улитка с корицей', 'Крендель', 'Сочник', 'Слойка с сыром', 'Чиабатта']},
       {'product_name': "Макароны", 'variations': ['Спирали', 'Спагетти', 'Ракушки', 'Детские', 'Перья']},
       {'product_name': "Мука", 'variations': ['Высший сорт', 'Ржаная', 'Первой категории', 'Рисовая', 'Цельнозерновая']},
       {'product_name': "Хлопья", 'variations': ['Овсяные', 'Гречныевые', 'Кукурузные', 'Пшеничные', 'Мюсли']},
       {'product_name': "Сухари", 'variations': ['Панировочные', 'Чесночные', 'С изюмом', 'Ванильные', 'Топленое']},
       {'product_name': "Печенье", 'variations': ['Овсяное', 'Молочное', 'Юбилейное', 'Крекер', 'Топленое']},
       {'product_name': "Батон", 'variations': ['Нарезной', 'С отрубями', 'Французский', 'Пшеничный для завтрака', 'Молодежный']}
   ]
}

Manufacturers = {
    'Молочные продукты': [
        {"name": "ООО Молочный комбинат", "country": "Россия", "website": "https://moloko.ru", "inn": "7723456789"},
        {"name": "АО ВБД", "country": "Россия", "website": "https://wbd.ru", "inn": "7723567910"},
        {"name": "ПК Молокомбинат", "country": "Россия", "website": "https://molzavod.ru", "inn": "7713789143"},
        {"name": "ООО Буренка", "country": "Россия", "website": "https://burenka.ru", "inn": "7714569874"}
    ],
    'Овощи и зелень': [
        {"name": "ООО Грядка", "country": "Россия", "website": "https://gryadka.ru", "inn": "7743475789"},
        {"name": "ООО Гранд Трейд", "country": "Россия", "website": "https://grandtrade.ru", "inn": "7709456180"},
        {"name": "ООО Лукошко", "country": "Россия", "website": "https://lukoshko.ru", "inn": "7707452581"},
        {"name": "ООО Крутой фрукт", "country": "Россия", "website": "https://fruct.ru", "inn": "7701459124"}
    ],
    'Мясо, рыба, яйца и бобовые': [
        {"name": "ООО Мясной двор", "country": "Россия", "website": "https://myasnik.ru", "inn": "7707481114"},
        {"name": "ООО Птичка", "country": "Россия", "website": "https://ptichka.ru", "inn": "7701456524"},
        {"name": "ООО Золотая рыбка", "country": "Россия", "website": "https://delice.ru", "inn": "7702458761"},
        {"name": "ООО Фермер", "country": "Россия", "website": "https://fermer.ru", "inn": "7705659554"}
    ],
    'Фрукты и ягоды': [
        {"name": "ООО Клубничка", "country": "Россия", "website": "https://klubnichka.ru", "inn": "7701415254"},
        {"name": "ООО Сады и огороды", "country": "Россия", "website": "https://sadyo.ru", "inn": "7716458751"},
        {"name": "ООО Полянка", "country": "Россия", "website": "https://moyapolyanka.ru", "inn": "7715889124"},
        {"name": "ООО Опушка", "country": "Россия", "website": "https://kryghovnik.ru", "inn": "7704999014"}
    ],
    'Зерновые и хлебобулочные изделия': [
         {"name": "АО ХЗ №1", "country": "Россия", "website": "https://hz1.ru", "inn": "7714919814"},
          {"name": "ООО Зернышко", "country": "Россия", "website": "https://zerno.ru", "inn": "7718987127"},
           {"name": "ООО Макарошки", "country": "Россия", "website": "https://makaroshki.ru", "inn": "7718901098"},
            {"name": "ООО Наша Пекарня", "country": "Россия", "website": "https://pekarnya.ru", "inn": "7721954254"}
    ]    
}

#функция для едениц измерения

def unit_category(category, product_name):
    if category == "Овощи и зелень":
        return 'кг'
    elif category == "Фрукты и ягоды":
        return 'кг'
    elif category == "Зерновые и хлебобулочные изделия":
        if any(word in product_name for word in ["Хлеб", "Батон", "Выпечка"]):
            return 'шт'
        else:
            return 'упаковка'
    elif category == "Мясо, рыба, яйца и бобовые":
        if "Яйца" in product_name:
            return 'упаковка'
        else:
            return 'кг'
    else:
        return 'упаковка'
    
# функция для кбжу продуктов
def kbju_products(category, product_name):
    if category == 'Молочные продукты':
        return {
            'calories': random.randint(50, 250),
            'protein': round(random.uniform(2.0, 25.0), 1),
            'fat': round(random.uniform(0.5, 30.0), 1),
            'carbohydrates': round(random.uniform(2.0, 15.0), 1)
        }
    elif category in ['Овощи и зелень', 'Фрукты и ягоды']:
        return {
            'calories': random.randint(20, 80),
            'protein': round(random.uniform(1.0, 5.0), 1),
            'fat': round(random.uniform(0.1, 1.0), 1),
            'carbohydrates': round(random.uniform(3.0, 20.0), 1)
        }
    elif category in ['Мясо, рыба, яйца и бобовые']:
        if 'Яйца' in product_name:
            return {
                'calories': random.randint(140, 180),
                'protein': round(random.uniform(12.0, 14.0), 1),
                'fat': round(random.uniform(10, 12.0), 1),
                'carbohydrates': round(random.uniform(0.5, 1.5), 1)
            }
        else:
            return {
                'calories': random.randint(100, 300),
                'protein': round(random.uniform(15.0, 25.0), 1),
                'fat': round(random.uniform(5.0, 20.0), 1),
                'carbohydrates': round(random.uniform(0.5, 5.0), 1)
        }
    else:
        return {
            'calories': random.randint(300, 400),
            'protein': round(random.uniform(8.0, 15.0), 1),
            'fat': round(random.uniform(1.0, 5.0), 1),
            'carbohydrates': round(random.uniform(60.0, 80.0), 1)
        }


#функция цены товаров
def price_products(category, product_name):
    price_rang = {
        "Молочные продукты": (50, 300),
        "Овощи и зелень": (50, 400),
        "Фрукты и ягоды": (200, 500),
        "Мясо, рыба, яйца и бобовые": (150, 1200),
        "Зерновые и хлебобулочные изделия": (50, 150)
    }
    min_price, max_price = price_rang[category]
    return round(random.uniform(min_price, max_price), 2)

# функция срока годности продуктов
def expiry_days(category):
    if category == "Молочные продукты":
        return random.randint(5, 14)
    elif category in ["Овощи и зелень", "Фрукты и ягоды"]:
        return random.randint(3, 7)
    elif category == ["Мясо, рыба, яйца и бобовые"]:
        return random.randint(2, 7)
    else:
        return random.randint(180, 365)
    
# функция генерации товара по параметрам
def generate_product(product_id, category, base_product):

    variation = random.choice(base_product["variations"])
    unit = unit_category(category, base_product["product_name"])

    print(f"DEBUG: base_product['product_name'] = {base_product['product_name']}")
    print(f"DEBUG: variation = {variation}")

    name = f"{base_product['product_name']} {variation}"

    product = {
        'id': f"prd-{product_id:04d}",
        'name': name,
        'group': category,
        'description': f"{base_product['product_name']} {variation}.",
        'kbju': kbju_products(category, base_product['product_name']),
        'price': price_products(category, base_product['product_name']),
        'unit': unit,
        'origin_country': "Россия",
        'expiry_days': expiry_days(category),
        'is_organic': random.choice([True, False, False, False]),
        'barcode': f"46{random.randint(1000000000, 9999999999):010d}",
        'manufacturer': random.choice(Manufacturers[category])
    }
    return product 

# функция генерации товаров по категориям
def generate_all_products():
    product_id = 1
    
    for category, products in Categories.items():
        for product_data in products:
            for i in range(random.randint(2, 3)):
                product = generate_product(product_id, category, product_data)

                filename = f"data/products/{product['id']}.json"
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(product, f, ensure_ascii=False, indent=2)
            
                print(f"\n Создан {filename} - {product['name']}")
                product_id += 1

    print(f"\n✅ Сгенерировано {product_id-1} товаров в папке data/products/")

if __name__ == "__main__":
    generate_all_products()




    
    



    

