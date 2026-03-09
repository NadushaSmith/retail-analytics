-- =====================================================
-- 1. Создаем базу данных для MART слоя
-- =====================================================
CREATE DATABASE IF NOT EXISTS pikcha_mart;

-- =====================================================
-- 2. Таблицы для очищенных данных
-- =====================================================

-- Таблица очищенных магазинов
CREATE TABLE IF NOT EXISTS pikcha_mart.stores_clean (
    store_id String,
    store_name String,
    store_network String,
    city String,
    street String,
    latitude Float64,
    longitude Float64,
    manager_name String,
    manager_phone String,
    accepts_online_orders UInt8,
    delivery_available UInt8,
    last_inventory_date Date,
    _loaded_at DateTime DEFAULT now(),
    _cleaned_at DateTime
) ENGINE = ReplacingMergeTree(_cleaned_at)
ORDER BY store_id;

-- Таблица очищенных товаров (ДОБАВЛЕНА КОЛОНКА description)
CREATE TABLE IF NOT EXISTS pikcha_mart.products_clean (
    product_id String,
    name String,
    category String,
    description String,  -- ✅ ДОБАВЛЕНО!
    price Float64,
    unit String,
    calories Float64,
    protein Float64,
    fat Float64,
    carbohydrates Float64,
    is_organic UInt8,
    expiry_days UInt16,
    manufacturer_name String,
    manufacturer_country String,
    _loaded_at DateTime DEFAULT now(),
    _cleaned_at DateTime
) ENGINE = ReplacingMergeTree(_cleaned_at)
ORDER BY product_id;

-- Таблица очищенных покупателей
CREATE TABLE IF NOT EXISTS pikcha_mart.customers_clean (
    customer_id String,
    first_name String,
    last_name String,
    email_hash String,
    phone_hash String,
    birth_date Date,
    age UInt8,
    gender String,
    registration_date DateTime,
    is_loyalty_member UInt8,
    loyalty_card_number String,
    city String,
    preferred_payment_method String,
    _loaded_at DateTime DEFAULT now(),
    _cleaned_at DateTime
) ENGINE = ReplacingMergeTree(_cleaned_at)
ORDER BY customer_id;

-- Таблица очищенных покупок (заголовки)
CREATE TABLE IF NOT EXISTS pikcha_mart.purchases_clean (
    purchase_id String,
    customer_id String,
    store_id String,
    purchase_date Date,
    purchase_datetime DateTime,
    total_amount Float64,
    payment_method String,
    is_delivery UInt8,
    items_count UInt16,
    products_count UInt16,
    categories_count UInt16,
    has_organic UInt8,
    delivery_city String,
    _loaded_at DateTime DEFAULT now(),
    _cleaned_at DateTime
) ENGINE = ReplacingMergeTree(_cleaned_at)
ORDER BY (purchase_date, purchase_id);

-- Таблица деталей покупок (позиции)
CREATE TABLE IF NOT EXISTS pikcha_mart.purchase_items_clean (
    purchase_id String,
    product_id String,
    product_name String,
    category String,
    quantity UInt16,
    price_per_unit Float64,
    total_price Float64,
    is_organic UInt8,
    _loaded_at DateTime DEFAULT now(),
    _cleaned_at DateTime
) ENGINE = ReplacingMergeTree(_cleaned_at)
ORDER BY (purchase_id, product_id);