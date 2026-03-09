-- =====================================================
-- Materialized Views для автоматической очистки данных
-- =====================================================

-- MV для очистки магазинов
CREATE MATERIALIZED VIEW IF NOT EXISTS pikcha_raw.stores_clean_mv
TO pikcha_mart.stores_clean
AS
SELECT
    JSONExtractString(data, 'store_id') AS store_id,
    lower(trimBoth(JSONExtractString(data, 'store_name'))) AS store_name,
    lower(trimBoth(JSONExtractString(data, 'store_network'))) AS store_network,
    lower(trimBoth(JSONExtractString(data, 'location', 'city'))) AS city,
    lower(trimBoth(JSONExtractString(data, 'location', 'street'))) AS street,
    if(JSONExtractFloat(data, 'location', 'coordinates', 'latitude') != 0,
       JSONExtractFloat(data, 'location', 'coordinates', 'latitude'),
       NULL) AS latitude,
    if(JSONExtractFloat(data, 'location', 'coordinates', 'longitude') != 0,
       JSONExtractFloat(data, 'location', 'coordinates', 'longitude'),
       NULL) AS longitude,
    lower(trimBoth(JSONExtractString(data, 'manager', 'name'))) AS manager_name,
    trimBoth(JSONExtractString(data, 'manager', 'phone')) AS manager_phone,
    if(JSONExtractBool(data, 'accepts_online_orders'), 1, 0) AS accepts_online_orders,
    if(JSONExtractBool(data, 'delivery_available'), 1, 0) AS delivery_available,
    if(toDateOrNull(JSONExtractString(data, 'last_inventory_date')) <= today(),
       toDate(JSONExtractString(data, 'last_inventory_date')),
       NULL) AS last_inventory_date,
    inserted_at AS _loaded_at,
    now() AS _cleaned_at
FROM pikcha_raw.stores_raw
WHERE length(JSONExtractString(data, 'store_id')) > 0
  AND length(JSONExtractString(data, 'store_name')) > 0;

-- MV для очистки товаров
CREATE MATERIALIZED VIEW IF NOT EXISTS pikcha_raw.products_clean_mv
TO pikcha_mart.products_clean
AS
SELECT
    JSONExtractString(data, 'id') AS product_id,
    lower(trimBoth(JSONExtractString(data, 'name'))) AS name,
    lower(trimBoth(JSONExtractString(data, 'group'))) AS category,
    lower(trimBoth(JSONExtractString(data, 'description'))) AS description,
    JSONExtractFloat(data, 'price') AS price,
    lower(trimBoth(JSONExtractString(data, 'unit'))) AS unit,
    JSONExtractFloat(data, 'kbju', 'calories') AS calories,
    JSONExtractFloat(data, 'kbju', 'protein') AS protein,
    JSONExtractFloat(data, 'kbju', 'fat') AS fat,
    JSONExtractFloat(data, 'kbju', 'carbohydrates') AS carbohydrates,
    if(JSONExtractBool(data, 'is_organic'), 1, 0) AS is_organic,
    JSONExtractInt(data, 'expiry_days') AS expiry_days,
    lower(trimBoth(JSONExtractString(data, 'manufacturer', 'name'))) AS manufacturer_name,
    lower(trimBoth(JSONExtractString(data, 'manufacturer', 'country'))) AS manufacturer_country,
    inserted_at AS _loaded_at,
    now() AS _cleaned_at
FROM pikcha_raw.products_raw
WHERE length(JSONExtractString(data, 'id')) > 0;

-- MV для очистки покупателей
CREATE MATERIALIZED VIEW IF NOT EXISTS pikcha_raw.customers_clean_mv
TO pikcha_mart.customers_clean
AS
SELECT
    JSONExtractString(data, 'customer_id') AS customer_id,
    lower(trimBoth(JSONExtractString(data, 'first_name'))) AS first_name,
    lower(trimBoth(JSONExtractString(data, 'last_name'))) AS last_name,
    email_encrypted AS email_hash,
    phone_encrypted AS phone_hash,
    toDateOrNull(JSONExtractString(data, 'birth_date')) AS birth_date,
    dateDiff('year', toDateOrNull(JSONExtractString(data, 'birth_date')), today()) AS age,
    lower(trimBoth(JSONExtractString(data, 'gender'))) AS gender,
    if(toDateTimeOrNull(JSONExtractString(data, 'registration_date')) IS NOT NULL,
       toDateTimeOrNull(JSONExtractString(data, 'registration_date')),
       toDateTime('2000-01-01 00:00:00')) AS registration_date,  -- ✅ ОБРАБОТКА NULL
    if(JSONExtractBool(data, 'is_loyalty_member'), 1, 0) AS is_loyalty_member,
    trimBoth(JSONExtractString(data, 'loyalty_card_number')) AS loyalty_card_number,
    lower(trimBoth(JSONExtractString(data, 'purchase_location', 'city'))) AS city,
    lower(trimBoth(JSONExtractString(data, 'preferences', 'preferred_payment_method'))) AS preferred_payment_method,
    inserted_at AS _loaded_at,  --
    now() AS _cleaned_at
FROM pikcha_raw.customers_raw
WHERE length(JSONExtractString(data, 'customer_id')) > 0;

-- MV для очистки покупок (заголовки)
CREATE MATERIALIZED VIEW IF NOT EXISTS pikcha_raw.purchases_clean_mv
TO pikcha_mart.purchases_clean
AS
SELECT
    JSONExtractString(data, 'purchase_id') AS purchase_id,
    JSONExtractString(data, 'customer', 'customer_id') AS customer_id,
    JSONExtractString(data, 'store', 'store_id') AS store_id,
    toDate(JSONExtractString(data, 'purchase_datetime')) AS purchase_date,
    toDateTime(JSONExtractString(data, 'purchase_datetime')) AS purchase_datetime,
    if(JSONExtractFloat(data, 'total_amount') > 0,
       JSONExtractFloat(data, 'total_amount'),
       0) AS total_amount,
    lower(trimBoth(JSONExtractString(data, 'payment_method'))) AS payment_method,
    if(JSONExtractBool(data, 'is_delivery'), 1, 0) AS is_delivery,
    length(JSONExtractArrayRaw(data, 'items')) AS items_count,
    arraySum(arrayMap(x -> JSONExtractUInt(x, 'quantity'), JSONExtractArrayRaw(data, 'items'))) AS products_count,
    length(arrayDistinct(arrayMap(x -> JSONExtractString(x, 'category'), JSONExtractArrayRaw(data, 'items')))) AS categories_count,
    if(arrayExists(x -> JSONExtractBool(x, 'is_organic'), JSONExtractArrayRaw(data, 'items')), 1, 0) AS has_organic,
    lower(trimBoth(JSONExtractString(data, 'delivery_address', 'city'))) AS delivery_city,
    inserted_at AS _loaded_at,
    now() AS _cleaned_at
FROM pikcha_raw.purchases_raw
WHERE length(JSONExtractString(data, 'purchase_id')) > 0
  AND length(JSONExtractString(data, 'customer', 'customer_id')) > 0;

-- MV для деталей покупок (разворачиваем items)
CREATE MATERIALIZED VIEW IF NOT EXISTS pikcha_raw.purchase_items_clean_mv
TO pikcha_mart.purchase_items_clean
AS
SELECT
    JSONExtractString(data, 'purchase_id') AS purchase_id,
    JSONExtractString(item, 'product_id') AS product_id,
    lower(trimBoth(JSONExtractString(item, 'name'))) AS product_name,
    lower(trimBoth(JSONExtractString(item, 'category'))) AS category,
    JSONExtractUInt(item, 'quantity') AS quantity,
    JSONExtractFloat(item, 'price_per_unit') AS price_per_unit,
    JSONExtractFloat(item, 'total_price') AS total_price,
    if(JSONExtractBool(item, 'is_organic'), 1, 0) AS is_organic,
    inserted_at AS _loaded_at,
    now() AS _cleaned_at
FROM pikcha_raw.purchases_raw
ARRAY JOIN JSONExtractArrayRaw(data, 'items') AS item
WHERE length(JSONExtractString(data, 'purchase_id')) > 0
  AND length(JSONExtractArrayRaw(data, 'items')) > 0;