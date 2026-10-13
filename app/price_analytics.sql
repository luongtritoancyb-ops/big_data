
-- 1. Loai bo cac su kien gia trung lap
CREATE OR REPLACE VIEW unique_prices AS
SELECT DISTINCT ON (
    product_id, platform, recorded_at
)
    id,
    product_id,
    platform,
    price,
    recorded_at
FROM price_history
ORDER BY
    product_id,
    platform,
    recorded_at,
    id DESC;


-- 2. Tinh bien dong gia
CREATE OR REPLACE VIEW price_changes AS
WITH history AS (
    SELECT
        product_id,
        platform,
        price,
        recorded_at,
        LAG(price) OVER (
            PARTITION BY product_id, platform
            ORDER BY recorded_at, id
        ) AS old_price
    FROM unique_prices
),
changes AS (
    SELECT
        product_id,
        platform,
        recorded_at,
        old_price,
        price AS new_price,
        price - old_price AS difference,
        ROUND(
            100.0 * (price - old_price)
            / NULLIF(old_price, 0),
            2
        ) AS change_percent
    FROM history
)
SELECT
    *,
    CASE
        WHEN change_percent <= -5 THEN 'BIG_DROP'
        WHEN change_percent < 0 THEN 'PRICE_DOWN'
        WHEN change_percent > 0 THEN 'PRICE_UP'
        ELSE 'UNCHANGED'
    END AS status
FROM changes
WHERE old_price IS NOT NULL;


-- 3. Gia moi nhat cua moi san pham
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (product_id, platform)
    product_id,
    platform,
    price AS current_price,
    recorded_at
FROM unique_prices
ORDER BY
    product_id,
    platform,
    recorded_at DESC,
    id DESC;


-- 4. Danh sach san pham giam gia manh
CREATE OR REPLACE VIEW price_alerts AS
SELECT
    product_id,
    platform,
    old_price,
    new_price,
    difference,
    change_percent,
    recorded_at
FROM price_changes
WHERE change_percent <= -5;
