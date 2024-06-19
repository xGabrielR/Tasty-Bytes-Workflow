CREATE OR REPLACE VIEW tasty_bytes.analytics.order_view AS 
WITH truck AS (
    SELECT DISTINCT 
        truck_id,
        primary_city,
        region,
        CASE WHEN year < 1990 THEN 'bronze'
             WHEN year > 1990 AND year <= 2004 THEN 'silver'
             WHEN year > 2004 THEN 'gold' END AS year_class,
        REPLACE(make, '_', '') AS make
    FROM tasty_bytes.raw_pos.truck
),
location AS (
    SELECT DISTINCT
        location_id,
        location,
        city,
        country
    FROM tasty_bytes.raw_pos.location
),
customer AS (
    select 
        customer_id,
        concat(first_name, ' ', last_name) as customer_name,
        gender,
        e_mail,
        phone_number,
        birthday_date,
        sign_up_date,
        favourite_brand,
        postal_code,
        country
    from tasty_bytes.raw_customer.customer_loyalty
),
orders AS (
    SELECT 
        order_id,
        truck_id,
        location_id,
        customer_id,
        order_ts,
        order_total
    FROM tasty_bytes.raw_pos.order_header
    WHERE customer_id IS NOT NULL
),
unique_orders AS (
    SELECT DISTINCT order_id FROM orders
),
order_detail AS (
    SELECT 
        order_id,
        SUM(quantity) AS total_quantity,
        AVG(quantity) AS avg_quantity,
        AVG(unit_price) AS avg_unit_price,
        COUNT(DISTINCT menu_item_id) AS total_quantity_unique_items 
    FROM tasty_bytes.raw_pos.order_detail
    INNER JOIN unique_orders USING(order_id)
    GROUP BY order_id
),
datas AS (
    SELECT 
        o.order_id,
        o.truck_id,
        o.location_id,
        o.customer_id,
        o.order_ts,
        o.order_total,
        
        d.total_quantity,
        d.avg_quantity,
        d.avg_unit_price,
        d.total_quantity_unique_items,
    
        t.make,
        t.year_class,
        t.primary_city AS truck_primary_city,
        t.region AS truck_region,
    
        l.location,
        l.city AS location_city,
        l.country AS location_country,
    
        c.customer_name,
        c.gender,
        c.e_mail,
        c.phone_number,
        c.birthday_date,
        c.sign_up_date,
        c.favourite_brand AS customer_favourite_brand,
        c.postal_code AS customer_postal_code,
        c.country AS customer_country
        
    FROM order_detail d
    INNER JOIN orders o using(order_id)
    LEFT JOIN truck t USING(truck_id)
    LEFT JOIN location l USING(location_id)
    LEFT JOIN customer c USING(customer_id)
)
SELECT * FROM datas;