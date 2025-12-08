-- DATA MODELLING
-- Dim Customers
CREATE TABLE DimCustomer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    zip_code INT,
    city VARCHAR(100),
    state CHAR(2)
);


INSERT INTO DimCustomer (customer_id, customer_unique_id, zip_code, city, state)
SELECT DISTINCT 
    customer_id,
    customer_unique_id,
    zip_code,
    customer_city,
    customer_state
FROM Customers;

Drop table Customers


-- Dim Products

CREATE TABLE DimProduct (
    product_id VARCHAR(50) PRIMARY KEY,
    category_name_en VARCHAR(100),
    weight_g DECIMAL(10,2),
    volume_cm3 DECIMAL(15,2),
    description_length INT,
    photos_qty INT
);

INSERT INTO DimProduct (product_id, category_name_en, weight_g, volume_cm3, description_length, photos_qty)
SELECT 
    p.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name) AS category_name_en,
    p.product_weight_g,
    (p.product_length_cm * p.product_height_cm * p.product_width_cm) AS volume_cm3,
    p.product_description_length,
    p.product_photos_qty
FROM products p
LEFT JOIN category_name_translate t
    ON p.product_category_name = t.product_category_name;

drop table products
drop table category_name_translate

-- Dim Seller

CREATE TABLE DimSeller (
    seller_id VARCHAR(50) PRIMARY KEY,
    zip_code_prefix INT,
    city VARCHAR(100),
    state CHAR(2)
);


INSERT INTO DimSeller (seller_id, zip_code_prefix, city, state)
SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM sellers;

drop table sellers


-- Dim date


SELECT 
    LEAST(
        MIN(NULLIF(order_purchase_timestamp, '0000-00-00 00:00:00')), 
        MIN(NULLIF(order_approved_at, '0000-00-00 00:00:00')), 
        MIN(NULLIF(order_delivered_carrier_date, '0000-00-00 00:00:00')), 
        MIN(NULLIF(order_delivered_customer_date, '0000-00-00 00:00:00')), 
        MIN(NULLIF(order_estimated_delivery_date, '0000-00-00 00:00:00')), 
        MIN(NULLIF(shipping_limit_date, '0000-00-00 00:00:00')), 
        MIN(NULLIF(review_creation_date, '0000-00-00 00:00:00')), 
        MIN(NULLIF(review_answer_timestamp, '0000-00-00 00:00:00'))
    ) AS min_dataset_date,
    GREATEST(
        MAX(NULLIF(order_purchase_timestamp, '0000-00-00 00:00:00')), 
        MAX(NULLIF(order_approved_at, '0000-00-00 00:00:00')), 
        MAX(NULLIF(order_delivered_carrier_date, '0000-00-00 00:00:00')), 
        MAX(NULLIF(order_delivered_customer_date, '0000-00-00 00:00:00')), 
        MAX(NULLIF(order_estimated_delivery_date, '0000-00-00 00:00:00')), 
        MAX(NULLIF(shipping_limit_date, '0000-00-00 00:00:00')), 
        MAX(NULLIF(review_creation_date, '0000-00-00 00:00:00')), 
        MAX(NULLIF(review_answer_timestamp, '0000-00-00 00:00:00'))
    ) AS max_dataset_date
FROM orders
JOIN order_items USING(order_id)
JOIN order_reviews USING(order_id);


CREATE TABLE DimDate (
    date_key INT PRIMARY KEY,   -- YYYYMMDD
    full_date DATE,
    year INT,
    month INT,
    day INT,
    week INT,
    weekday VARCHAR(10),
    quarter INT
);


SET @start = '2016-09-04';
SET @end = '2020-04-09';

INSERT INTO DimDate (date_key, full_date, year, month, day, week, weekday, quarter)
SELECT 
    DATE_FORMAT(d, '%Y%m%d') + 0 AS date_key,
    d AS full_date,
    YEAR(d),
    MONTH(d),
    DAY(d),
    WEEK(d),
    DAYNAME(d),
    QUARTER(d)
FROM (
    SELECT ADDDATE(@start, t4.i*1000 + t3.i*100 + t2.i*10 + t1.i) d
    FROM 
      (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t1,
      (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t2,
      (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t3,
      (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t4
) v
WHERE d BETWEEN @start AND @end;
select * from DimDate
-- Dim Payment

CREATE TABLE DimPayment (
    payment_key INT AUTO_INCREMENT PRIMARY KEY,
    payment_type VARCHAR(20) -- Credit card, Boleto, etc.
);


insert into DimPayment(payment_type)
select distinct payment_type from 
order_payments



-- Dim Geolocation
CREATE TABLE DimGeolocation (
    zip_code_prefix INT PRIMARY KEY,
    city VARCHAR(100),
    state CHAR(2),
    latitude FLOAT,
    longitude FLOAT
);



DROP table  DimGeolocation
INSERT INTO DimGeolocation (zip_code_prefix, city, state, latitude, longitude)
SELECT DISTINCT 
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state,
    geolocation_lat,
    geolocation_lng
FROM geolocation;


INSERT INTO DimGeolocation (zip_code_prefix, city, state, latitude, longitude)
SELECT 
    geolocation_zip_code_prefix,
    MIN(geolocation_city) AS city,
    MIN(geolocation_state) AS state,
    AVG(geolocation_lat) AS latitude,
    AVG(geolocation_lng) AS longitude
FROM geolocation
GROUP BY geolocation_zip_code_prefix;

select * from DimGeolocation


-- Fact Order
drop table FactOrders

CREATE TABLE FactOrders (
    order_id VARCHAR(50),
    order_item_id INT,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    purchase_date_key INT,
    delivered_date_key INT,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    total_value DECIMAL(10,2),
    review_score INT,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (customer_id) REFERENCES DimCustomer(customer_id),
    FOREIGN KEY (product_id) REFERENCES DimProduct(product_id),
    FOREIGN KEY (seller_id) REFERENCES DimSeller(seller_id),
    FOREIGN KEY (purchase_date_key) REFERENCES DimDate(date_key),
    FOREIGN KEY (delivered_date_key) REFERENCES DimDate(date_key)
);


INSERT INTO FactOrders (
    order_id,
    order_item_id,
    customer_id,
    product_id,
    seller_id,
    purchase_date_key,
    delivered_date_key,
    price,
    freight_value,
    total_value,
    review_score
)
SELECT
    oi.order_id,
    oi.order_item_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    DATE_FORMAT(o.order_purchase_timestamp, '%Y%m%d') AS purchase_date_key,
    CASE 
        WHEN o.order_delivered_customer_date IS NULL 
             OR o.order_delivered_customer_date = '0000-00-00'
        THEN 19000101
        ELSE DATE_FORMAT(o.order_delivered_customer_date, '%Y%m%d')
    END AS delivered_date_key,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_value,
    r.avg_review_score
FROM order_items oi
JOIN orders o
    ON oi.order_id = o.order_id
LEFT JOIN (
    SELECT order_id, ROUND(AVG(review_score)) AS avg_review_score
    FROM order_reviews
    GROUP BY order_id
) r
    ON o.order_id = r.order_id;


------------- Fact Payment
drop table FactPayments

CREATE TABLE FactPayments (
    order_id VARCHAR(50),
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    PRIMARY KEY (order_id, payment_type)
);


INSERT INTO FactPayments (
    order_id,
    payment_type,
    payment_installments,
    payment_value
)
SELECT
    order_id,
    payment_type,
    MAX(payment_installments) AS payment_installments, -- take highest installment plan
    SUM(payment_value) AS payment_value                 -- sum up split payments
FROM order_payments
GROUP BY order_id, payment_type;
