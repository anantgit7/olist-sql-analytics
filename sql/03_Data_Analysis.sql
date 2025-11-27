-- ANALYSIS -  1) Cohort, Retention & Churn Analysis

-- ----------------Cohort View----------------

CREATE OR REPLACE VIEW cohort_view AS
-- 1) Extract customer + purchase month (first day of month format)
WITH cohort1 AS (
  SELECT DISTINCT
      C.customer_unique_id AS cid,
      STR_TO_DATE(DATE_FORMAT(D.full_date, '%Y-%m-01'), '%Y-%m-%d') AS cohort_month
  FROM FactOrders O
  JOIN DimDate D ON O.purchase_date_key = D.date_key
  JOIN DimCustomer C ON C.customer_id = O.customer_id
),

-- 2) Rank all purchase months per customer to find first purchase
cohort2 AS (
  SELECT
      cid AS cohort_cid,
      cohort_month,
      ROW_NUMBER() OVER (PARTITION BY cid ORDER BY cohort_month) AS rnk
  FROM cohort1
)

-- 3) Final cohort table: keep only first purchase per customer
SELECT
    cohort_cid,
    DATE_FORMAT(cohort_month, '%Y-%m') AS cohort_month
FROM cohort2
WHERE rnk = 1;

-- ----------------Activity View----------------

CREATE OR REPLACE VIEW activity_view AS
-- 1) Monthly purchase count for each unique customer
WITH Q1 AS (
  SELECT 
      C.customer_unique_id AS cuid,
      DATE_FORMAT(D.full_date, '%Y-%m') AS year_month1,
      COUNT(*) AS cnt
  FROM FactOrders O
  JOIN DimDate D
    ON O.purchase_date_key = D.date_key
  JOIN DimCustomer C
    ON C.customer_id = O.customer_id
  GROUP BY C.customer_unique_id, DATE_FORMAT(D.full_date, '%Y-%m')
),

-- 2) List of all distinct Year-Months in the dataset
T1 AS (
  SELECT DISTINCT DATE_FORMAT(D.full_date, '%Y-%m') AS all_year_month
  FROM FactOrders O
  JOIN DimDate D
    ON O.purchase_date_key = D.date_key
),

-- 3) All unique customers who ever placed an order
T2 AS (
  SELECT DISTINCT C.customer_unique_id AS cuid
  FROM FactOrders O
  JOIN DimCustomer C
    ON C.customer_id = O.customer_id	
),

-- 4) Full customer × month grid (to detect inactivity)
Full_grid AS (
  SELECT T2.cuid, T1.all_year_month
  FROM T2
  CROSS JOIN T1
)

-- 5) Final activity flag (1 if purchased that month, else 0)
SELECT 
    G.cuid,
    G.all_year_month,
    Q.year_month1,
    CASE WHEN Q.year_month1 IS NOT NULL THEN 1 ELSE 0 END AS purchased_flag
FROM Full_grid G
LEFT JOIN Q1 Q
  ON G.cuid = Q.cuid
 AND G.all_year_month = Q.year_month1
ORDER BY G.cuid, G.all_year_month;



-- ----------------Cohort–Activity Merge (Retention Backbone)----------------


CREATE OR REPLACE VIEW cohort_activity_final AS
SELECT 
    A.cohort_cid,
    A.cohort_month,                     -- cohort month (YYYY-MM)
    B.year_month1 AS activity_month,    -- activity month (YYYY-MM)

    -- Month offset = how many months after cohort_month the activity happened
    CONCAT(
        'M',
        TIMESTAMPDIFF(
            MONTH,
            STR_TO_DATE(CONCAT(A.cohort_month, '-01'), '%Y-%m-%d'),
            STR_TO_DATE(CONCAT(B.year_month1, '-01'), '%Y-%m-%d')
        )
    ) AS month_offset,

    CASE WHEN B.year_month1 IS NULL THEN 0 ELSE 1 END AS flag_purchased  -- purchase indicator
FROM cohort_view AS A
LEFT JOIN activity_view AS B
    ON A.cohort_cid = B.cuid
ORDER BY A.cohort_cid, B.year_month1;



-- ---------------- Retention Analysis View ----------------
CREATE OR REPLACE VIEW retention_view AS 

-- 1) Count customers retained in each month-offset (M0–M23)
WITH cte AS (
SELECT 
    cohort_month, 
    COUNT(CASE WHEN month_offset = 'M0'  THEN 1 END) AS M0,
    COUNT(CASE WHEN month_offset = 'M1'  THEN 1 END) AS M1,
    COUNT(CASE WHEN month_offset = 'M2'  THEN 1 END) AS M2,
    COUNT(CASE WHEN month_offset = 'M3'  THEN 1 END) AS M3,
    COUNT(CASE WHEN month_offset = 'M4'  THEN 1 END) AS M4,
    COUNT(CASE WHEN month_offset = 'M5'  THEN 1 END) AS M5,
    COUNT(CASE WHEN month_offset = 'M6'  THEN 1 END) AS M6,
    COUNT(CASE WHEN month_offset = 'M7'  THEN 1 END) AS M7,
    COUNT(CASE WHEN month_offset = 'M8'  THEN 1 END) AS M8,
    COUNT(CASE WHEN month_offset = 'M9'  THEN 1 END) AS M9,
    COUNT(CASE WHEN month_offset = 'M10' THEN 1 END) AS M10,
    COUNT(CASE WHEN month_offset = 'M11' THEN 1 END) AS M11,
    COUNT(CASE WHEN month_offset = 'M12' THEN 1 END) AS M12,
    COUNT(CASE WHEN month_offset = 'M13' THEN 1 END) AS M13,
    COUNT(CASE WHEN month_offset = 'M14' THEN 1 END) AS M14,
    COUNT(CASE WHEN month_offset = 'M15' THEN 1 END) AS M15,
    COUNT(CASE WHEN month_offset = 'M16' THEN 1 END) AS M16,
    COUNT(CASE WHEN month_offset = 'M17' THEN 1 END) AS M17,
    COUNT(CASE WHEN month_offset = 'M18' THEN 1 END) AS M18,
    COUNT(CASE WHEN month_offset = 'M19' THEN 1 END) AS M19,
    COUNT(CASE WHEN month_offset = 'M20' THEN 1 END) AS M20,
    COUNT(CASE WHEN month_offset = 'M21' THEN 1 END) AS M21,
    COUNT(CASE WHEN month_offset = 'M22' THEN 1 END) AS M22,
    COUNT(CASE WHEN month_offset = 'M23' THEN 1 END) AS M23
FROM cohort_activity_final
GROUP BY cohort_month
)

-- 2) Convert counts → retention percentages
SELECT 
    cohort_month,
    CONCAT(ROUND(100*M0/M0,  2), '%') AS M0,     -- base = 100%
    CONCAT(ROUND(100*M1/M0,  2), '%') AS M1,
    CONCAT(ROUND(100*M2/M0,  2), '%') AS M2,
    CONCAT(ROUND(100*M3/M0,  2), '%') AS M3,
    CONCAT(ROUND(100*M4/M0,  2), '%') AS M4,
    CONCAT(ROUND(100*M5/M0,  2), '%') AS M5,
    CONCAT(ROUND(100*M6/M0,  2), '%') AS M6,
    CONCAT(ROUND(100*M7/M0,  2), '%') AS M7,
    CONCAT(ROUND(100*M8/M0,  2), '%') AS M8,
    CONCAT(ROUND(100*M9/M0,  2), '%') AS M9,
    CONCAT(ROUND(100*M10/M0, 2), '%') AS M10,
    CONCAT(ROUND(100*M11/M0, 2), '%') AS M11,
    CONCAT(ROUND(100*M12/M0, 2), '%') AS M12,
    CONCAT(ROUND(100*M13/M0, 2), '%') AS M13,
    CONCAT(ROUND(100*M14/M0, 2), '%') AS M14,
    CONCAT(ROUND(100*M15/M0, 2), '%') AS M15,
    CONCAT(ROUND(100*M16/M0, 2), '%') AS M16,
    CONCAT(ROUND(100*M17/M0, 2), '%') AS M17,
    CONCAT(ROUND(100*M18/M0, 2), '%') AS M18,
    CONCAT(ROUND(100*M19/M0, 2), '%') AS M19,
    CONCAT(ROUND(100*M20/M0, 2), '%') AS M20,
    CONCAT(ROUND(100*M21/M0, 2), '%') AS M21,
    CONCAT(ROUND(100*M22/M0, 2), '%') AS M22,
    CONCAT(ROUND(100*M23/M0, 2), '%') AS M23
FROM cte
ORDER BY cohort_month;


-- ---------------- Churn Analysis View ----------------
SELECT
    cohort_month,

    CONCAT(ROUND(100 - REPLACE(M0,  '%', ''), 2), '%')  AS C0,
    CONCAT(ROUND(100 - REPLACE(M1,  '%', ''), 2), '%')  AS C1,
    CONCAT(ROUND(100 - REPLACE(M2,  '%', ''), 2), '%')  AS C2,
    CONCAT(ROUND(100 - REPLACE(M3,  '%', ''), 2), '%')  AS C3,
    CONCAT(ROUND(100 - REPLACE(M4,  '%', ''), 2), '%')  AS C4,
    CONCAT(ROUND(100 - REPLACE(M5,  '%', ''), 2), '%')  AS C5,
    CONCAT(ROUND(100 - REPLACE(M6,  '%', ''), 2), '%')  AS C6,
    CONCAT(ROUND(100 - REPLACE(M7,  '%', ''), 2), '%')  AS C7,
    CONCAT(ROUND(100 - REPLACE(M8,  '%', ''), 2), '%')  AS C8,
    CONCAT(ROUND(100 - REPLACE(M9,  '%', ''), 2), '%')  AS C9,
    CONCAT(ROUND(100 - REPLACE(M10, '%', ''), 2), '%')  AS C10,
    CONCAT(ROUND(100 - REPLACE(M11, '%', ''), 2), '%')  AS C11,
    CONCAT(ROUND(100 - REPLACE(M12, '%', ''), 2), '%')  AS C12,
    CONCAT(ROUND(100 - REPLACE(M13, '%', ''), 2), '%')  AS C13,
    CONCAT(ROUND(100 - REPLACE(M14, '%', ''), 2), '%')  AS C14,
    CONCAT(ROUND(100 - REPLACE(M15, '%', ''), 2), '%')  AS C15,
    CONCAT(ROUND(100 - REPLACE(M16, '%', ''), 2), '%')  AS C16,
    CONCAT(ROUND(100 - REPLACE(M17, '%', ''), 2), '%')  AS C17,
    CONCAT(ROUND(100 - REPLACE(M18, '%', ''), 2), '%')  AS C18,
    CONCAT(ROUND(100 - REPLACE(M19, '%', ''), 2), '%')  AS C19,
    CONCAT(ROUND(100 - REPLACE(M20, '%', ''), 2), '%')  AS C20,
    CONCAT(ROUND(100 - REPLACE(M21, '%', ''), 2), '%')  AS C21,
    CONCAT(ROUND(100 - REPLACE(M22, '%', ''), 2), '%')  AS C22,
    CONCAT(ROUND(100 - REPLACE(M23, '%', ''), 2), '%')  AS C23

FROM retention_view
ORDER BY cohort_month;
----


-- ANALYSIS - 2 - SELLER PERFORMANCE ANALYSIS
-- ---------------- Seller Performance Analysis ----------------
-- 1) Performance Analysis

create or replace view seller_performance as 
with cte as (
select seller_id, sum(total_value) as sum1,count(order_id) as cnt1,avg(review_score) as avg1 from FactOrders
group by seller_id
),
cte1 as (
select seller_id,sum1 as total_revenue,cnt1 as total_orders ,sum1/cnt1 as aov, avg1 as avg_rating
from cte
)
-- cte2 as (
select * , CASE 
  WHEN avg_rating >= 4.5 AND total_orders >= 50 THEN 'Top Seller'
  WHEN avg_rating >= 4.0 AND total_orders >= 20 THEN 'Reliable'
  WHEN avg_rating < 4.0 AND total_orders >= 20 THEN 'Needs Improvement'
  WHEN total_orders < 20 AND avg_rating >= 4.5 THEN 'New & Promising'
  ELSE 'Low Volume / Unstable'
END as status
from cte1

select * from seller_performance


-- 2) Percentage wise distribution
select status,concat(round(100*count(status)/(select count(status) from seller_performance),2),"%") as seller_performance_percentage
from seller_performance
group by status
order by seller_performance_percentage desc

