create database inventory_optimization;
use inventory_optimization;

/*--------------------CREATE 3 TABLE----------------------------------------------------------------------*/
CREATE TABLE demand_for_casting (
    product_id INT,
    store_id INT,
    date DATE,
    sales_quantity INT,
    price DECIMAL(10,2),
    promotions VARCHAR(50),
    seasonality_factors VARCHAR(50),
    external_factors VARCHAR(50),
    demand_trend VARCHAR(50),
    customer_segments VARCHAR(50),
    PRIMARY KEY (product_id, store_id, date)
);

CREATE TABLE inventory_monitoring (
    product_id INT,
    store_id INT,
    stock_levels INT,
    supplier_lead_time INT,
    stockout_frequency INT,
    reorder_point INT,
    expiry_date DATE,
    warehouse_capacity INT,
    order_fulfillment_time INT,
    PRIMARY KEY (product_id, store_id)
);

CREATE TABLE pricing_optimization (
    product_id INT,
    store_id INT,
    price DECIMAL(10,2),
    competitor_prices DECIMAL(10,2),
    discounts DECIMAL(10,2),
    sales_volume INT,
    customer_reviews DECIMAL(3,2),
    return_rate DECIMAL(5,2),
    storage_cost DECIMAL(10,2),
    elasticity_index DECIMAL(5,2),
    PRIMARY KEY (product_id, store_id)
);

/*--------------------END----------------------------------------------------------------------*/

DESC demand_for_casting;
DESC inventory_monitoring;
DESC pricing_optimization;

SHOW VARIABLES LIKE 'secure_file_priv';
SET GLOBAL local_infile = 1;


/*--------------------LOAD DATA INTO TABLE----------------------------------------------------------------------*/
/*--------------------demand_for_casting------------------------------------*/
ALTER TABLE demand_for_casting DROP PRIMARY KEY;
ALTER TABLE demand_for_casting
ADD COLUMN index_id INT AUTO_INCREMENT PRIMARY KEY;
CREATE INDEX idx_product_store_demand ON demand_for_casting(Product_ID, Store_ID,date);
SHOW INDEX FROM demand_for_casting;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\demand_forecasting.csv'
INTO TABLE demand_for_casting
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, date, store_id, sales_quantity, price, promotions, seasonality_factors, external_factors, demand_trend, customer_segments);


SELECT * FROM demand_for_casting;

/*--------------------end---------------------------------------------------*/


/*--------------------inventory_monitoring----------------------------------*/

ALTER TABLE inventory_monitoring DROP PRIMARY KEY;
ALTER TABLE inventory_monitoring
ADD COLUMN index_id INT AUTO_INCREMENT PRIMARY KEY;
CREATE INDEX idx_product_store ON inventory_monitoring(Product_ID, Store_ID);
SHOW INDEX FROM inventory_monitoring;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/inventory_monitoring.csv'
INTO TABLE inventory_monitoring
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, store_id, stock_levels, supplier_lead_time, stockout_frequency, reorder_point, expiry_date, warehouse_capacity, order_fulfillment_time);

SELECT * FROM inventory_monitoring;

/*--------------------end---------------------------------------------*/

/*--------------------pricing_optimization----------------------------*/

ALTER TABLE pricing_optimization DROP PRIMARY KEY;
ALTER TABLE pricing_optimization
ADD COLUMN index_id INT AUTO_INCREMENT PRIMARY KEY;
CREATE INDEX idx_product_store_pricing ON pricing_optimization(Product_ID, Store_ID);
SHOW INDEX FROM pricing_optimization;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\pricing_optimization.csv'
INTO TABLE pricing_optimization
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, store_id, price, competitor_prices, discounts, sales_volume, customer_reviews, return_rate, storage_cost, elasticity_index);

SELECT * FROM pricing_optimization;

/*--------------------end----------------------------------------------------------------*/

/*--------------------JOIN 3 TABLES----------------------------------------------------------------*/

SELECT 
    dfc.product_id,
    dfc.store_id,
    dfc.date,
    dfc.sales_quantity,
    dfc.price AS demand_price,
    dfc.promotions,
    dfc.seasonality_factors,
    dfc.external_factors,
    dfc.demand_trend,
    dfc.customer_segments,
    im.stock_levels,
    im.supplier_lead_time,
    im.stockout_frequency,
    im.reorder_point,
    im.expiry_date,
    im.warehouse_capacity,
    im.order_fulfillment_time,
    po.price AS optimized_price,
    po.competitor_prices,
    po.discounts,
    po.sales_volume,
    po.customer_reviews,
    po.return_rate,
    po.storage_cost,
    po.elasticity_index
FROM demand_for_casting dfc
LEFT JOIN inventory_monitoring im
  ON dfc.product_id = im.product_id 
  AND dfc.store_id = im.store_id
LEFT JOIN pricing_optimization po
  ON dfc.product_id = po.product_id 
  AND dfc.store_id = po.store_id;

CREATE OR REPLACE VIEW inventory_optimization_view AS
SELECT 
    dfc.product_id,
    dfc.store_id,
    dfc.date,
    dfc.sales_quantity,
    dfc.price AS demand_price,
    dfc.promotions,
    dfc.seasonality_factors,
    dfc.external_factors,
    dfc.demand_trend,
    dfc.customer_segments,
    im.stock_levels,
    im.supplier_lead_time,
    im.stockout_frequency,
    im.reorder_point,
    im.expiry_date,
    im.warehouse_capacity,
    im.order_fulfillment_time,
    po.price AS optimized_price,
    po.competitor_prices,
    po.discounts,
    po.sales_volume,
    po.customer_reviews,
    po.return_rate,
    po.storage_cost,
    po.elasticity_index
FROM demand_for_casting dfc
LEFT JOIN inventory_monitoring im
  ON dfc.product_id = im.product_id 
  AND dfc.store_id = im.store_id
LEFT JOIN pricing_optimization po
  ON dfc.product_id = po.product_id 
  AND dfc.store_id = po.store_id;

SELECT * FROM inventory_optimization_view LIMIT 20;

CREATE TABLE full_inventory_optimization AS
SELECT * FROM inventory_optimization_view;

SELECT * FROM full_inventory_optimization;

/*--------------------1. Remove Duplicate Rows----------------------------------------------------------------*/
# CHECK DUPLICATES
#############################
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY product_id,store_id,date
) AS row_num
FROM full_inventory_optimization;

WITH duplicate_row AS 
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY product_id,store_id,date
) AS row_num
FROM full_inventory_optimization
)
SELECT * FROM duplicate_row WHERE row_num > 1;

/*--------------------2. Handle NULL or Missing Values----------------------------------------------------------------*/
SELECT 
  COUNT(stock_levels) AS stock_levels_non_null,
  COUNT(*) - COUNT(stock_levels) AS stock_levels_null,
  COUNT(supplier_lead_time) AS supplier_lead_time_non_null,
  COUNT(*) - COUNT(supplier_lead_time) AS supplier_lead_time_null,
  COUNT(stockout_frequency) AS stockout_frequency_non_null,
  COUNT(*) - COUNT(stockout_frequency) AS stockout_frequency_null,
  COUNT(reorder_point) AS reorder_point_non_null,
  COUNT(*) - COUNT(reorder_point) AS reorder_point_null,
  COUNT(expiry_date) AS expiry_date_non_null,
  COUNT(*) - COUNT(expiry_date) AS expiry_date_null,
  COUNT(warehouse_capacity) AS warehouse_capacity_non_null,
  COUNT(*) - COUNT(warehouse_capacity) AS warehouse_capacity_null,
  COUNT(order_fulfillment_time) AS order_fulfillment_time_non_null,
  COUNT(*) - COUNT(order_fulfillment_time) AS order_fulfillment_time_null,
  COUNT(optimized_price) AS optimized_price_non_null,
  COUNT(*) - COUNT(optimized_price) AS optimized_price_null,
  COUNT(competitor_prices) AS competitor_prices_non_null,
  COUNT(*) - COUNT(competitor_prices) AS competitor_prices_null,
  COUNT(discounts) AS discounts_non_null,
  COUNT(*) - COUNT(discounts) AS discounts_null,
  COUNT(sales_volume) AS sales_volume_non_null,
  COUNT(*) - COUNT(sales_volume) AS sales_volume_null,
  COUNT(customer_reviews) AS customer_reviews_non_null,
  COUNT(*) - COUNT(customer_reviews) AS customer_reviews_null,
  COUNT(return_rate) AS return_rate_non_null,
  COUNT(*) - COUNT(return_rate) AS return_rate_null,
  COUNT(storage_cost) AS storage_cost_non_null,
  COUNT(*) - COUNT(storage_cost) AS storage_cost_null,
  COUNT(elasticity_index) AS elasticity_index_non_null,
  COUNT(*) - COUNT(elasticity_index) AS elasticity_index_null
FROM full_inventory_optimization;

/*_____________Replace or fill NULLs as needed:____________*/


UPDATE full_inventory_optimization
SET stock_levels = COALESCE(stock_levels, 0),
    supplier_lead_time = COALESCE(supplier_lead_time, 0),
    stockout_frequency = COALESCE(stockout_frequency, 0),
    reorder_point = COALESCE(reorder_point, 0),
    expiry_date = COALESCE(expiry_date,'1900-01-01'),
    warehouse_capacity = COALESCE(warehouse_capacity, 0),
    order_fulfillment_time = COALESCE(order_fulfillment_time, 0),
    optimized_price = COALESCE(optimized_price, 0),
    competitor_prices = COALESCE(competitor_prices, 0),
    discounts = COALESCE(discounts, 0),
    sales_volume = COALESCE(sales_volume, 0),
    customer_reviews = COALESCE(customer_reviews, 0),
    return_rate = COALESCE(return_rate, 0),
    storage_cost = COALESCE(storage_cost, 0),
    elasticity_index = COALESCE(elasticity_index, 0);
   
SET SQL_SAFE_UPDATES = 0;
SELECT * FROM full_inventory_optimization;



/*_____________end____________*/


/*_____________EXPLORATORY DATA ANALYSIS____________*/

# 1. Total Sales Quantity and Average Price per Product Store

SELECT 
    product_id,
    store_id,
    SUM(sales_quantity) AS total_sales,
    AVG(demand_price) AS avg_demand_price,
    AVG(optimized_price) AS avg_optimized_price
FROM full_inventory_optimization
GROUP BY product_id, store_id
ORDER BY total_sales DESC;

# 2. Products with Stockouts (stock_levels = 0) and Demand

SELECT 
    product_id,
    store_id,
    SUM(CASE WHEN stock_levels = 0 THEN 1 ELSE 0 END) AS stockout_days,
    SUM(sales_quantity) AS total_sales_during_stockout
FROM full_inventory_optimization
GROUP BY product_id, store_id
HAVING stockout_days > 0
ORDER BY stockout_days DESC;

# If you want to see on which dates those stockouts occurred, you could query:
SELECT product_id, store_id, date
FROM full_inventory_optimization
WHERE stock_levels = 0
ORDER BY product_id, store_id, date;


# 3. Average Lead Time and Its Relation to Stockout Frequency

SELECT 
    product_id,
    store_id,
    AVG(supplier_lead_time) AS avg_lead_time,
    AVG(stockout_frequency) AS avg_stockout_freq
FROM full_inventory_optimization
GROUP BY product_id, store_id
ORDER BY avg_stockout_freq DESC;

#4. Correlation Between Discounts and Sales Volume Increase
SELECT 
    product_id,
    store_id,
    AVG(discounts) AS avg_discount,
    AVG(sales_volume) AS avg_sales_volume
FROM full_inventory_optimization
GROUP BY product_id, store_id
HAVING avg_discount > 0
ORDER BY avg_sales_volume DESC;

#5. Top Customer Segments by Sales Volume
SELECT 
    customer_segments,
    SUM(sales_volume) AS total_sales_volume
FROM full_inventory_optimization
GROUP BY customer_segments
ORDER BY total_sales_volume DESC
LIMIT 5;


# 6. Monthly Sales Trend per Product

SELECT 
    product_id,
    DATE_FORMAT(date, '%Y-%m') AS year_month_sales,
    SUM(sales_quantity) AS total_sales
FROM full_inventory_optimization
GROUP BY product_id, year_month_sales
ORDER BY product_id, year_month_sales;

# 7. Average Inventory Turnover Rate by Product
-- Inventory turnover can be approximated by:
-- Turnover Rate=   Total Sales Quantity /Average Stock Level

SELECT 
    product_id,
    store_id,
    SUM(sales_quantity) / NULLIF(AVG(stock_levels), 0) AS inventory_turnover_rate
FROM full_inventory_optimization
GROUP BY product_id, store_id
ORDER BY inventory_turnover_rate DESC;

# 8. Identify Products with High Return Rates but Low Sales Volume
SELECT 
    product_id,
    store_id,
    AVG(return_rate) AS avg_return_rate,
    SUM(sales_volume) AS total_sales_volume
FROM full_inventory_optimization
GROUP BY product_id, store_id
HAVING avg_return_rate > 5 AND total_sales_volume < 100
ORDER BY avg_return_rate DESC;

