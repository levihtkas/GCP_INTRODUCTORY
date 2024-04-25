# Creating and managing SQL pipelines
## Scenario 
In recent years, TheLook eCommerce's profits have soared thanks to online shopping. But delivery times have not kept up, and customer satisfaction has decreased.

As a cloud data analyst for TheLook eCommerce, you have been asked to collaborate with Kai, the head of the logistics team, to develop a data pipeline to collect, clean, transform, and load data about customer deliveries, including the distance traveled from the distribution center to each customer.

This information will help the logistics team determine the ways they can improve delivery times and increase customer satisfaction, such as whether to open new distribution centers, relocate existing distribution centers, or invest in new transportation methods.

You'll apply your BigQuery and SQL skills to design a flexible pipeline that provides the logistics team with reliable data to better monitor delivery performance, and can be easily updated as the logistic team’s data needs change.

First, you'll create a dataset and define table schemas for data that will be ingested. Next, you'll perform and explore a series of transformations. Then, you'll apply the transformations to the data before loading the transformed data into newly defined tables. Finally, you'll formalize those queries into a stored procedure.

### Create a dataset 
Task 1. Create a dataset
Navigate to big Query Studio '+' sign and create a dataset as prescibed and also use 
'''
--Create empty product_orders_fulfillment table
CREATE OR REPLACE TABLE
 `thelook_ecommerce.product_orders_fulfillment`
 ( order_id INT64,
 user_id INT64,
 status STRING,
 product_id INT64,
 created_at TIMESTAMP,
 returned_at TIMESTAMP,
 shipped_at TIMESTAMP,
 delivered_at TIMESTAMP,
 cost NUMERIC,
 sale_price NUMERIC,
 retail_price NUMERIC,
 category STRING,
 name STRING,
 brand STRING,
 department STRING,
 sku STRING,
 distribution_center_id INT64);
 '''
 to create the table.
 ### Task 2. Create a table from query results
--Create empty customers table
CREATE OR REPLACE TABLE
 `thelook_ecommerce.customers`
 ( id INT64,
 first_name STRING,
 last_name STRING,
 email STRING,
 age INT64,
 gender STRING,
 state STRING,
 street_address STRING,
 postal_code STRING,
 city STRING,
 country STRING,
 traffic_source STRING,
 created_at TIMESTAMP,
 latitude FLOAT64,
 longitude FLOAT64,
 point_location GEOGRAPHY);
 --Create empty centers table
 CREATE OR REPLACE TABLE
 `thelook_ecommerce.centers`
 ( id INT64,
 name STRING,
 latitude FLOAT64,
 longitude FLOAT64,
 point_location GEOGRAPHY);
### Task 3. Perform a transformation on BigQuery data
'''
--load the centers table from public dataset and include geography transformation
CREATE OR REPLACE TABLE
`thelook_ecommerce.centers` AS
SELECT
id,
name,
latitude,
longitude,
ST_GEOGPOINT(dcenters.longitude, dcenters.latitude) AS point_location
FROM
`bigquery-public-data.thelook_ecommerce.distribution_centers` AS dcenters;
'''
'''
--load the customers table from public dataset and include geography transformation
CREATE OR REPLACE TABLE
`thelook_ecommerce.customers` AS
SELECT
id,
first_name,
last_name,
email,
age,
gender,
state,
street_address,
postal_code,
city,
country,
traffic_source,
created_at,
latitude,
longitude,
ST_GEOGPOINT(users.longitude, users.latitude) AS point_location
FROM
`bigquery-public-data.thelook_ecommerce.users` AS users;

'''

'''
SELECT
customers.id as customer_id,
(
SELECT
MIN(ST_DISTANCE(centers.point_location, customers.point_location))/1000,
FROM
`thelook_ecommerce.centers` AS centers) AS distance_to_closest_center
FROM
`thelook_ecommerce.customers` AS customers ;
'''
