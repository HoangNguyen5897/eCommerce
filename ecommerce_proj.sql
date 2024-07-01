-- Create 4 necessary tables for the datasets. 
CREATE TABLE orders
(
order_id numeric primary key,
user_id numeric,
status varchar,
gender varchar,
created_at  timestamp,
returned_at timestamp,
shipped_at timestamp,
delivered_at timestamp,
num_of_item numeric
);

CREATE TABLE users
(
id numeric primary key,
first_name varchar,
last_name varchar,
email varchar,
age numeric, 
gender varchar,
state varchar,
street_address varchar,
postal_code varchar,
city varchar,
country varchar,
latitude numeric,
longitude numeric,
traffic_source varchar,
created_at timestamp
);

CREATE TABLE products
(
id numeric primary key,
cost numeric,
category varchar,
name varchar,
brand varchar,
retail_price numeric,
department varchar,
sku varchar,
distribution_center_id numeric
);


CREATE TABLE order_item
(
id numeric primary key,
order_id numeric,
user_id numeric,
product_id numeric ,
inventory_item_id numeric,
status varchar,
created_at  timestamp,
shipped_at timestamp,
delivered_at timestamp,
returned_at timestamp,
sale_price numeric
);


-- CLEANING THE DATA
-- Check for NULL values
select * from order_item
where id is NULL;

select * from orders
where order_id is NULL;

select * from products
where id IS NULL;

select * from users
where id IS NULL;

-- Check for Duplicate values
SELECT * FROM (
select  *,
        ROW_NUMBER() OVER( PARTITION BY order_id, user_id, product_id, inventory_item_id ) as row_num
from order_item
) as t1
WHERE row_num>1;


SELECT * FROM (
select  *,
        ROW_NUMBER() OVER( PARTITION BY order_id, user_id) as row_num
from orders
) as t1
WHERE row_num>1;


SELECT * FROM (
select  *,
        ROW_NUMBER() OVER( PARTITION BY id, cost, category, name) as row_num
from products
) as t1
WHERE row_num>1;


SELECT * FROM (
select  *,
        ROW_NUMBER() OVER( PARTITION BY id) as row_num
from users
) as t1
WHERE row_num>1;


-- ANALYZING THE DATA

-- Number of orders and customers each month in 2023
with num_ord_user as (
	
	select to_char(created_at, 'yyyy-mm') as month_year,
	        count(distinct user_id) as total_user,
	        count(order_id) as total_order    
	from orders 
	where extract(year from created_at) = 2023 
	and status ilike 'Complete'
	group by to_char(created_at, 'yyyy-mm')
	order by month_year
),

pre_ord_user as(
	select	month_year,
		total_user,
		lag(total_user) over(order by month_year) as pre_customer,
		total_order,
		lag(total_order) over(order by month_year) as pre_order
	from num_ord_user
)

select	month_year,
		total_user,
		COALESCE(
			ROUND(100.00*(total_user - pre_customer) / pre_order,2), 
				'0.00') as customer_growth,
		total_order,
		COALESCE(
			ROUND(100.00*(total_order - pre_order) / pre_order,2),
				'0.00') as order_growth
from pre_ord_user;


-- Average sale price and number of distinct users per month in 2023
with avg_ord as (
	
	select to_char(o.created_at, 'yyyy-mm') as month_year,
	        count(distinct o.user_id) as total_user,
	        round(avg(oi.sale_price), 2) as avg_order_value
	from order_item oi
	join orders o
	on oi.order_id = o.order_id
	where extract(year from o.created_at) = 2023 
	group by to_char(o.created_at, 'yyyy-mm')
	order by month_year
),

pre_avg_ord as(
	
	select	month_year,
			total_user,
			lag(total_user) over(order by month_year) as pre_customer,
			avg_order_value,
			lag(avg_order_value) over(order by month_year) as pre_order
	from avg_ord
)

select month_year,
	total_user,
	COALESCE(
		ROUND(100.00*(total_user - pre_customer) / pre_customer,2),
			'0.00') as customer_growth,
	avg_order_value,
	COALESCE(
		ROUND(100.00*(avg_order_value - pre_order) / pre_order,2),
			'0.00') as value_growth
from pre_avg_ord;


-- Youngest and Oldest customers by Genders in 2023
with youngest as (
	select min(age) as age
	from users	
),

oldest as (
	select max(age) as age
	from users  
),

sum_age as(

	select first_name, last_name, gender, age
	from users
	where age = (select age from youngest)
	and extract(year from created_at) = 2023

	UNION all

	select first_name, last_name, gender, age
	from users
	where age = (select age from oldest)
	and extract(year from created_at) = 2023
),

age_category as(
	select gender, age,
		case
		when age = (select age from youngest) and gender = 'F' then 'youngest female'
		when age = (select age from youngest) and gender = 'M' then 'youngest male'
		when age = (select age from oldest) and gender = 'F' then 'oldest female'
		when age = (select age from oldest) and gender = 'M' then 'oldest male'
		end as tag,
    	count(*)
	from sum_age
	group by gender, age, tag
)
 
select * 
from age_category;


-- Top 5 products each month
with sum_table as(

	select to_char(o.created_at, 'yyyy-mm') as month_year,
          o.product_id as product_id,
          round(sum(p.retail_price)-sum(p.cost), 2) as profit
	from order_item o
  
	join products p
	on p.id = o.product_id
	group by o.product_id, to_char(o.created_at, 'yyyy-mm')
	order by month_year, sum(o.sale_price) desc
),

rank_table as (

	select *,
          dense_rank() over(partition by month_year order by profit desc) as rank
	from sum_table
)

select rt.month_year as month_year, 
        rt.product_id as product_id, 
        p.name as product_name,
        round(p.retail_price, 2) as sales,
        round(p.cost, 2) as cost,
        rt.profit as profit,
        rt.rank as rank
from rank_table rt
join products p
on rt.product_id = p.id
where rank <=5
order by month_year;


-- Total sales by day of each category in the last 3 months till today (15/04/2023)
with sum_price as (

  select to_char(o.created_at, 'yyyy-mm') as date_order,
          round(sum(o.sale_price), 2) as sum_sales,
          p.category
  from order_item o
  join products p
  on o.product_id = p.id
  group by p.category, to_char(o.created_at, 'yyyy-mm')
  order by date_order
)

select *
from sum_price
where to_date(date_order, 'YYYY-MM-DD') BETWEEN (DATE '2023-04-15' - INTERVAL '3 MONTH') AND DATE '2023-04-15'


-- COHORT ANALYSIS
-- Step 1: Find the first purchase date and monthly difference from the first purchase date
with a as
	(Select user_id, amount, to_char(first_purchase_date, 'yyyy-mm') as cohort_month,
	created_at,
	(Extract(year from created_at) - extract(year from first_purchase_date))*12 
	  + Extract(MONTH from created_at) - extract(MONTH from first_purchase_date) +1
	  as index
from 
(
	Select user_id, 
	round(sale_price,2) as amount,
	Min(created_at) OVER (PARTITION BY user_id) as first_purchase_date,
	created_at
	from order_item
	) as b),
	cohort_data as
(
	Select cohort_month, 
	index,
	COUNT(DISTINCT user_id) as user_count,
	round(SUM(amount),2) as revenue
	from a
	Group by cohort_month, index
	ORDER BY INDEX
),
	
--CUSTOMER COHORT with pivot table case-when
Customer_cohort as
(
	Select 
	cohort_month,
	Sum(case when index=1 then user_count else 0 end) as m1,
	Sum(case when index=2 then user_count else 0 end) as m2,
	Sum(case when index=3 then user_count else 0 end) as m3,
	Sum(case when index=4 then user_count else 0 end) as m4
	from cohort_data
	Group by cohort_month
	Order by cohort_month
),
	
--RETENTION COHORT
retention_cohort as
(
	Select cohort_month,
	round(100.00* m1/m1,2) || '%' as m1,
	round(100.00* m2/m1,2) || '%' as m2,
	round(100.00* m3/m1,2) || '%' as m3,
	round(100.00* m4/m1,2) || '%' as m4
	from customer_cohort
),
	
--CHURN COHORT
churn_cohort as
(
	Select cohort_month,
	(100.00 - round(100.00* m1/m1,2)) || '%' as m1,
	(100.00 - round(100.00* m2/m1,2)) || '%' as m2,
	(100.00 - round(100.00* m3/m1,2)) || '%' as m3,
	(100.00 - round(100.00* m4/m1,2))|| '%' as m4
	from customer_cohort
)

select * from retention_cohort
	












