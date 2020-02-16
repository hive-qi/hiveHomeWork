use bd20;

drop table if exists ods_department;
create external table ods_department(
	department_id int,
	department_name string
)
row format delimited
fields terminated by '|'
location '/orderdata/departments';


drop table if exists categories;
create external table categories(
	category_id int,
	category_department_id int,
	category_name string
)
row format delimited
fields terminated by '|'
location '/orderdata/categories';


drop table if exists products;
create external table products(
	product_id int,
	product_category_id int,
	product_name string,
	product_description string,
	product_price float,
	product_image string
)
row format delimited
fields terminated by '|'
location '/orderdata/products';


create external table if not exists order_items(
	order_item_id int,
	order_item_order_id int,
	order_item_product_id int,
	order_item_quantity int,
	order_item_subtotal float,
	order_item_product_price float
)
partitioned by(p_date string)
row format delimited
fields terminated by '|'
location '/orderdata/order_items';

alter table order_items drop if exists partition(p_date='${datetime}');
alter table order_items add partition(p_date='${datetime}') location '/orderdata/order_items/${datetime}'; 



create external table if not exists orders(
	order_id int,
	order_date string,
	order_customer_id int,
	order_status string
)
partitioned by(p_date string)
row format delimited
fields terminated by '|'
location '/orderdata/orders';

alter table orders drop if exists partition(p_date='${datetime}');
alter table orders add partition(p_date='${datetime}') location '/orderdata/orders/${datetime}'; 


drop table if exists customers;
create external table customers(
	customer_id int,
	customer_fname string,
	customer_lname string,
	customer_email string,
	customer_password string,
	customer_street string,
	customer_city string,
	customer_state string,
	customer_zipcode string
)
row format delimited
fields terminated by '|'
location '/orderdata/customers';

--  2.做数据统计分析
--  按天统计，每天的销售额最高的店铺top5
--  该统计分析脚本也要接受参数：yyyyMMdd样式的日期
--  生成数据保存在一张表中（不用分区）

drop table if exists department_rank${datetime};
create table department_rank${datetime} as
select
department_name,sum(order_item_subtotal) total_sale
from ods_department
inner join categories on department_id=category_department_id
inner join products on category_id=product_category_id
inner join order_items on product_id=order_item_product_id
inner join orders on order_item_order_id=order_id
where order_items.p_date='${datetime}' and
order_status='COMPLETE'
group by department_name
order by total_sale desc
limit 5;


-- 3.根据state、city、street维度生成数据立方体，统计每个维度下的消费订单数量，和总金额，该统计日期也是需要考虑的因素
-- 该统计分析脚本也要接受参数：yyyyMMdd样式的日期
-- 每天生成的新数据单独放一张表中

drop table if exists cube_table${datetime};
create table cube_table${datetime} as
select
customer_state,customer_city,customer_street,
count(distinct order_id) order_count,sum(order_item_subtotal) total_sale
from customers
inner join orders on order_customer_id=customer_id
inner join order_items on order_item_order_id=order_id
where orders.p_date='${datetime}'
and order_status='COMPLETE'
group by customer_state,customer_city,customer_street 
grouping sets((customer_state),(customer_city),(customer_street));


-- 4.统计每天的热销商品（根据销售数量）top10

drop table if exists hot_product${datetime};
create table hot_product${datetime} as
select
product_name,sum(order_item_quantity) product_count
from products
inner join order_items on
product_id=order_item_product_id 
inner join orders on order_item_order_id=order_id
where order_items.p_date='${datetime}'
and order_status='COMPLETE'
group by product_name
order by product_count desc 
limit 10;



