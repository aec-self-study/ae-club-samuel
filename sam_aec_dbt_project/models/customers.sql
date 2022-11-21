with customers as (

  select 
    id as customer_id, 
    name as customer_name, 
    email as customer_email 
  from `analytics-engineers-club.coffee_shop.customers`

), orders as (
  
  select 
    customer_id, 
    min(created_at) as first_order_date, 
    count(*) as orders 
  from `analytics-engineers-club.coffee_shop.orders` 
  group by 1
  
)

select
  customers.customer_id,
  customers.customer_name,
  customers.customer_email,
  orders.first_order_at,
  orders.count_orders
from customers
left join orders
  on customers.customer_id = orders.customer_id
order by first_order_date
