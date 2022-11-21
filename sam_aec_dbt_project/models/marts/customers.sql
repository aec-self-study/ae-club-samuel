with customers as (

  select 
    customer_id, 
    customer_name, 
    customer_email 
  from {{ ref('stg_customers') }}

), orders as (
  
  select 
    customer_id, 
    min(created_at) as first_order_at, 
    count(*) as orders 
  from {{ ref('stg_orders') }}
  group by 1
  
)

select
  customers.customer_id,
  customers.customer_name,
  customers.customer_email,
  orders.first_order_at,
  orders.orders
from customers
left join orders
  on customers.customer_id = orders.customer_id
order by first_order_at
