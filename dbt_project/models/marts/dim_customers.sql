with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        count(*)            as total_orders,
        min(order_date)     as first_order_date,
        max(order_date)     as last_order_date
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.country,
    c.segment,
    c.signup_date,
    coalesce(o.total_orders, 0)     as total_orders,
    o.first_order_date,
    o.last_order_date
from customers c
left join orders o on c.customer_id = o.customer_id
