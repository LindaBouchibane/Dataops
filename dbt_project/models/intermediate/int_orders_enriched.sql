with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

order_totals as (
    select
        order_id,
        sum(quantity * unit_price)  as total_amount,
        sum(quantity)               as total_items,
        count(*)                    as nb_items
    from order_items
    group by order_id
)

select
    o.order_id,
    o.order_date,
    o.status,
    o.store_id,
    c.customer_id,
    c.country,
    c.segment,
    ot.total_amount,
    ot.total_items,
    ot.nb_items
from orders o
left join customers c on o.customer_id = c.customer_id
left join order_totals ot on o.order_id = ot.order_id
