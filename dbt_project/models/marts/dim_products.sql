with products as (
    select * from {{ ref('stg_products') }}
),

sales as (
    select
        product_id,
        sum(quantity)               as total_quantity_sold,
        sum(quantity * unit_price)  as total_revenue
    from {{ ref('stg_order_items') }}
    group by product_id
)

select
    p.product_id,
    p.name,
    p.category,
    p.cost,
    coalesce(s.total_quantity_sold, 0)  as total_quantity_sold,
    coalesce(s.total_revenue, 0)        as total_revenue
from products p
left join sales s on p.product_id = s.product_id
