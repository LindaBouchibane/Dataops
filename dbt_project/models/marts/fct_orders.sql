with enriched as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    order_id,
    order_date,
    status,
    store_id,
    customer_id,
    country,
    segment,
    total_amount,
    total_items,
    nb_items
from enriched
