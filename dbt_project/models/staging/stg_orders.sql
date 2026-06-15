with source as (
    select * from {{ source('staging', 'orders') }}
)

select
    order_id::integer       as order_id,
    customer_id::integer    as customer_id,
    store_id::integer       as store_id,
    order_date::date        as order_date,
    status
from source
