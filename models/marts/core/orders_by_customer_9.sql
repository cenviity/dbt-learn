select
    customer_id,
    count(order_id) as num_orders

from {{ ref('stg_orders') }}

group by 1
