select
    customer_id,
    count(*) as count

from {{ ref('dim_customers') }}

group by 1

having count > 1
