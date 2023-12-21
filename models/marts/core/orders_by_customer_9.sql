-- This overrides the config in `dbt_project.yml`
-- and this model will not require tests.
{{
  config(
    required_tests = None,
    )
}}

select
    customer_id,
    count(order_id) as num_orders

from {{ ref('stg_orders') }}

group by 1
