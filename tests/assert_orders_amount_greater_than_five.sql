select amount

from {{ ref('fct_orders') }}

where amount <= 5
