with

-- Import CTEs

base_orders as (

  select * from {{ source('jaffle_shop', 'orders') }}

),

base_customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

),

payments as (

  select * from {{ source('stripe', 'payment') }}

),

-- Logical CTEs

-- Staging

customers as (

  select
    id as customer_id,
    last_name as surname,
    first_name as givenname,
    first_name || ' ' || last_name as full_name

  from base_customers

),

orders as (

  select
    id as order_id,
    user_id as customer_id,
    order_date,
    status as order_status,
    row_number() over (
      partition by user_id
      order by order_date, id
    ) as user_order_seq

  from base_orders

),

-- Marts

customer_order_history as (

  select
    customers.customer_id,
    customers.full_name,
    customers.surname,
    customers.givenname,
    min(order_date) as first_order_date,

    min(case
      when orders.order_status not in ('returned','return_pending')
      then order_date
    end) as first_non_returned_order_date,

    max(case
      when orders.order_status not in ('returned','return_pending')
      then order_date
    end) as most_recent_non_returned_order_date,

    coalesce(max(user_order_seq),0) as order_count,

    coalesce(count(case
      when orders.order_status != 'returned'
      then 1
    end),0) as non_returned_order_count,

    sum(case
      when orders.order_status not in ('returned','return_pending')
      then ROUND(c.amount/100.0,2)
      else 0
    end) as total_lifetime_value,

    sum(case
      when orders.order_status not in ('returned','return_pending')
      then ROUND(c.amount/100.0,2)
      else 0
    end)
    / nullif(count(case
      when orders.order_status not in ('returned','return_pending')
      then 1
    end),0) as avg_non_returned_order_value,

    array_agg(distinct orders.order_id) as order_ids

    from orders

  join customers
  on orders.customer_id = customers.customer_id

  left outer join payments c
  on orders.order_id = c.orderid

  where orders.order_status not in ('pending') and c.status != 'fail'

  group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

),

-- Final CTE

final as (

  select
    orders.order_id,
    orders.customer_id,
    customers.surname,
    customers.givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    round(amount/100.0,2) as order_value_dollars,
    orders.order_status,
    payments.status as payment_status

  from orders

  join customers
  on orders.customer_id = customers.customer_id

  join customer_order_history
  on orders.customer_id = customer_order_history.customer_id

  left outer join payments
  on orders.order_id = payments.orderid

  where payments.status != 'fail'

)

-- Simple `select` statement

select * from final
