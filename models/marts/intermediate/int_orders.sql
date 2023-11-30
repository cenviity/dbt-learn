with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

completed_payments as (

    select
        order_id,

        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid

    from payments

    where payment_status != 'fail'

    group by 1

),

paid_orders as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,

        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,

        customers.customer_first_name,
        customers.customer_last_name

    FROM orders

    left join completed_payments
        using (order_id)

    left join customers
        using (customer_id)

)

select * from paid_orders
