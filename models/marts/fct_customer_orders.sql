with

orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

base_payments as (

    select * from {{ ref('stg_stripe__payments') }}

    where payment_status != 'fail'

),

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

payments as (

    select
        order_id,

        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid

    from base_payments

    group by 1

),

paid_orders as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,

        payments.total_amount_paid,
        payments.payment_finalized_date,

        customers.customer_first_name,
        customers.customer_last_name

    FROM orders

    left join payments
        ON orders.order_id = payments.order_id

    left join customers
        on orders.customer_id = customers.customer_id

),

final as (

    select
        paid_orders.*,

        -- Sales transaction sequence
        row_number() over (order by paid_orders.order_id) as transaction_seq,

        -- Customer sales sequence
        row_number() over (
            partition by paid_orders.customer_id
            order by paid_orders.order_id
        ) as customer_sales_seq,

        -- New vs returning customer
        case when (
            rank() over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) = 1
        )
            then 'new'
            else 'return'
        end as nvsr,

        -- Customer lifetime value
        sum(paid_orders.total_amount_paid) over (
            partition by paid_orders.customer_id
            order by order_id
        ) as customer_lifetime_value,

        -- First day of sale
        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as fdos

        from paid_orders

)

select * from final

order by order_id
