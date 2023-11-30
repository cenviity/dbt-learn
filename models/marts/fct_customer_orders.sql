with

orders as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

base_payments as (

    select * from {{ source('stripe', 'payment') }}

    where status != 'fail'

),

customers as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

payments as (

    select
        orderid as order_id,

        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid

    from base_payments

    group by 1

),

paid_orders as (

    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,

        payments.total_amount_paid,
        payments.payment_finalized_date,

        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name

    FROM orders

    left join payments
        ON orders.id = payments.order_id

    left join customers
        on orders.user_id = customers.id

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

        order by order_id

)

select * from final
