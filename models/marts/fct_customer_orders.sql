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

customer_orders as (

    select
        customers.id as customer_id,

        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders

    from customers

    left join orders
        on orders.user_id = customers.id

    group by 1

),

add_customer_lifetime_value as (

    select
        paid_orders.order_id,

        sum(paid_orders.total_amount_paid) over (
            partition by paid_orders.customer_id
            order by order_id
        ) as customer_lifetime_value

    from paid_orders

    order by 1

),

final as (

    select
        paid_orders.*,

        row_number() over (order by paid_orders.order_id) as transaction_seq,

        row_number() over (
            partition by paid_orders.customer_id
            order by paid_orders.order_id
        ) as customer_sales_seq,

        case when customer_orders.first_order_date = paid_orders.order_placed_at
            then 'new'
            else 'return'
        end as nvsr,

        add_customer_lifetime_value.customer_lifetime_value,
        customer_orders.first_order_date as fdos

        from paid_orders

        left join customer_orders
            using (customer_id)

        left join add_customer_lifetime_value
            using (order_id)

        order by order_id

)

select * from final
