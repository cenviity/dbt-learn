with

paid_orders as (

    select * from {{ ref('int_orders') }}

),

final as (

    select
        order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,

        -- Sales transaction sequence
        row_number() over (order by order_id) as transaction_seq,

        -- Customer sales sequence
        row_number() over (
            partition by customer_id
            order by order_id
        ) as customer_sales_seq,

        -- New vs returning customer
        case when (
            rank() over (
                partition by customer_id
                order by order_placed_at, order_id
            ) = 1
        )
            then 'new'
            else 'return'
        end as nvsr,

        -- Customer lifetime value
        sum(total_amount_paid) over (
            partition by customer_id
            order by order_placed_at
        ) as customer_lifetime_value,

        -- First day of sale
        first_value(order_placed_at) over (
            partition by customer_id
            order by order_placed_at
        ) as fdos

        from paid_orders

)

select * from final

order by order_id
