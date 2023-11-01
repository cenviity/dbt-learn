with

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select
        payment_id,
        order_id,
        status,
        amount

    from {{ ref('stg_payments') }}

    where status = 'success'

),

order_payment_amounts as (

    select
        order_id,
        sum(amount) as amount

    from payments

    group by 1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        order_payment_amounts.amount

    from orders

    left join order_payment_amounts
        on orders.order_id = order_payment_amounts.order_id

)

select * from final
