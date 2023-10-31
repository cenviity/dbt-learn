with

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

successful_payments as (

    select
        order_id,
        sum(amount) as amount

    from payments

    where status = 'success'

    group by
        1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        successful_payments.amount

    from orders

    left join successful_payments
        on orders.order_id = successful_payments.order_id

)

select * from final
