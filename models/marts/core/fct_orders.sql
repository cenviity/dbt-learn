with

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

successful_payments as (

    select
        payment_id,
        order_id,
        status,
        amount

    from payments

    where status = 'success'

),

successful_payment_amounts as (

    select
        order_id,
        sum(amount) as amount

    from successful_payments

    group by 1

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        successful_payment_amounts.amount

    from orders

    left join successful_payment_amounts
        on orders.order_id = successful_payment_amounts.order_id

)

select * from final
