with

payments as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        amount,
        created as created_date,
        _batched_at

    from raw.stripe.payment

)

select * from payments
