with

payments as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        created as created_date,
        _batched_at,

        -- Convert from cents to dollars
        amount / 100 as amount

    from raw.stripe.payment

)

select * from payments
