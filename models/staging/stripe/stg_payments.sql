with

source as (

    select * from {{ source('stripe', 'payment') }}

),

staged as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        created as created_date,

        -- Convert from cents to dollars
        {{ cents_to_dollars('amount', 4) }} as amount

    from source

)

select * from staged
