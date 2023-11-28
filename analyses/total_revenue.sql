with

payments as (

    select * from {{ ref('stg_payments') }}

),

successful_payments as (

    select *

    from payments

    where status = 'success'

),

total_revenue as (

    select
        sum(amount)
    
    from successful_payments

)

select * from total_revenue
