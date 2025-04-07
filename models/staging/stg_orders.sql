with

source as (

    select * from {{ source('ecom', 'raw_orders') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        {{ cents_to_dollars('subtotal') }} as subtotal,
        {{ cents_to_dollars('tax_paid') }} as tax_paid,
        {{ cents_to_dollars('order_total') }} as order_total,

        ---------- timestamps
        {{ dbt.date_trunc('day','ordered_at') }} as ordered_at

    from source

)

select * from renamed
union all 
select 
    null as order_id
    null as location_id
    null as customer_id
    0 as subtotal_cents
    0 as tax_paid_cents
    0 as order_total_cents
    {{ cents_to_dollars('0') }} as subtotal,
    {{ cents_to_dollars('0') }} as tax_paid,
    {{ cents_to_dollars('0') }} as order_total,
    {{ dbt.date_trunc('day','current_date()') }} as ordered_at
