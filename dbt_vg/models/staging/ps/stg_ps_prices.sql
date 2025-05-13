{{ config(materialized='view') }}

with source as (
    select
        gameid,
        usd,
        eur,
        gbp,
        jpy,
        rub,
        date_acquired
    from {{ source('vg_raw', 'ps_prices_raw') }}
),

renamed as (
    select
        cast(gameid as integer) as game_id,
        cast(usd as float64) as price_usd,
        cast(eur as float64) as price_eur,
        cast(gbp as float64) as price_gbp,
        cast(jpy as float64) as price_jpy,
        cast(rub as float64) as price_rub,
        cast(date_acquired as date) as date_acquired
    from source
)

select * from renamed
