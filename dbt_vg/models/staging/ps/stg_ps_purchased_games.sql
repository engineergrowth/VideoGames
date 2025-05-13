{{ config(materialized='view') }}

with source as (
    select
        playerid,
        library
    from {{ source('vg_raw', 'ps_purchased_games_raw') }}
),

renamed as (
    select
        cast(playerid as integer) as player_id,
        library
    from source
)

select * from renamed
