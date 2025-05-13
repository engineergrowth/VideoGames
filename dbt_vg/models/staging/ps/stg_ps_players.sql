{{ config(materialized='view') }}

with source as (
    select
        playerid
    from {{ source('vg_raw', 'ps_players_raw') }}
),

renamed as (
    select
        cast(playerid as integer) as player_id
    from source
)

select * from renamed
