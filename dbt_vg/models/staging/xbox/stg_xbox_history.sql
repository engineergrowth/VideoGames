{{ config(materialized='view') }}

with source as (
    select
        playerid,
        gameid,
        timestamp
    from {{ source('vg_raw', 'xbox_history_raw') }}
),

renamed as (
    select
        cast(playerid as integer) as player_id,
        cast(gameid as integer) as game_id,
        cast(timestamp as timestamp) as played_at
    from source
)

select * from renamed
