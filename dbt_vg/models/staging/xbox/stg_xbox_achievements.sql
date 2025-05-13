{{ config(materialized='view') }}

with source as (
    select
        achievementid,
        gameid,
        title,
        description,
        points
    from {{ source('vg_raw', 'xbox_achievements_raw') }}
),

renamed as (
    select
        cast(achievementid as integer) as achievement_id,
        cast(gameid as integer) as game_id,
        title,
        description,
        cast(points as integer) as points
    from source
)

select * from renamed
