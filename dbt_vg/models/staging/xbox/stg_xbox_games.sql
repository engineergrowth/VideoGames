{{ config(materialized='view') }}

with source as (
    select
        gameid,
        title,
        developers,
        publishers,
        genres,
        supported_languages,
        release_date
    from {{ source('vg_raw', 'xbox_games_raw') }}
),

renamed as (
    select
        cast(gameid as integer) as game_id,
        title,
        developers,
        publishers,
        genres,
        supported_languages,
        cast(release_date as date) as release_date
    from source
)

select * from renamed
