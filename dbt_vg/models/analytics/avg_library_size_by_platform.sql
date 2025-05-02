WITH ps_exploded AS (
    SELECT
        playerid,
        CAST(game_id AS INT64) AS game_id
    FROM (
        SELECT
            playerid,
            SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(library, r'^\[|\]$', ''), r'\s+', ''), ',') AS game_ids
        FROM {{ source('vg_raw', 'ps_purchased_games_raw') }}
    ), UNNEST(game_ids) AS game_id
),

xbox_exploded AS (
    SELECT
        playerid,
        CAST(game_id AS INT64) AS game_id
    FROM (
        SELECT
            playerid,
            SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(library, r'^\[|\]$', ''), r'\s+', ''), ',') AS game_ids
        FROM {{ source('vg_raw', 'xbox_purchased_games_raw') }}
    ), UNNEST(game_ids) AS game_id
),

exploded_library AS (
    SELECT * FROM ps_exploded
    UNION ALL
    SELECT * FROM xbox_exploded
),

library_with_platform AS (
    SELECT
        e.playerid,
        e.game_id,
        CASE
            WHEN x.gameid IS NOT NULL THEN 'Xbox'
            WHEN p.gameid IS NOT NULL THEN 'PlayStation'
            ELSE 'Unknown'
        END AS platform
    FROM
        exploded_library e
        LEFT JOIN {{ source('vg_raw', 'xbox_games_raw') }} x ON e.game_id = x.gameid
        LEFT JOIN {{ source('vg_raw', 'ps_games_raw') }} p ON e.game_id = p.gameid
),

filtered_library AS (
    SELECT *
    FROM library_with_platform
    WHERE platform IN ('Xbox', 'PlayStation')
),

library_counts AS (
    SELECT
        playerid,
        platform,
        COUNT(DISTINCT game_id) AS total_games
    FROM
        filtered_library
    GROUP BY
        playerid, platform
)

SELECT
    platform,
    ROUND(AVG(total_games), 2) AS avg_library_size
FROM
    library_counts
GROUP BY
    platform
ORDER BY
    avg_library_size DESC
