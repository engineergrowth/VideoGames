WITH exploded_ps AS (
    SELECT
        CAST(game_id AS INT64) AS game_id
    FROM (
        SELECT
            SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(library, r'^\[|\]$', ''), r'\s+', ''), ',') AS game_ids
        FROM {{ source('vg_raw', 'ps_purchased_games_raw') }}
    ), UNNEST(game_ids) AS game_id
),

exploded_xbox AS (
    SELECT
        CAST(game_id AS INT64) AS game_id
    FROM (
        SELECT
            SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(library, r'^\[|\]$', ''), r'\s+', ''), ',') AS game_ids
        FROM {{ source('vg_raw', 'xbox_purchased_games_raw') }}
    ), UNNEST(game_ids) AS game_id
),

combined_purchases AS (
    SELECT game_id FROM exploded_ps
    UNION ALL
    SELECT game_id FROM exploded_xbox
),

game_titles AS (
    SELECT
        gameid,
        title
    FROM {{ source('vg_raw', 'ps_games_raw') }}
    UNION DISTINCT
    SELECT
        gameid,
        title
    FROM {{ source('vg_raw', 'xbox_games_raw') }}
),

purchase_counts AS (
    SELECT
        c.game_id,
        g.title,
        COUNT(*) AS purchase_count
    FROM combined_purchases c
    LEFT JOIN game_titles g ON c.game_id = g.gameid
    WHERE g.title IS NOT NULL
    GROUP BY g.title, c.game_id
),

deduped_titles AS (
    SELECT
        title,
        SUM(purchase_count) AS total_purchases
    FROM purchase_counts
    GROUP BY title
)

SELECT
    title,
    total_purchases
FROM deduped_titles
ORDER BY total_purchases DESC
LIMIT 25
