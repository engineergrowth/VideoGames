WITH all_games AS (
    SELECT
        gameid,
        genres,
        'PlayStation' AS platform
    FROM {{ source('vg_raw', 'ps_games_raw') }}
    WHERE genres IS NOT NULL

    UNION ALL

    SELECT
        gameid,
        genres,
        'Xbox' AS platform
    FROM {{ source('vg_raw', 'xbox_games_raw') }}
    WHERE genres IS NOT NULL
),

cleaned_genres AS (
    SELECT
        gameid,
        platform,
        REGEXP_REPLACE(
            REGEXP_REPLACE(genres, r'[\[\]\'"]', ''),
            r'\s*,\s*', '|'
        ) AS genres_str
    FROM all_games
),

exploded_genres AS (
    SELECT
        platform,
        gameid,
        TRIM(genre) AS genre
    FROM cleaned_genres,
    UNNEST(SPLIT(genres_str, '|')) AS genre
    WHERE genre IS NOT NULL AND genre != ''
),

genre_counts AS (
    SELECT
        platform,
        genre,
        COUNT(DISTINCT gameid) AS game_count
    FROM exploded_genres
    GROUP BY platform, genre
),

ranked_genres AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY platform ORDER BY game_count DESC) AS rank
    FROM genre_counts
)

SELECT platform, genre, game_count
FROM ranked_genres
WHERE rank <= 10
ORDER BY platform, game_count DESC
