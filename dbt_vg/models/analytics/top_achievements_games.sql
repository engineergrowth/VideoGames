WITH all_achievements AS (
  SELECT 
    'PlayStation' AS platform,
    a.gameid AS game_id,
    COUNT(*) AS total_achievements
  FROM {{ source('vg_raw', 'ps_achievements_raw') }} a
  GROUP BY a.gameid

  UNION ALL

  SELECT 
    'Xbox' AS platform,
    a.gameid AS game_id,
    COUNT(*) AS total_achievements
  FROM {{ source('vg_raw', 'xbox_achievements_raw') }} a
  GROUP BY a.gameid
),

all_games AS (
  SELECT 
    'PlayStation' AS platform,
    gameid,
    title,
    genres
  FROM {{ source('vg_raw', 'ps_games_raw') }}

  UNION ALL

  SELECT 
    'Xbox' AS platform,
    gameid,
    title,
    genres
  FROM {{ source('vg_raw', 'xbox_games_raw') }}
),

achievements_with_details AS (
  SELECT 
    g.platform,
    g.title,
    a.total_achievements,
    g.genres
  FROM all_achievements a
  JOIN all_games g ON a.game_id = g.gameid
  WHERE g.title IS NOT NULL
)

SELECT 
  platform,
  title,
  MAX(total_achievements) AS total_achievements,
  genres
FROM achievements_with_details
WHERE genres IS NOT NULL
GROUP BY platform, title, genres
ORDER BY total_achievements DESC
LIMIT 10
