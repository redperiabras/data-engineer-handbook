-- - A query that uses window functions on `game_details` to find out the following things: 
--     - What is the most games a team has won in a 90 game stretch ? 77

WITH raw_games AS (
    SELECT DISTINCT
        g.game_id,
        season,
        COALESCE(team_abbreviation, 'NA') AS team_name,
        CASE
            WHEN g.team_id_home = d.team_id AND g.home_team_wins = 1 
                THEN 1
            WHEN g.team_id_away = d.team_id AND g.home_team_wins = 0 
                THEN 1
            ELSE 0
        END AS is_win
    FROM games g
        LEFT JOIN game_details d 
            USING(game_id)
), winning_streaks AS (
    SELECT
        team_name,
        season,
        SUM(is_win) OVER (
            PARTITION BY team_name
            ORDER BY season, game_id
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games,
        COUNT(1) OVER (
            PARTITION BY team_name
            ORDER BY season, game_id
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS games_count
    FROM
        raw_games
)
SELECT 
    team_name,
    wins_in_90_games,
    games_count
FROM winning_streaks
WHERE games_count >= 90
ORDER BY wins_in_90_games DESC
LIMIT 1;

--     - How many games in a row did LeBron James score over 10 points a game ? 68
WITH raw_games AS (
    SELECT DISTINCT
        g.game_id,
        season,
        COALESCE(d.pts, 0) AS pts,
        CASE WHEN COALESCE(d.pts, 0) > 10 THEN 1 ELSE 0 END AS over_10_pts
    FROM
        games g
        LEFT JOIN game_details d USING(game_id)
    WHERE player_name = 'LeBron James'
), scoring_streaks AS (
    SELECT
        game_id,
        season,
        pts,
        over_10_pts,
        SUM(CASE WHEN over_10_pts = 0 THEN 1 ELSE 0 END) OVER(ORDER BY season, game_id) AS streak_group
    FROM raw_games
)
SELECT
    COUNT(1) AS streak_length
FROM scoring_streaks
WHERE over_10_pts = 1
GROUP BY streak_group
ORDER BY streak_length DESC
LIMIT 1;
