-- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
--   Aggregate this dataset along the following dimensions
--     - player and team
--       - Answer questions like who scored the most points playing for one team? Giannis Antetokounmpo for the MIL
--     - player and season
--       - Answer questions like who scored the most points in one season? James Harden for season 2018
--     - team
--       - Answer questions like which team has won the most games? GSW

WITH raw_games AS (
    SELECT
        COALESCE(player_name, 'NA') AS player_name,
        COALESCE(team_abbreviation, 'NA') AS team_name,
        COALESCE(season::varchar(9), 'NA') AS season,
        COALESCE(d.pts, 0) AS pts,
        CASE 
            WHEN g.team_id_home = d.team_id AND g.home_team_wins = 1 
                THEN 1
            WHEN g.team_id_away = d.team_id AND g.home_team_wins = 0
                THEN 1
            ELSE 0 
        END AS is_win
    FROm games g
        LEFT JOIN game_details d 
            USING(game_id)
), aggregated AS (
    SELECT
        COALESCE(player_name, '(overall)') AS player_name,
        COALESCE(team_name, '(overall)') AS team_name,
        COALESCE(season :: varchar(9), '(overall)') AS season,
        COUNT(1) AS number_of_games,
        SUM(pts) AS total_points_scored,
        SUM(is_win) AS total_wins
    FROM
        raw_games
    GROUP BY
        GROUPING SETS (
            (player_name, team_name),
            (player_name, season),
            (team_name)
        )
)
-- This query will return the player who scored the most points for a team, excluding overall aggregates.
-- SELECT * 
-- FROM aggregated 
-- WHERE player_name != '(overall)' 
--     AND team_name != '(overall)' 
-- ORDER BY total_points_scored DESC
-- LIMIT 1;

-- This query will return the player who scored the most points for a season, excluding overall aggregates.
-- SELECT * FROM aggregated
-- WHERE player_name != '(overall)' 
--     AND season != '(overall)'
-- ORDER BY
--     total_points_scored DESC
-- LIMIT 1;

-- This query will return the team who won the most games, excluding overall aggregates.
SELECT * FROM aggregated
WHERE team_name != '(overall)' 
ORDER BY
    total_wins DESC
LIMIT 1;
