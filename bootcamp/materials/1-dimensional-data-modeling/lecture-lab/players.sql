CREATE TYPE season_stats AS (
    season Integer,
    pts REAL,
    ast REAL,
    reb REAL,
    weight INTEGER
);

CREATE TYPE scoring_class AS
    ENUM ('bad', 'average', 'good', 'star');


DROP TABLE players;

CREATE TABLE players (
    player_name TEXT,
    height TEXT,
    college TEXT,
    country TEXT,
    draft_year TEXT,
    draft_round TEXT,
    draft_number TEXT,
    seasons season_stats[],
    scoring_class scoring_class,
    -- years_since_last_active INTEGER,
    is_active BOOLEAN,
    current_season INTEGER,
    PRIMARY KEY (player_name, current_season)
);


SELECT * FROM player_seasons;

INSERT INTO players
    WITH yesterday AS (
        SELECT * FROM players
        WHERE current_season = 1995
    ), today AS (
        SELECT * FROM player_seasons
        WHERE season = 1996
    )
    SELECT
        COALESCE(t.player_name, y.player_name) AS player_name,
        COALESCE(t.height, y.height) AS height,
        COALESCE(t.college, y.college) AS college,
        COALESCE(t.country, y.country) AS country,
        COALESCE(t.draft_year, y.draft_year) AS draft_year,
        COALESCE(t.draft_round, y.draft_round) AS draft_round,
        COALESCE(t.draft_number, y.draft_number) AS draft_number,
        CASE
            WHEN y.seasons IS NULL
                THEN ARRAY[ROW(
                    t.season,
                    t.gp,
                    t.pts,
                    t.reb,
                    t.ast
                )::season_stats]
            WHEN t.season IS NOT NULL
                THEN y.seasons || ARRAY[ROW(
                    t.season,
                    t.gp,
                    t.pts,
                    t.reb,
                    t.ast
                )::season_stats]
            ELSE y.seasons
        END AS season_stats,
        CASE
            WHEN t.season IS NOT NULL
                THEN CASE
                        WHEN t.pts > 20 THEN 'star'
                        WHEN t.pts > 15 THEN 'good'
                        WHEN t.pts > 10 THEN 'average'
                        ELSE 'bad'
                    END::scoring_class
            ELSE y.scoring_class
        END AS scoring_class,
        CASE
            WHEN t.season IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_active,
        COALESCE(t.season, y.current_season + 1) AS current_season
    FROM today t
        FULL OUTER JOIN yesterday y
            ON t.player_name = y.player_name;

SELECT * FROM players
SELECT
    player_name,
    (seasons[cardinality(seasons)]::season_stats).pts /
    CASE WHEN (seasons[1]::season_stats).pts = 0 THEN 1 ELSE (seasons[1]::season_stats).pts END
FROM players
WHERE current_season = 2001
AND scoring_class = 'star'

