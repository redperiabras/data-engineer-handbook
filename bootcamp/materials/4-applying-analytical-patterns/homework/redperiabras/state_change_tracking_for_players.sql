-- A query that does state change tracking for players

-- A player entering the league should be New
-- A player leaving the league should be Retired
-- A player staying in the league should be Continued Playing
-- A player that comes out of retirement should be Returned from Retirement
-- A player that stays out of the league should be Stayed Retired

-- DDL to store growth accounting for players
DROP TABLE IF EXISTS players_growth_accounting;
CREATE TABLE players_growth_accounting (
    player_name TEXT,
    first_active_season INT,
    last_active_season INT,
    active_state TEXT,
    seasons_active INT[],
    season INT,
    PRIMARY KEY (player_name, season)
);


-- DML to insert data into players_growth_accounting
INSERT INTO players_growth_accounting
WITH last_season AS (
    SELECT
        *
    FROM
        players_growth_accounting
    WHERE
        season = 1999
),
this_season AS (
    SELECT
        player_name,
        season
    FROM
        player_seasons
    WHERE
        season = 2000
)
SELECT
    COALESCE(ts.player_name, ls.player_name) AS player_name,
    COALESCE(ls.first_active_season, ts.season) AS first_active_season,
    COALESCE(ts.season, ls.last_active_season) AS last_active_season,
    CASE
        WHEN ls.player_name IS NULL 
            THEN 'NEW'
        WHEN ls.last_active_season = ts.season - 1
            THEN 'CONTINUED PLAYING'
        WHEN ls.last_active_season < ts.season - 1 
            THEN 'RETURNED FROM RETIREMENT'
        WHEN ts.season IS NULL AND ls.last_active_season = ls.season 
            THEN 'RETIRED'
        ELSE 'STAYED RETIRED'
    END AS active_state,
    COALESCE(ls.seasons_active, ARRAY[]::INT[])
        || CASE
            WHEN ts.player_name IS NOT NULL
                THEN ARRAY[ts.season]
            ELSE ARRAY[]::INT[]
    END AS seasons_active,
    COALESCE(ts.season, ls.season + 1) AS season
FROM this_season AS ts
FULL OUTER JOIN last_season AS ls
ON ls.player_name = ts.player_name

--- Test Query to check the active states for players in the year 2000
SELECT active_state, COUNT(1) 
FROM players_growth_accounting
WHERE season = 2000
GROUP BY
    active_state
ORDER BY
    2 DESC

