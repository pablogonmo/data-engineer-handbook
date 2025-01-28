-- Week 4 Applying Analytical Patterns
-- The homework this week will be using the players, players_scd, and player_seasons tables from week 1

/* A query that does state change tracking for players

    A player entering the league should be New
    A player leaving the league should be Retired
    A player staying in the league should be Continued Playing
    A player that comes out of retirement should be Returned from Retirement
    A player that stays out of the league should be Stayed Retired
*/

-- Query 1: State Change Tracking

CREATE TABLE players_growth_accounting (
    player_name         TEXT,
    draft_year          INTEGER,
    retirement_year     INTEGER,
    yearly_active_state TEXT,
    years_active        INTEGER[],
    "year"              INTEGER,
    PRIMARY KEY (player_name, "year")
);

INSERT INTO players_growth_accounting
WITH previous_year AS (
    SELECT *
    FROM players_growth_accounting
    WHERE "year" = 2009
),
current_year AS (
    SELECT
        CAST(player_name AS TEXT) AS player_name,
        current_season AS season_year
    FROM players
    WHERE current_season = 2010
      AND player_name IS NOT NULL
    GROUP BY player_name, current_season
)
SELECT
    COALESCE(curr.player_name, prev.player_name) AS player_name,
    COALESCE(prev.draft_year, curr.season_year) AS draft_year,
    COALESCE(curr.season_year, prev.retirement_year) AS retirement_year,
    CASE
        WHEN prev.player_name IS NULL AND curr.player_name IS NOT NULL THEN 'New'
        WHEN prev.retirement_year = curr.season_year - 1 THEN 'Continued Playing'
        WHEN prev.retirement_year < curr.season_year - 1 THEN 'Returned from Retirement'
        WHEN curr.player_name IS NULL AND prev.retirement_year = prev."year" THEN 'Retired'
        ELSE 'Stayed Retired'
    END AS yearly_active_state,
    COALESCE(prev.years_active, ARRAY[]::INTEGER[]) ||
    CASE
        WHEN curr.player_name IS NOT NULL THEN ARRAY[curr.season_year]
        ELSE ARRAY[]::INTEGER[]
    END AS years_active,
    COALESCE(curr.season_year, prev."year" + 1) AS "year"
FROM current_year curr
FULL OUTER JOIN previous_year prev
    ON curr.player_name = prev.player_name;

SELECT 
    "year" - draft_year AS years_since_draft,
    CAST(COUNT(CASE WHEN yearly_active_state IN ('Continued Playing', 'Returned from Retirement', 'New') THEN 1 END) AS REAL) / COUNT(1) AS pct_active,
    COUNT(1)
FROM players_growth_accounting
GROUP BY "year" - draft_year
ORDER BY 1;

/* A query that uses GROUPING SETS to do efficient aggregations of game_details data

    Aggregate this dataset along the following dimensions:
        - player and team
        - player and season
        - team
*/

-- Query 2: GROUPING SETS Aggregation

WITH games_augmented AS (
    SELECT
        COALESCE(g.season, 0) AS season,
        COALESCE(gd.player_name, 'unknown') AS player_name,
        COALESCE(gd.nickname, 'unknown') AS team_name,
        COALESCE(gd.pts, 0) AS pts
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
)
SELECT
    CASE
        WHEN GROUPING(season) = 0 AND GROUPING(player_name) = 0 AND GROUPING(team_name) = 0 THEN 'season__player_name__team_name'
        WHEN GROUPING(team_name) = 0 AND GROUPING(player_name) = 0 THEN 'team_name_player_name'
        WHEN GROUPING(season) = 0 AND GROUPING(player_name) = 0 THEN 'season__player_name'
        WHEN GROUPING(player_name) = 0 THEN 'player_name'
        WHEN GROUPING(team_name) = 0 THEN 'team_name'
    END AS aggregation_level,
    COALESCE(player_name, 'overall') AS player_name,
    COALESCE(team_name, 'overall') AS team_name,
    SUM(pts) AS total_points
FROM games_augmented
GROUP BY GROUPING SETS (
    (season, player_name, team_name),
    (season, player_name),
    (team_name, player_name),
    (player_name),
    (team_name),
    (season)
)
ORDER BY total_points DESC;

-- Query 3: Player Scoring the Most Points for a Single Team

SELECT
    player_name,
    team_name,
    SUM(pts) AS total_points
FROM game_details
GROUP BY player_name, team_name
ORDER BY total_points DESC
LIMIT 1;

-- Query 4: Player Scoring the Most Points in a Single Season

SELECT
    player_name,
    season,
    SUM(pts) AS total_points
FROM game_details
JOIN games ON game_details.game_id = games.game_id
GROUP BY player_name, season
ORDER BY total_points DESC
LIMIT 1;

/* Query 5: Team with Most Total Wins */
WITH team_wins AS (
    SELECT
        game_id,
        team_id,
        CASE WHEN MAX(pts) OVER(PARTITION BY game_id) = pts THEN 1 ELSE 0 END AS win
    FROM game_details
    GROUP BY game_id, team_id, pts
)
SELECT
    team_id,
    SUM(win) AS total_wins
FROM team_wins
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;



/* A query that uses window functions on game_details to find out:

    What is the most games a team has won in a 90-game stretch?
*/

-- Query 6: Team Wins in a 90-Game Stretch

WITH games_agg AS (
    SELECT
        game_id,
        team_id,
        SUM(pts) AS pts
    FROM game_details
    GROUP BY game_id, team_id
),
won_lost_agg AS (
    SELECT
        game_id,
        team_id,
        pts,
        CASE WHEN MAX(pts) OVER(PARTITION BY game_id) = pts THEN 1 ELSE 0 END AS won
    FROM games_agg
),
max_wins_agg AS (
    SELECT
        team_id,
        SUM(won) OVER(PARTITION BY team_id ORDER BY game_id ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_90_game_stretch
    FROM won_lost_agg
)
SELECT MAX(wins_90_game_stretch) AS max_wins_in_90_games
FROM max_wins_agg;

/*
    How many games in a row did LeBron James score over 10 points?
*/

-- Query 7: Longest Streak for LeBron James

WITH lebron_games AS (
    SELECT DISTINCT
        player_id,
        game_id,
        player_name,
        pts,
        CASE WHEN pts > 10 THEN 1 ELSE 0 END AS scored_gt_10
    FROM game_details
    WHERE player_id = 2544
),
streaks AS (
    SELECT
        game_id,
        player_name,
        pts,
        ROW_NUMBER() OVER(ORDER BY game_id) - ROW_NUMBER() OVER(PARTITION BY scored_gt_10 ORDER BY game_id) AS streak_group
    FROM lebron_games
    WHERE scored_gt_10 = 1
)
SELECT MAX(COUNT(*)) AS longest_scoring_streak
FROM streaks
GROUP BY streak_group;
