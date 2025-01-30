''' PySpark Testing Homework
----------------------------------------------------------------------------------------------------------------------------------------------

Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
Create new PySpark jobs in src/jobs for these queries
Create tests in src/tests folder with fake input and expected output data

'''

'''
Convert 2 queries from Weeks 1-2 from PostgreSQL to SparkSQL
'''

# Query 1:
WITH deduped AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id 
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(INSTR(comment, 'DNP'), 0) > 0 AS dim_did_not_play,
    COALESCE(INSTR(comment, 'DND'), 0) > 0 AS dim_did_not_dress,
    COALESCE(INSTR(comment, 'NWT'), 0) > 0 AS dim_not_with_team,
    CAST(SPLIT(min, ':')[0] AS DOUBLE) + CAST(SPLIT(min, ':')[1] AS DOUBLE) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    `TO` AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1
;

# Query 2:
WITH with_previous AS (
    SELECT
        player_name,
        current_season,
        scoring_class,
        is_active,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active
    FROM players
    WHERE current_season <= 2021
),
with_indicators AS (
    SELECT *,
        CASE 
            WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM with_previous
),
with_streaks AS (
    SELECT *,
        SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
)
SELECT player_name,
       scoring_class,
       is_active,
       MIN(current_season) AS start_season,
       MAX(current_season) AS end_season,
       2021 AS current_season
FROM with_streaks
GROUP BY player_name, streak_identifier, is_active, scoring_class
ORDER BY player_name, start_season, end_season
;
