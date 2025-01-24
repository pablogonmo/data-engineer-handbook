-- Week 4 Applying Analytical Patterns
--The homework this week will be using the players, players_scd, and player_seasons tables from week 1

/* A query that does state change tracking for players

    A player entering the league should be New
    A player leaving the league should be Retired
    A player staying in the league should be Continued Playing
    A player that comes out of retirement should be Returned from Retirement
    A player that stays out of the league should be Stayed Retired
*/

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
 with yesterday as (
    select *
    from players_growth_accounting
    where "year" = 2009
 ),
 today as (
    select
    cast(player_name as text) as player_name,
    current_season as today_date,
    count(1)
    from players
    where current_season = 2010
    and player_name is not null
    group by player_name, current_season
 )
 select
 coalesce(t.player_name, y.player_name)         as player_name,
 coalesce(y.draft_year, t.today_date)    as draft_year,
 coalesce(t.today_date, y.retirement_year)     as retirement_year,
case
    when y.player_name is null and t.player_name is not null    then 'New'
    when y.retirement_year = t.today_date - 1                  then 'Continued Playing'
    when y.retirement_year < t.today_date - 1                  then 'Returned from Retirement'
    when t.today_date is null and y.retirement_year = y.year   then 'Retired'
    else 'Stayed Retired'
end as yearly_active_state,
coalesce(y.years_active,
    array[]::integer[])
        || case when
                t.player_name is not null
                then array[t.today_date]
                else array[]::integer[]
            end as year_list,
coalesce(t.today_date, y.year + 1) as "year"
from today t
    full outer join yesterday y
        on t.player_name = y.player_name
;

select "year" - draft_year as years_since_draft,
    cast(count(case
                    when yearly_active_state in ('Continued Playing','Returned from Retirement','New') 
                    then 1
                end) as real)/count(1) as pct_active,
    count(1)
from players_growth_accounting
group by "year" - draft_year
order by 1
;

/* A query that uses GROUPING SETS to do efficient aggregations of game_details data

    Aggregate this dataset along the following dimensions
        player and team
            Answer questions like who scored the most points playing for one team?
        player and season
            Answer questions like who scored the most points in one season?
        team
            Answer questions like which team has won the most games?
*/

with games_augmented as (
    select
        coalesce(g.season, 0)                   as season,
        coalesce(gd.player_name, 'unknown')     as player_name,
        coalesce(gd.nickname, 'unknown')        as team_name,
        coalesce(gd.pts, 0)                     as pts
    from game_details gd
        join games g on gd.game_id = g.game_id
)
select
    case
        when grouping(season) = 0 and grouping(player_name) = 0 and grouping(team_name) = 0 then 'season__player_name__team_name'
        when grouping(team_name) = 0 and grouping(player_name) = 0 then 'team_name_player_name'
        when grouping(season) = 0 and grouping(player_name) = 0 then 'season__player_name'
        when grouping(player_name) = 0 then 'player_name'
        when grouping(team_name) = 0 then 'team_name'
    end as aggregation_level,
    coalesce(player_name, 'overall') as player_name,
    coalesce(team_name, 'overall')    as team_name,
    sum(pts)
from games_augmented
group by grouping sets (
    (season, player_name, team_name),
    (season, player_name),
    (team_name, player_name),
    player_name,
    team_name,
    season
    )
order by sum(pts) desc
;

/* A query that uses window functions on game_details to find out the following things:

    What is the most games a team has won in a 90 game stretch?
*/

with games_agg as (
    select
        game_id
        , team_id
        , sum(pts) as pts
    from game_details
    group by game_id, team_id
),
won_lost_agg as (
    select 
            game_id
            , team_id
            , pts
            , case when max(pts) over(partition by game_id) = pts then 1 else 0 end won_lost
    from games_agg
),
max_wons_agg as (
    select
        team_id
        , sum(won_lost) over(partition by team_id order by game_id desc rows between 90 preceding and current row) as "90_days_win"
    from won_lost_agg
)
select max("90_days_win") as max_90_days_win
from max_wons_agg
;

/*
    How many games in a row did LeBron James score over 10 points a game?
*/

with game_distinct_details as (
    select distinct player_id, game_id, player_name, pts from game_details
),
lebron_points_per_game as (
    select distinct
        game_id
        , player_name
        , pts
        , case when pts > 10 then 1 else 0 end gt_10
        , case when lag(pts) over(partition by player_name order by game_id) > 10 then 1 else 0 end lt_10
        , case when lead(pts) over(partition by player_name order by game_id) > 10 then 1 else 0 end nt_10
        , row_number() over(partition by player_name order by game_id) rownumber
    from game_distinct_details
    where player_id = 2544
),
games_over_10 as (
    select
    player_name
    , game_id
    , pts
    , gt_10
    , case when lt_10!=0 then rownumber end lt
    , case when nt_10=0 then rownumber end nt
    , rownumber
    from lebron_points_per_game
    where gt_10!=0
    order by game_id
),
strikes_in_a_row as (
    select rownumber-lag(rownumber) over (partition by player_name order by game_id) as strike
    from games_over_10 where (lt is null and nt is null) or (lt is not null and nt is not null)
)
select max(strike) from strikes_in_a_row
;