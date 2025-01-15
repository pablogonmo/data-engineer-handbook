/*
    A query to deduplicate game_details from Day 1 so there's no duplicates
*/

create table fct_game_details (
    dim_game_date date,
    dim_season integer,
    dim_team_id integer,
    dim_player_id integer,
    dim_player_name text,
    dim_start_position text,
    dim_is_playing_at_home boolean,
    dim_did_not_play boolean,
    dim_did_not_dress boolean,
    dim_not_with_team boolean,
    m_minutes real,
    m_fgm integer,
    m_fga integer,
    m_fg3m integer,
    m_fg3a integer,
    m_ftm integer,
    m_fta integer,
    m_oreb integer,
    m_dreb integer,
    m_reb integer,
    m_ast integer,
    m_stl integer,
    m_blk integer,
    m_turnovers integer,
    m_pf integer,
    m_pts integer,
    m_plus_minus integer,
primary key (dim_game_date, dim_team_id, dim_player_id)
)
;

insert into fct_game_details
with deduped as (
    select g.game_date_est,
           g.season,
           g.home_team_id,
           gd.*,
           row_number() over (partition by gd.game_id, team_id, player_id order by g.game_date_est) as row_num
    from game_details gd
    join games g on gd.game_id = g.game_id 
)
select game_date_est as dim_gate_date,
           season as dim_season,
           team_id as dim_team_id,
           player_id as dim_player_id,
           player_name as dim_player_name,
           start_position as dim_start_position,
           team_id = home_team_id as dim_is_playing_at_home,
           coalesce(position('DNP' in comment),0) > 0  as dim_did_not_play, 
           coalesce(position('DND' in comment),0) > 0  as dim_did_not_dress, 
           coalesce(position('NWT' in comment),0) > 0  as dim_not_with_team, 
           cast(split_part(min,':',1) as real) + cast(split_part(min,':',2) as real)/60 as m_minutes,
           fgm as m_fgm,
           fga as m_fga,
           fg3m as m_fg3m,
           fg3a as m_fg3a,
           ftm as m_ftm,
           fta as m_fta,
           oreb as m_oreb,
           dreb as m_dreb,
           reb as m_reb,
           ast as m_ast,
           stl as m_stl,
           blk as m_blk,
           "TO" as m_turnovers,
           pf as m_pf,
           pts as m_pts,
           plus_minus as m_plus_minus
from deduped
where row_num = 1
;

/*
    A DDL for an user_devices_cumulated table that has:
        a device_activity_datelist which tracks a users active days by browser_type
        data type here should look similar to MAP<STRING, ARRAY[DATE]>
            or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)
    
    A cumulative query to generate device_activity_datelist from events

    A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
*/

create table user_devices_cumulated (
    "device_id" text,
    "browser_type" text,
    "device_activity_datelist" date[],
    "date" date,
primary key ("device_id", "browser_type", "date")
)
;

insert into user_devices_cumulated
with
    yesterday as (
    select *
    from user_devices_cumulated
    where "date" = date('2023-01-30')
),
    today as (
    select
        cast(e.device_id as text) as device_id,
        d.browser_type,
        date(cast(e.event_time as timestamp)) as "device_activity_datelist"
    from events e
        join devices d on e.device_id = d.device_id
    where date(cast(e.event_time as timestamp)) = date('2023-01-31')
        and e.device_id is not null
    group by e.device_id, d.browser_type, date(cast(e.event_time as timestamp))
)
select coalesce(t.device_id, y.device_id) as device_id, coalesce(t.browser_type, y.browser_type) as browser_type,
        case 
            when y.device_activity_datelist is null then array[t.device_activity_datelist]
            when t.device_activity_datelist is null then y.device_activity_datelist 
            else array[t.device_activity_datelist] || y.device_activity_datelist
        end as device_activity_datelist,
        coalesce(t.device_activity_datelist, y.date + interval '1 day') as "date"
from today t
    full outer join yesterday y 
        on t.device_id = y.device_id and t.browser_type = y.browser_type
;

with devices as (
    select *
    from user_devices_cumulated
    where "date"= date('2023-01-31')
),
series as (
    select *
    from generate_series(date('2023-01-01'),date('2023-01-31'), interval '1 day')
        as series_date
),
datelist_int as (
    select (case when device_activity_datelist @> array[date(series_date)]
                then cast(pow(2, 32 -("date" - date(series_date))) as bigint)
                else 0
            end) as datelist_int
            , *
    from devices
        cross join series
)
select 
    device_id, browser_type,
    cast(cast(sum(datelist_int) as bigint) as bit(32)),
    bit_count(cast(cast(sum(datelist_int) as bigint) as bit(32))) > 0 as dim_is_monthly_active,
    bit_count(cast('1111111'as bit(32)) & cast(cast(sum(datelist_int) as bigint) as bit(32))) > 0 as dim_is_weekly_active,
    bit_count(cast('1000000'as bit(32)) & cast(cast(sum(datelist_int) as bigint) as bit(32))) > 0 as dim_is_daily_active
from datelist_int
group by device_id, browser_type
;

/*
    A DDL for hosts_cumulated table
        a metric_array which logs to see which dates each host is experiencing any activity
    
    The incremental query to generate metric_array

    A monthly, reduced fact table DDL host_hits_reduced
        month
        host
        hit_array - think COUNT(1)
        unique_visitors array - think COUNT(DISTINCT user_id)

    An incremental query that loads host_hits_reduced
        day-by-day
*/

create table hosts_cumulated (
    host_id text,
    month_start date,
    metric_name text,
    metric_array real[],
primary key (host_id, month_start, metric_name)
)
;

insert into hosts_cumulated
with events_metrics as (    
    select
        host                    as host_id,
        'host_hits'             as metric_name,
        date(event_time)        as date,
        count(1)                as metric_value
    from events
    group by host_id, metric_name, date
        union all
    select 
        host                    as host_id,
        'unique_visitors'       as metric_name,
        date(event_time)        as date,
        count(distinct user_id) as metric_value
    from events
    group by host_id, metric_name, date
),
daily_aggregate as (
    select
        host_id,
        metric_name,
        date,
        metric_value
    from events_metrics
    where date = date('2023-01-04')
        and host_id is not null
),
yesterday_array as (
    select * from hosts_cumulated
    where month_start = date('2023-01-01')
)
    select coalesce(da.host_id, ya.host_id) as host_id,
            coalesce(ya.month_start, date_trunc('month', da.date)) as month_start,
            coalesce(da.metric_name, ya.metric_name) as metric_name,
            case
                when ya.metric_array is not null 
                    then ya.metric_array || array[coalesce(da.metric_value, 0)]
                when ya.metric_array is null
                    then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.metric_value, 0)]
            end as metric_array
    from daily_aggregate da
        full outer join yesterday_array ya
            on da.host_id = ya.host_id and da.metric_name = ya.metric_name
on conflict (host_id, month_start, metric_name)
    do
        update set metric_array = excluded.metric_array
;

create table host_activity_reduced (
    month                   date,
    host                    text,
    hit_array               real[],
    unique_visitors_array   real[],
primary key (month, host)
)
;

insert into host_activity_reduced
with agg as (
    select host_id
            , metric_name
            , month_start
            , array[sum(metric_array[1]), sum(metric_array[2]), sum(metric_array[3]), sum(metric_array[4])] as summed_array
from hosts_cumulated
group by host_id, metric_name, month_start
)
select 
 month_start as month
, host_id as host
, max(case when metric_name = 'host_hits' then summed_array end) as hit_array
, max(case when metric_name = 'unique_visitors' then summed_array end) as unique_visitors_array
from agg
group by month_start, host_id
;