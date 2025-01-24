/* Funnel Analysis */

with deduped_events as (
    select
        user_id,
        url,
        event_time,
        date(event_time) as event_date 
    from events
    where url in ('/signup', '/api/v1/users')
    group by user_id, url, event_time, date(event_time)
),
selfjoined as (
    select d1.user_id, d1.url, d2.url as destination_url, d1.event_time, d2.event_time
    from deduped_events d1
        join deduped_events d2
            on d1.user_id = d2.user_id
            and d1.event_date = d2.event_date
            and d2.event_time > d1.event_time
    where d1.url = '/signup' --and d2.url = '/api/v1/users' 
),
userlevel as (
    select
        user_id,
        url,
        count(1) as number_of_hits,
        sum(case when destination_url = '/api/v2/users' then 1 else 0 end) as converted
    from selfjoined
    group by user_id, url
)
select
url,
sum(number_of_hits)                                 as num_hits,
sum(converted)                                      as num_converted,
cast(sum(converted) as real)/sum(number_of_hits)    as pct_converted
from userlevel
group by url
having sum(number_of_hits) > 500
;

/* Grouping Sets */
with events_augmented as 
(
    select
        coalesce(d.os_type, 'unknown')          as os_type,
        coalesce(d.device_type, 'unknown')      as device_type,
        coalesce(d.browser_type, 'unknown')     as browser_type,
        url,
        user_id
    from events e
        join devices d on e.device_id = d.device_id
)
select
--    grouping(os_type),
--    grouping(device_type),
--    grouping(browser_type),
    case 
        when grouping(os_type) = 0
            and grouping(device_type) = 0
            and grouping(browser_type) = 0
            then 'os_type__device_type__browser'
        when grouping(browser_type) = 0 then 'browser_type'
        when grouping(device_type) = 0  then 'device_type'
        when grouping(os_type) = 0      then 'os_type'
    end as aggregation_level,
    coalesce(os_type, '(overall)')          as os_type,
    coalesce(device_type, '(overall)')      as device_type,
    coalesce(browser_type, '(overall)')     as browser_type,
    count(1)                                as number_of_hits
from events_augmented
group by grouping sets (
    (browser_type, os_type, device_type),
    (browser_type),
    (os_type),
    (device_type)
)
order by count(1) desc
;

with events_augmented as 
(
    select
        coalesce(d.os_type, 'unknown')          as os_type,
        coalesce(d.device_type, 'unknown')      as device_type,
        coalesce(d.browser_type, 'unknown')     as browser_type,
        url,
        user_id
    from events e
        join devices d on e.device_id = d.device_id
)
select
    case 
        when grouping(os_type) = 0
            and grouping(device_type) = 0
            and grouping(browser_type) = 0
            then 'os_type__device_type__browser'
        when grouping(browser_type) = 0 then 'browser_type'
        when grouping(device_type) = 0  then 'device_type'
        when grouping(os_type) = 0      then 'os_type'
    end as aggregation_level,
    coalesce(os_type, '(overall)')          as os_type,
    coalesce(device_type, '(overall)')      as device_type,
    coalesce(browser_type, '(overall)')     as browser_type,
    count(1)                                as number_of_hits
from events_augmented
group by cube (os_type, device_type, browser_type)
order by count(1) desc
;

with events_augmented as 
(
    select
        coalesce(d.os_type, 'unknown')          as os_type,
        coalesce(d.device_type, 'unknown')      as device_type,
        coalesce(d.browser_type, 'unknown')     as browser_type,
        url,
        user_id
    from events e
        join devices d on e.device_id = d.device_id
)
select
    case 
        when grouping(os_type) = 0
            and grouping(device_type) = 0
            and grouping(browser_type) = 0
            then 'os_type__device_type__browser'
        when grouping(browser_type) = 0 then 'browser_type'
        when grouping(device_type) = 0  then 'device_type'
        when grouping(os_type) = 0      then 'os_type'
    end as aggregation_level,
    coalesce(os_type, '(overall)')          as os_type,
    coalesce(device_type, '(overall)')      as device_type,
    coalesce(browser_type, '(overall)')     as browser_type,
    count(1)                                as number_of_hits
from events_augmented
group by rollup (os_type, device_type, browser_type)
order by count(1) desc
;
