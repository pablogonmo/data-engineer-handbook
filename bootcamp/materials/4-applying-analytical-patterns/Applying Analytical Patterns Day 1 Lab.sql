CREATE TABLE users_growth_accounting (
    user_id TEXT,
    first_active_date DATE,
    last_active_date DATE,
    daily_active_state TEXT,
    weekly_active_state TEXT,
    dates_active DATE[],
    date DATE,
    PRIMARY KEY (user_id, date)
);

insert into users_growth_accounting
 with yesterday as (
    select *
    from users_growth_accounting
    where date = DATE('2023-01-08')
 ),
 today as (
    select
    cast(user_id as text) as user_id,
    date_trunc('day',event_time::timestamp) as today_date,
    count(1)
    from events
    where date_trunc('day', event_time::timestamp) = DATE('2023-01-09') 
    and user_id is not null
    group by user_id, date_trunc('day',event_time::timestamp)
 )
 select
 coalesce(t.user_id, y.user_id)                 as user_id,
 coalesce(y.first_active_date, t.today_date)    as first_active_date,
 coalesce(t.today_date, y.last_active_date)     as last_active_date,
case
    when y.user_id is null and t.user_id is not null            then 'New'
    when y.last_active_date = t.today_date - Interval '1 day'   then 'Retained'
    when y.last_active_date < t.today_date - Interval '1 day'   then 'Resurrected'
    when t.today_date is null and y.last_active_date = y.date   then 'Churned'
    else 'Stale'
end as daily_active_state,
case 
    when y.user_id is null and t.user_id is not null                                then 'New'
    when y.last_active_date >= y.date - Interval '7 day'                            then 'Retained'
    when y.last_active_date < t.today_date - Interval '7 day'                       then 'Resurrected'
    when t.today_date is null and y.last_active_date = y.date - Interval '7 day'    then 'Churned'
    else 'Satle'
end as weekly_active_state,
coalesce(y.dates_active,
    array[]::date[])
        || case when
                t.user_id is not null
                then array[t.today_date]
                else array[]::date[]
            end as date_list,
coalesce(t.today_date, y.date + Interval '1 day') as date
from today t
    full outer join yesterday y
        on t.user_id = y.user_id
;

select date, daily_active_state, count(1)
from users_growth_accounting
where date = DATE('2023-01-03') and daily_active_state = 'Churned'
group by date, daily_active_state
;

select extract(dow from first_active_date) as dow,
    date - first_active_date as days_since_first_active,
    cast(count(case
                    when daily_active_state in ('Retained','Resurrected','New') 
                    then 1
                end) as real)/count(1) as pct_active,
    count(1)
from users_growth_accounting
--where first_active_date = date('2023-01-02')
group by  extract(dow from first_active_date), date - first_active_date
order by 2