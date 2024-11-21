/* 
1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.

    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
*/
select * from actor_films where actor='Elizabeth Taylor' order by year
;

create type films as (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

create type quality_class as enum ('star','good','average','bad')
;

--drop table actors;

create table actors (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER
);

-- 2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.
select min(year), max(year) from actor_films
;

--truncate table actors;

insert into actors
with yesterday as (
    select * from actor where current_year=1971
),
    today as (
    select * from actor_films where year=1972
)
select 
    coalesce(t.actor, y.actor) as actor,
    coalesce(t.actorid, y.actorid) as actorid,
    case when y.actorid is null then ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::films]
        when t.actorid is not null then y.films || ARRAY_AGG(ROW(t.film, t.votes, t.rating, t.filmid)::films)
        else y.films
    end as films,
    case when t.actorid is not null then
        case 
            when t.rating > 8 then 'star'
            when t.rating > 7 then 'good'
            when t.rating > 6 then 'average'
            else 'bad'
        end::quality_class
        else y.quality_class
    end as quality_class,
    case when t.actorid is not null then true else false end as is_active,
    coalesce(t.year,y.current_year+1) as current_year
from today t full outer join yesterday y
    on t.actorid = y.actorid
group by t.actor, y.actor,t.actorid, y.actorid, t.film, t.votes, t.rating, t.filmid, y.films, y.quality_class, t.year, y.current_year
;

select * from actor where actor='Elizabeth Taylor'
;

/* 
3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.
*/
--drop table actors_history_scd;

create table actors_history_scd
(
	actor TEXT,
	actorid TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER--,	PRIMARY KEY(actorid, start_year)
);

-- 4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
insert into actors_history_scd
with with_previous as (
	select
		actor,
        actorid,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class,1) over (partition by actorid order by current_year) as previous_quality_class,
		LAG(is_active,1) over (partition by actorid order by current_year) as previous_is_active
	from actors
	where current_year <= 2021
),
with_indicators as (
	select *,
	case 
		when quality_class <> previous_quality_class then 1
		when is_active <> previous_is_active then 1 
		else 0 
	end as change_indicator
	from with_previous
),
with_streaks as (
	select *
	, sum(change_indicator) over(partition by actorid order by current_year) as streak_identifier
	from with_indicators
)
select actor, 
actorid,
quality_class,
is_active,
min(current_year) as start_year,
max(current_year) as end_year,
2021 as current_year
from with_streaks
group by actor, actorid, streak_identifier, is_active, quality_class
order by 1,4,5
;

select * from actors_history_scd
;

-- 5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.
create type actors_scd_type as (
	quality_class quality_class,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
)
;

with last_year_scd as (
	select * from actors_history_scd where current_year = 2021 and end_year = 2021
), 
historical_scd as (
	select actor, actorid,
	quality_class,
	is_active,
	start_year,
	end_year
	from actors_history_scd
    where current_year = 2021 and end_year < 2021
),
this_year_data as (
	select * from actors where current_year = 2022
),
unchanged_records as (
select ts.actor, ts.actorid, ts.quality_class, ts.is_active, ls.start_year, ls.current_year as end_year 
from this_year_data ts
	join last_year_scd ls 
		on ts.actorid = ls.actorid
	where ts.quality_class = ls.quality_class
	and ts.is_active = ls.is_active
),
changed_records as (
	select ts.actor, ts.actorid,
	unnest(
		array[
			row(
				ls.quality_class,
				ls.is_active,
				ls.start_year,
				ls.end_year			
			)::actors_scd_type,
			row(
				ts.quality_class,
				ts.is_active,
				ts.current_year,
				ts.current_year			
			)::actors_scd_type
		]) as records
from this_year_data ts
	left join last_year_scd ls 
		on ts.actorid = ls.actorid
	where (ts.quality_class <> ls.quality_class
	or ts.is_active <> ls.is_active)
),
unnested_changed_records as (
	select actor, actorid,
	(records::actors_scd_type).quality_class,
	(records::actors_scd_type).is_active,
	(records::actors_scd_type).start_year,
	(records::actors_scd_type).end_year
	from changed_records
),
new_records as (
	select ts.actor, ts.actorid,
	ts.quality_class,
	ts.is_active,
	ts.current_year as start_year,
	ts.current_year as end_year
	from this_year_data ts
	left join last_year_scd ls
		on ts.actorid = ls.actorid
	where ls.actorid is null
)
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records
;