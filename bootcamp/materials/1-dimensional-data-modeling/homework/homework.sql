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

drop table actor
;

create table actor (
    actor TEXT,
    actorid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER
);

select min(year), max(year) from actor_films
;

truncate table actor
;

/* from 1970 to 2021 */
insert into actor
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