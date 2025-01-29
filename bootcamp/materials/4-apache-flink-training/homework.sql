/* What is the average number of web events of a session from a user on Tech Creator? */
select avg("num_hits")
from processed_events_aggregated_source
;

/* Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io) */
select "host", avg("num_hits")
from processed_events_aggregated_source
group by "host";