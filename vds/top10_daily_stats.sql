select
    *
from
    sf.daily_stat
where
    daily_stat.mean_prcp > 0
order by
    3 desc
limit
    10;