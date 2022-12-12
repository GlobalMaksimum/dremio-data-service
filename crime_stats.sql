select
    PdDistrict,
    min(n) mx,
    max(n) mn,
    stddev(min_n_by_district) jitter_min,
    stddev(max_n_by_district) jitter_max
from
    (
        select
            PdDistrict,
            DayOfWeek,
            n - min(n) over(partition by PdDistrict) as min_n_by_district,
            n - max(n) over(partition by PdDistrict) as max_n_by_district,
            n
        from
            (
                SELECT
                    PdDistrict,
                    DayOfWeek,
                    avg(X) mean_x,
                    avg(y) as mean_y,
                    count(1) as n
                FROM
                    Samples."samples.dremio.com"."SF_incidents2016.json"
                group by
                    PdDistrict,
                    DayOfWeek
            ) top5
    )
group by
    PdDistrict
order by
    mx