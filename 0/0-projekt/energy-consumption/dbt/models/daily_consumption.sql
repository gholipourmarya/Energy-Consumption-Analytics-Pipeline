select
    reading_date,
    sum(readings)::bigint as readings,
    sum(total_kwh) as total_kwh,
    sum(flagged_readings)::bigint as flagged_readings
from {{ ref('daily_meter_consumption') }}
group by reading_date
