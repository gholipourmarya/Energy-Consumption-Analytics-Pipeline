select
    reading_date,
    meter_id,
    count(*) as readings,
    sum(consumption_kwh) as total_kwh,
    sum(high_consumption) as flagged_readings
from {{ ref('stg_readings') }}
group by reading_date, meter_id
