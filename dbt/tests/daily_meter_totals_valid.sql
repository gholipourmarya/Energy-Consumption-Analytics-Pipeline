select reading_date, meter_id
from {{ ref('daily_meter_consumption') }}
group by reading_date, meter_id
having count(*) > 1
    or min(readings) < 1
    or min(total_kwh) < 0
    or min(flagged_readings) < 0
    or bool_or(flagged_readings > readings)
