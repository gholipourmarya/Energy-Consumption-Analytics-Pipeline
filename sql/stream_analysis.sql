SELECT
    meter_id,
    reading_date,
    COUNT(*) AS readings,
    ROUND(SUM(consumption_kwh::numeric), 2) AS total_kwh,
    SUM(high_consumption) AS flagged_readings
FROM public.readings
GROUP BY meter_id, reading_date
ORDER BY reading_date, meter_id;
