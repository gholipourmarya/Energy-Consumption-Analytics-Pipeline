{{ config(materialized='view') }}

select
    reading_id,
    meter_id,
    reading_date,
    consumption_kwh::numeric as consumption_kwh,
    high_consumption
from {{ source('stream', 'readings') }}
