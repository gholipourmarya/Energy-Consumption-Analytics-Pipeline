{{ config(materialized='view') }}

select recorded_at, accepted, rejected, duplicates
from {{ source('stream', 'quality_batches') }}
