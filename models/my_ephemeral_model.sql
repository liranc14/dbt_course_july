
{{
    config(
        materialized='view'
    )
}}


select * from {{ source('nba_raw_data', 'team') }}