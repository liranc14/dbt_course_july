
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        on_schema_change='fail'
    )
}}


select *,
case when city != 'Toronto' then 'USA' else 'Canada' end as country
 from {{ source('nba_raw_data', 'team') }}

{% if is_incremental() %}

    where last_updated > (
        select max(last_updated) from {{ this }}
    )

{% endif %}