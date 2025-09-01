{{
    config(
        schema='public',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='season',
        on_schema_change='append_new_columns'
    )
}}



with totals as (
    
    select 
        year(game_date_est::date) as season,
        sum(pts_home + pts_away) as total_points,
        sum(case when pts_home > pts_away then 1 else 0 end) as total_home_wins,
        count(game_id) as total_games
    from {{ source('nba_raw_data', 'line_score') }}

    {% if is_incremental() %}
        where year(game_date_est::date) >= (select max(season) from {{ this }})
    {% endif %}

    group by 1

)

,final as (
    select 
        season,
        round(total_points / total_games, 2) as ppg,
        round(total_home_wins / total_games, 2) as home_win_percentage,
    from totals
)

select * from final