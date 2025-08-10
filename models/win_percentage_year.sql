SELECT
    year(game_date::date) season,
    team_id_home as team_id,
    SUM(CASE WHEN wl_home = 'W' THEN 1 ELSE 0 END) / COUNT(1) AS win_percentage
FROM
    {{ source('nba_raw_data', 'game') }}
where season > 2000
GROUP BY all