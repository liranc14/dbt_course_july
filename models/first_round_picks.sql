
select 
    season, 
    player_name, 
    team_id, 
    round_pick
from {{ source('nba_raw_data', 'draft_history') }} 
where round_number = 1