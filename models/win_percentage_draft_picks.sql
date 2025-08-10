
with team_percentage_year as (
select * from {{ ref('win_percentage_year') }}
)

,draft_picks as (
select * from {{ ref('first_round_picks') }}
)


,teams_id as (
select 
    full_name,
    id as team_id
from {{source('nba_raw_data', 'team')}}
)

,final as (
select 
    teams_id.full_name,
    team_percentage_year.win_percentage,
    draft_picks.season,
    draft_picks.round_pick,
    draft_picks.player_name,
from teams_id
left join team_percentage_year
using (team_id)
left join draft_picks
using (team_id, season)
)

select * from final