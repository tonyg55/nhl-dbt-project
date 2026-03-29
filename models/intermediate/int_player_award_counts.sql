-- int_player_award_counts.sql
-- Pivots award_winners into one row per player with a column per award type.
-- Joins on player_name since the awards seed does not carry player_id.
-- Referenced by fct_gretzky_career to get Gretzky's trophy counts cleanly.

with awards as (
    select * from {{ ref('stg_award_winners') }}
)

select
    winner as player_name,
    sum(case when award_name = 'Hart Trophy' then 1 else 0 end) as hart_trophies,
    sum(case when award_name = 'Art Ross Trophy' then 1 else 0 end) as art_ross_trophies
from awards
group by winner
