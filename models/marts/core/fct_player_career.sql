-- fct_player_career.sql
-- Career fact table for all players in the dataset. One row per player.
-- Combines career totals with trophy counts. The foundational career table —
-- analytics marts layer on top of this for comparison and dominance analysis.

with career as (
    select * from {{ ref('int_player_career_totals') }}
),

awards as (
    select * from {{ ref('int_player_award_counts') }}
)

select
    c.player_id,
    c.player_name,
    c.position_label,
    c.birth_country_label,
    c.headshot_url,
    c.first_season,
    c.last_season,
    c.seasons_played,
    c.career_games,
    c.career_goals,
    c.career_assists,
    c.career_points,
    c.career_pp_goals,
    c.career_gwg,
    c.best_season_points,
    c.seasons_100_plus_points,
    c.goals_per_game,
    c.assists_per_game,
    c.points_per_game,

    coalesce(a.hart_trophies, 0) as hart_trophies,
    coalesce(a.art_ross_trophies, 0) as art_ross_trophies

from career c
left join awards a on c.player_name = a.player_name
