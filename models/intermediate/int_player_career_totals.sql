-- int_player_career_totals.sql
-- Aggregates int_player_season_enriched to the career level for all players.
-- Single source of truth for career totals — referenced by fct_player_career,
-- fct_player_dominance, mart_greats_comparison, and mart_gretzky_case_for_greatest.
-- Sources from int_player_season_enriched so player/team context is already resolved.

with enriched as (
    select * from {{ ref('int_player_season_enriched') }}
)

select
    player_id,
    player_name,
    position_label,
    birth_country_label,
    headshot_url,

    count(*) as seasons_played,
    sum(games_played) as career_games,
    sum(goals) as career_goals,
    sum(assists) as career_assists,
    sum(points) as career_points,
    sum(power_play_goals) as career_pp_goals,
    sum(game_winning_goals) as career_gwg,
    min(season_year) as first_season,
    max(season_year) as last_season,
    max(points) as best_season_points,

    -- per-game rates
    round(sum(goals)::numeric / nullif(sum(games_played), 0), 3) as goals_per_game,
    round(sum(assists)::numeric / nullif(sum(games_played), 0), 3) as assists_per_game,
    round(sum(points)::numeric / nullif(sum(games_played), 0), 3) as points_per_game,

    -- 100-point seasons, a marker of elite production
    sum(case when points >= 100 then 1 else 0 end) as seasons_100_plus_points

from enriched
group by player_id, player_name, position_label, birth_country_label, headshot_url
