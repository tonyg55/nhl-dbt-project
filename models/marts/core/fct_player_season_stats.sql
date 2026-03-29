-- fct_player_season_stats.sql
-- Season-level fact table. One row per player per season.
-- Intentionally thin — enrichment and joins are already resolved in
-- int_player_season_enriched. This model adds YoY delta and peer rankings
-- from int_player_peer_benchmarks, then exposes the full row to the website.

with enriched as (
    select * from {{ ref('int_player_season_enriched') }}
),

benchmarks as (
    select
        player_id,
        season_year,
        points_rank,
        goals_rank,
        assists_rank,
        points_behind_season_leader
    from {{ ref('int_player_peer_benchmarks') }}
)

select
    e.player_id,
    e.player_name,
    e.position_label,
    e.birth_country_label,
    e.headshot_url,
    e.season_year,
    e.season_code,
    e.team_name,
    e.team_abbr,
    e.conference,
    e.division,
    e.team_color,
    e.games_played,
    e.goals,
    e.assists,
    e.points,
    e.plus_minus,
    e.power_play_goals,
    e.game_winning_goals,
    e.shots,
    e.shooting_pct,
    e.goals_per_game,
    e.assists_per_game,
    e.points_per_game,

    -- peer context
    b.points_rank,
    b.goals_rank,
    b.assists_rank,
    b.points_behind_season_leader,

    -- year-over-year delta
    e.points - lag(e.points) over (
        partition by e.player_id
        order by e.season_year
    ) as points_yoy_delta

from enriched e
left join benchmarks b using (player_id, season_year)
order by e.season_year, b.points_rank
