-- int_player_season_enriched.sql
-- Central join hub for the season layer.
-- Combines season stats with player profile and team context so that
-- every downstream model — fct_player_season_stats, int_player_career_totals,
-- and int_player_peer_benchmarks — reads from one consistent enriched source
-- instead of each re-joining stg_players and stg_teams independently.

with seasons as (
    select * from {{ ref('stg_player_season_stats') }}
),

players as (
    select
        player_id,
        position_label,
        birth_country,
        birth_country_label,
        headshot_url,
        shoots_catches,
        height_inches,
        weight_lbs
    from {{ ref('dim_players') }}
),

teams as (
    select
        team_name,
        team_abbr,
        conference,
        division,
        primary_color as team_color
    from {{ ref('dim_teams') }}
)

select
    -- identifiers
    s.player_id,
    s.player_name,
    s.season_year,
    s.season_code,
    s.team_name,

    -- player context
    p.position_label,
    p.birth_country,
    p.birth_country_label,
    p.headshot_url,
    p.shoots_catches,

    -- team context
    t.team_abbr,
    t.conference,
    t.division,
    t.team_color,

    -- season stats
    s.games_played,
    s.goals,
    s.assists,
    s.points,
    s.plus_minus,
    s.power_play_goals,
    s.game_winning_goals,
    s.shots,
    s.shooting_pct,

    -- per-game rates
    round(s.goals::numeric / nullif(s.games_played, 0), 3) as goals_per_game,
    round(s.assists::numeric / nullif(s.games_played, 0), 3) as assists_per_game,
    round(s.points::numeric / nullif(s.games_played, 0), 3) as points_per_game

from seasons s
left join players p using (player_id)
left join teams t on s.team_name = t.team_name
