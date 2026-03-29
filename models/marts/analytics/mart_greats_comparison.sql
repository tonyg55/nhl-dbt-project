-- mart_greats_comparison.sql
-- Wide side-by-side comparison of all players in the dataset.
-- One row per player. Designed for the website's comparison table where
-- a visitor can see Gretzky's numbers next to Lemieux, Jagr, Howe, etc.
-- all in a single query result.

with career as (
    select * from {{ ref('fct_player_career') }}
),

dominance as (
    select
        player_id,
        all_time_points_rank,
        all_time_goals_rank,
        all_time_assists_rank,
        all_time_ppg_rank,
        points_behind_leader,
        assists_only_gap,
        is_gretzky
    from {{ ref('fct_player_dominance') }}
)

select
    d.all_time_points_rank,
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
    c.best_season_points,
    c.seasons_100_plus_points,
    c.goals_per_game,
    c.assists_per_game,
    c.points_per_game,
    c.hart_trophies,
    c.art_ross_trophies,
    d.all_time_goals_rank,
    d.all_time_assists_rank,
    d.all_time_ppg_rank,
    d.points_behind_leader,
    d.assists_only_gap,
    d.is_gretzky

from career c
inner join dominance d using (player_id)
order by d.all_time_points_rank
