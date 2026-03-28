-- fct_gretzky_dominance.sql
-- Compares Gretzky's career totals to all-time scoring peers.
-- One row per player. Powers the leaderboard and comparison charts on the website.
--
-- The defining stat: Gretzky's career assists alone (1,963) exceed the total
-- career points of every other player in NHL history.

with seasons as (
    select * from {{ ref('stg_player_season_stats') }}
),

career_totals as (
    select
        player_id,
        player_name,
        count(*)                                                as seasons_played,
        sum(games_played)                                       as career_games,
        sum(goals)                                              as career_goals,
        sum(assists)                                            as career_assists,
        sum(points)                                             as career_points
    from seasons
    group by player_id, player_name
),

ranked as (
    select
        *,
        rank() over (order by career_points desc)               as all_time_points_rank,

        -- per-game rates
        round(career_goals::numeric   / nullif(career_games, 0), 3) as goals_per_game,
        round(career_assists::numeric / nullif(career_games, 0), 3) as assists_per_game,
        round(career_points::numeric  / nullif(career_games, 0), 3) as points_per_game,

        -- gap vs Gretzky (only meaningful for non-Gretzky rows, but useful for display)
        max(career_points) over ()                              as gretzky_career_points,
        max(career_assists) over ()                             as gretzky_career_assists,

        -- Gretzky's assists-only advantage over this player's total points
        max(career_assists) over () - career_points             as assists_only_gap

    from career_totals
),

final as (
    select
        all_time_points_rank,
        player_id,
        player_name,
        seasons_played,
        career_games,
        career_goals,
        career_assists,
        career_points,
        goals_per_game,
        assists_per_game,
        points_per_game,
        gretzky_career_points,
        gretzky_career_assists,
        -- how far behind Gretzky's points total is this player?
        gretzky_career_points - career_points                   as points_behind_gretzky,
        -- Gretzky's assists alone vs. this player's total points (the famous stat)
        assists_only_gap,
        case
            when player_id = 8447400 then true
            else false
        end                                                     as is_gretzky
    from ranked
)

select * from final
order by all_time_points_rank
