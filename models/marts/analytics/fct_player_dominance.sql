-- fct_player_dominance.sql
-- Career comparison fact for all players. One row per player.
-- Adds dominance context columns relative to the career points leader
-- (always Gretzky in this dataset). Powers the leaderboard table and
-- bar chart visuals on the website.

with career as (
    select * from {{ ref('int_player_career_totals') }}
),

ranked as (
    select
        *,
        rank() over (order by career_points desc) as all_time_points_rank,
        rank() over (order by career_goals desc) as all_time_goals_rank,
        rank() over (order by career_assists desc) as all_time_assists_rank,
        rank() over (order by points_per_game desc) as all_time_ppg_rank,

        -- Gretzky's career totals as constants for gap calculations
        max(career_points) over () as leader_career_points,
        max(career_assists) over () as leader_career_assists,

        -- how far behind the career points leader is each player?
        max(career_points) over () - career_points as points_behind_leader,

        -- the famous stat: Gretzky's assists alone vs. this player's career points
        max(career_assists) over () - career_points as assists_only_gap,

        case when player_id = 8447400 then true else false end as is_gretzky

    from career
)

select * from ranked
order by all_time_points_rank
