-- mart_gretzky_case_for_greatest.sql
-- Single-row analytical showcase. Every data point needed to make the case
-- that Wayne Gretzky is the greatest offensive player in NHL history.
-- Aggregates evidence from career stats, peer benchmarks, trophies, and Cups.
-- Intended to power a dedicated "The Case for the Greatest" section on the website.

with career as (
    select * from {{ ref('fct_player_career') }}
    where player_id = 8447400
),

-- 2nd-best all-time career points in dataset (for assists_vs_best comparison)
runner_up as (
    select career_points as runner_up_career_points
    from {{ ref('int_player_career_totals') }}
    where player_id != 8447400
    order by career_points desc
    limit 1
),

benchmarks as (
    select * from {{ ref('int_player_peer_benchmarks') }}
    where player_id = 8447400
),

cups as (
    select count(*) as stanley_cups
    from {{ ref('stanley_cup_champions') }}
    where gretzky_on_roster = true
),

-- aggregate benchmark rows into a single evidence block
benchmark_summary as (
    select
        sum(case when points_rank = 1 then 1 else 0 end) as seasons_led_scoring,
        round(avg(case when points_rank = 1 then margin_above_2nd_place end), 1) as avg_margin_when_led,
        max(margin_above_2nd_place) as largest_margin_above_2nd,
        sum(case when points_rank <= 3 then 1 else 0 end) as seasons_top_3_scoring
    from benchmarks
)

select
    -- identity
    c.player_id,
    c.player_name,

    -- career counting stats
    c.career_games,
    c.career_goals,
    c.career_assists,
    c.career_points,
    c.career_pp_goals,
    c.career_gwg,

    -- career rates
    c.goals_per_game,
    c.assists_per_game,
    c.points_per_game,

    -- season milestones
    c.best_season_points,
    c.seasons_100_plus_points,
    c.seasons_played,
    c.first_season,
    c.last_season,

    -- hardware
    c.hart_trophies,
    c.art_ross_trophies,
    cu.stanley_cups,

    -- dominance vs peers
    bs.seasons_led_scoring,
    bs.avg_margin_when_led,
    bs.largest_margin_above_2nd,
    bs.seasons_top_3_scoring,

    -- the signature stat: assists alone beat the next best player's career points
    c.career_assists - r.runner_up_career_points as assists_vs_runner_up_gap,
    r.runner_up_career_points

from career c
cross join cups cu
cross join benchmark_summary bs
cross join runner_up r
