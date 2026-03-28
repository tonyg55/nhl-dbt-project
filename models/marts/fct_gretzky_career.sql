-- fct_gretzky_career.sql
-- Wayne Gretzky's complete career summary.
-- One row. Feeds the "Gretzky career card" on the website.
--
-- Answers: career points, goals, assists, per-game rates,
--          Hart Trophy count, Art Ross Trophy count, Stanley Cup count.

with seasons as (
    select * from {{ ref('stg_player_season_stats') }}
    where player_id = 8447400
),

awards as (
    select * from {{ ref('stg_award_winners') }}
    where winner = 'Wayne Gretzky'
),

cups as (
    select * from {{ ref('stg_stanley_cup_champions') }}
    where gretzky_on_roster = true
),

career_totals as (
    select
        player_id,
        player_name,
        count(*)                                        as seasons_played,
        sum(games_played)                               as career_games,
        sum(goals)                                      as career_goals,
        sum(assists)                                    as career_assists,
        sum(points)                                     as career_points,
        sum(power_play_goals)                           as career_pp_goals,
        sum(game_winning_goals)                         as career_gwg,
        min(season_year)                                as first_season,
        max(season_year)                                as last_season
    from seasons
    group by player_id, player_name
),

trophy_counts as (
    select
        sum(case when award_name = 'Hart Trophy'     then 1 else 0 end) as hart_trophies,
        sum(case when award_name = 'Art Ross Trophy' then 1 else 0 end) as art_ross_trophies
    from awards
),

cup_count as (
    select count(*) as stanley_cups
    from cups
)

select
    t.player_id,
    t.player_name,
    t.first_season,
    t.last_season,
    t.seasons_played,
    t.career_games,
    t.career_goals,
    t.career_assists,
    t.career_points,
    t.career_pp_goals,
    t.career_gwg,

    -- per-game rates
    round(t.career_goals::numeric   / nullif(t.career_games, 0), 3) as goals_per_game,
    round(t.career_assists::numeric / nullif(t.career_games, 0), 3) as assists_per_game,
    round(t.career_points::numeric  / nullif(t.career_games, 0), 3) as points_per_game,

    -- trophies
    tr.hart_trophies,
    tr.art_ross_trophies,
    c.stanley_cups

from career_totals t
cross join trophy_counts tr
cross join cup_count c
