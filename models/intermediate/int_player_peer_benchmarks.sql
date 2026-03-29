-- int_player_peer_benchmarks.sql
-- Season-by-season rankings and dominance margins for all players in the dataset.
-- One row per player per season.
--
-- Answers questions like:
--   How many seasons was Gretzky the #1 scorer in the dataset?
--   When he led, by how many points was he ahead of 2nd place?
--   How many seasons did he score 100+ points?
-- Those aggregations happen in mart_gretzky_case_for_greatest.
-- This model just provides the ranked season rows.

with enriched as (
    select * from {{ ref('int_player_season_enriched') }}
),

ranked as (
    select
        player_id,
        player_name,
        season_year,
        team_name,
        games_played,
        goals,
        assists,
        points,
        points_per_game,

        -- season ranks within dataset
        rank() over (partition by season_year order by points desc) as points_rank,
        rank() over (partition by season_year order by goals desc) as goals_rank,
        rank() over (partition by season_year order by assists desc) as assists_rank,

        -- season leader totals (constant per season, used for margin calculations)
        max(points) over (partition by season_year) as season_top_points,
        max(goals) over (partition by season_year) as season_top_goals,
        max(assists) over (partition by season_year) as season_top_assists,

        -- each player's gap to the season points leader
        max(points) over (partition by season_year) - points as points_behind_season_leader

    from enriched
),

with_margin as (
    select
        *,
        -- margin above 2nd place: only non-zero for the season leader
        -- computed by subtracting this season's 2nd-highest points from the leader
        case
            when points_rank = 1
            then points - (
                select max(r2.points)
                from ranked r2
                where r2.season_year = ranked.season_year
                  and r2.points_rank > 1
            )
            else null
        end as margin_above_2nd_place

    from ranked
)

select * from with_margin
