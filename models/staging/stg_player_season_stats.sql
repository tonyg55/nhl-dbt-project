-- stg_player_season_stats.sql
-- Cleans and types the raw API player season stats.
-- One row per player per season (regular season only).

with source as (
    select * from {{ source('raw', 'player_season_stats') }}
),

renamed as (
    select
        player_id::bigint as player_id,
        player_name,
        position,
        -- convert 8-digit season code (19791980) to a readable end-year integer (1980)
        cast(right(season::text, 4) as int) as season_year,
        season::text as season_code,
        team_name,
        coalesce(games_played, 0)::int as games_played,
        coalesce(goals, 0)::int as goals,
        coalesce(assists, 0)::int as assists,
        coalesce(points, 0)::int as points,
        plus_minus::int as plus_minus,
        coalesce(penalty_minutes, 0)::int as penalty_minutes,
        coalesce(power_play_goals, 0)::int as power_play_goals,
        coalesce(power_play_points, 0)::int as power_play_points,
        coalesce(short_handed_goals, 0)::int as short_handed_goals,
        coalesce(game_winning_goals, 0)::int as game_winning_goals,
        coalesce(shots, 0)::int as shots,
        shooting_pct::numeric(5, 2) as shooting_pct

    from source
    where player_id is not null
      and games_played > 0
)

select * from renamed
