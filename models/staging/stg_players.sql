-- stg_players.sql
-- Cleans and types the raw player profile metadata.
-- One row per player. Used to build dim_players.

with source as (
    select * from {{ source('raw', 'players') }}
)

select
    player_id::bigint as player_id,
    first_name,
    last_name,
    full_name,
    position,
    shoots_catches,
    birth_date::date as birth_date,
    birth_city,
    birth_country,
    height_inches::int as height_inches,
    weight_lbs::int as weight_lbs,
    headshot_url,
    is_active::boolean as is_active

from source
where player_id is not null
