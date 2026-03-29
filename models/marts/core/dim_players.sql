-- dim_players.sql
-- Player dimension. Descriptive attributes only — no stats or facts.
-- Joins into all fact tables on player_id for display attributes.

with players as (
    select * from {{ ref('stg_players') }}
)

select
    player_id,
    full_name,
    first_name,
    last_name,
    position,
    shoots_catches,
    birth_date,
    birth_city,
    birth_country,
    height_inches,
    weight_lbs,
    headshot_url,
    is_active,

    case position
        when 'C'  then 'Center'
        when 'LW' then 'Left Wing'
        when 'RW' then 'Right Wing'
        when 'D'  then 'Defenseman'
        when 'G'  then 'Goalie'
        else position
    end as position_label,

    case birth_country
        when 'CAN' then 'Canada'
        when 'USA' then 'United States'
        when 'SWE' then 'Sweden'
        when 'FIN' then 'Finland'
        when 'RUS' then 'Russia'
        when 'CZE' then 'Czech Republic'
        when 'SVK' then 'Slovakia'
        else birth_country
    end as birth_country_label

from players
