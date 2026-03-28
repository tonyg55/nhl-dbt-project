-- stg_stanley_cup_champions.sql
-- Cleans the stanley_cup_champions seed.
-- One row per season year.

with source as (
    select * from {{ ref('stanley_cup_champions') }}
)

select
    season_year::int                as season_year,
    champion,
    gretzky_on_roster::boolean      as gretzky_on_roster

from source
where season_year is not null
