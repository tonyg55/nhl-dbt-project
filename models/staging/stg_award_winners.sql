-- stg_award_winners.sql
-- Cleans the award_winners seed.
-- One row per award per season year.

with source as (
    select * from {{ ref('award_winners') }}
)

select
    award_name,
    season_year::int as season_year,
    winner,
    team,
    points_that_season::int as points_that_season

from source
where award_name is not null
