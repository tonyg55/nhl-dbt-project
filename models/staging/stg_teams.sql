-- stg_teams.sql
-- Cleans the teams seed. One row per NHL franchise.
-- Used to build dim_teams.

with source as (
    select * from {{ ref('teams') }}
)

select
    team_abbr,
    team_name,
    city,
    conference,
    division,
    primary_color,
    secondary_color

from source
where team_abbr is not null
