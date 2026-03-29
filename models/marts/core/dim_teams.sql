-- dim_teams.sql
-- Team dimension. One row per NHL franchise.
-- Join to season facts on team_name to enrich with conference, division,
-- and brand colors for the website.

with teams as (
    select * from {{ ref('stg_teams') }}
)

select
    team_abbr,
    team_name,
    city,
    conference,
    division,
    primary_color,
    secondary_color,
    city || ' ' || team_name        as full_team_name

from teams
