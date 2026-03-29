# NHL Analytics — Just How Dominant Was Wayne Gretzky?

A focused dbt portfolio project that showcases being able to tell a story with data that runs in a production environment and quantifies Wayne Gretzky's statistical dominance over his 20-season NHL career. 

Output here: https://lightthelamp.ai/

## The question this project answers

> How dominant was Wayne Gretzky — exactly?

The short answer: **Gretzky's career assists alone (1,963) exceed the total career point totals of every other player in NHL history.** This project surfaces that and every other dimension of his dominance in analytics-ready tables for website consumption.

## What the website will display

From `marts.fct_gretzky_career` (single row — his career summary):

| Metric | Value |
|---|---|
| Career Points | 2,857 |
| Career Goals | 894 |
| Career Assists | 1,963 |
| Points Per Game | 1.921 |
| Goals Per Game | 0.601 |
| Assists Per Game | 1.320 |
| Hart Trophies (MVP) | 9 |
| Art Ross Trophies (scoring title) | 10 |
| Stanley Cups | 4 |

From `marts.fct_gretzky_dominance` (one row per player — leaderboard):
- All-time points ranking with `points_behind_gretzky`
- `assists_only_gap` — Gretzky's assists vs. each player's career points total

## Architecture

```
NHL web API (nhl-api-py)          seeds/ (static CSV)
        │                              │
        ▼                              ▼
  raw.player_season_stats    raw.award_winners
                             raw.stanley_cup_champions
        │                              │
        └──────────────┬───────────────┘
                       ▼  dbt
               staging.* (views)
                       │
                       ▼
                  marts.* (tables)
                 ┌──────────────────────────┐
                 │ fct_gretzky_career       │  ← career card
                 │ fct_gretzky_dominance    │  ← leaderboard
                 └──────────────────────────┘
```

No intermediate layer — the data is simple enough that staging → marts is sufficient.

## Setup

### 1. Database
Create a free PostgreSQL instance on [Neon](https://neon.tech) or [Supabase](https://supabase.com).

### 2. Ingest player stats

```bash
cd ingest
pip install -r requirements.txt
cp ../.env.example ../.env   # fill in DATABASE_URL
python load_nhl_data.py
```

### 3. Configure dbt

Copy `profiles.yml` to `~/.dbt/profiles.yml` and set environment variables:

```bash
export DBT_HOST=your-neon-host
export DBT_USER=your-user
export DBT_PASSWORD=your-password
export DBT_DBNAME=nhl_db
```

Or connect via **dbt Cloud** — add a PostgreSQL connection under Project Settings.

### 4. Build

```bash
dbt deps           # install dbt_utils
dbt seed           # load award_winners + stanley_cup_champions CSVs
dbt build          # run models + tests
dbt docs generate  # generate lineage docs
dbt docs serve     # open in browser
```

## Data sources

- **Player stats**: [NHL web API](https://api-web.nhle.com) via `nhl-api-py`
- **Award winners**: Static CSV seed (Hart Trophy, Art Ross Trophy 1980–1999)
- **Stanley Cup champions**: Static CSV seed (1980–1999)
