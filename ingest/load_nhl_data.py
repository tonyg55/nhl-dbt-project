
import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

DB_URL    = os.environ["DATABASE_URL"]
BASE_URL  = "https://api-web.nhle.com/v1"
RAW_SCHEMA = "raw"

# Players to pull for the Gretzky narrative
COMPARISON_PLAYER_IDS = [
    8447400,  # Wayne Gretzky
    8448208,  # Jaromir Jagr
    8449600,  # Mark Messier
    8446720,  # Gordie Howe
    8449474,  # Steve Yzerman
    8447935,  # Mario Lemieux
    8448567,  # Joe Sakic
    8471675,  # Sidney Crosby
    8474141,  # Patrick Kane
    8446404,  # Bobby Orr
    8449048,  # Pavel Bure
]


def get_engine():
    return create_engine(DB_URL)


def ensure_schema(engine):
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
        conn.commit()


def load_table(engine, df: pd.DataFrame, table_name: str):
    print(f"  Loading {len(df):,} rows → {RAW_SCHEMA}.{table_name}")
    df.columns = [c.lower() for c in df.columns]
    df.to_sql(table_name, engine, schema=RAW_SCHEMA, if_exists="replace", index=False)


def fetch_player_data(player_id: int) -> tuple[dict, list[dict]]:
    """
    Fetch one player from the NHL API landing endpoint.
    Returns (metadata_row, season_stat_rows).
    Both are derived from the same API call to avoid duplicate requests.
    """
    url = f"{BASE_URL}/player/{player_id}/landing"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    first = data.get("firstName", {}).get("default", "")
    last  = data.get("lastName", {}).get("default", "")
    full_name = f"{first} {last}".strip()

    metadata = {
        "player_id":       player_id,
        "first_name":      first,
        "last_name":       last,
        "full_name":       full_name,
        "position":        data.get("position"),
        "shoots_catches":  data.get("shootsCatches"),
        "birth_date":      data.get("birthDate"),
        "birth_city":      data.get("birthCity", {}).get("default"),
        "birth_country":   data.get("birthCountry"),
        "height_inches":   data.get("heightInInches"),
        "weight_lbs":      data.get("weightInPounds"),
        "headshot_url":    data.get("headshot"),
        "is_active":       data.get("isActive", False),
    }

    season_rows = []
    for season in data.get("seasonTotals", []):
        if season.get("gameTypeId") != 2:  # 2 = regular season only
            continue
        season_rows.append(
            {
                "player_id":          player_id,
                "player_name":        full_name,
                "position":           data.get("position"),
                "season":             season.get("season"),
                "team_name":          season.get("teamName", {}).get("default"),
                "games_played":       season.get("gamesPlayed", 0),
                "goals":              season.get("goals", 0),
                "assists":            season.get("assists", 0),
                "points":             season.get("points", 0),
                "plus_minus":         season.get("plusMinus"),
                "penalty_minutes":    season.get("pim", 0),
                "power_play_goals":   season.get("powerPlayGoals", 0),
                "power_play_points":  season.get("powerPlayPoints", 0),
                "short_handed_goals": season.get("shorthandedGoals", 0),
                "game_winning_goals": season.get("gameWinningGoals", 0),
                "shots":              season.get("shots", 0),
                "shooting_pct":       season.get("shootingPctg"),
            }
        )
    return metadata, season_rows


def ingest_players(engine):
    print("Fetching player data...")
    all_metadata   = []
    all_season_rows = []

    for pid in COMPARISON_PLAYER_IDS:
        try:
            meta, seasons = fetch_player_data(pid)
            all_metadata.append(meta)
            all_season_rows.extend(seasons)
            print(f"  ✓ {meta['full_name']} — {len(seasons)} seasons")
        except Exception as e:
            print(f"  ✗ player {pid} failed: {e}")

    load_table(engine, pd.DataFrame(all_metadata),    "players")
    load_table(engine, pd.DataFrame(all_season_rows), "player_season_stats")


def main():
    engine = get_engine()
    ensure_schema(engine)
    ingest_players(engine)
    print("\nIngestion complete. Run `dbt seed` then `dbt build` next.")


if __name__ == "__main__":
    main()
