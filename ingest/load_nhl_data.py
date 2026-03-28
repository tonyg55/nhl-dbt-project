
import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

DB_URL    = os.environ["DATABASE_URL"]
BASE_URL  = "https://api-web.nhle.com/v1"
RAW_SCHEMA = "raw"

# Gretzky's NHL player ID — fixed forever
GRETZKY_ID = 8447400

# Top all-time scorers to pull for comparison context
COMPARISON_PLAYER_IDS = [
    8447400,  # Wayne Gretzky
    8448208,  # Jaromir Jagr
    8449600,  # Mark Messier
    8446720,  # Gordie Howe
    8448676,  # Ron Francis
    8445180,  # Marcel Dionne
    8449474,  # Steve Yzerman
    8445596,  # Phil Esposito
    8452079,  # Teemu Selanne
    8445693,  # Mike Gartner
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


def fetch_player_season_stats(player_id: int) -> list[dict]:
    """Pull season-by-season regular season stats for one player."""
    url = f"{BASE_URL}/player/{player_id}/landing"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for season in data.get("seasonTotals", []):
        if season.get("gameTypeId") != 2:  # 2 = regular season
            continue
        rows.append(
            {
                "player_id":          player_id,
                "player_name":        f"{data['firstName']['default']} {data['lastName']['default']}",
                "position":           data.get("position"),
                "season":             season.get("season"),           # e.g. 19791980
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
    return rows


def ingest_player_season_stats(engine):
    print("Fetching player season stats...")
    all_rows = []
    for pid in COMPARISON_PLAYER_IDS:
        try:
            rows = fetch_player_season_stats(pid)
            all_rows.extend(rows)
            print(f"  ✓ player {pid} — {len(rows)} seasons")
        except Exception as e:
            print(f"  ✗ player {pid} failed: {e}")

    df = pd.DataFrame(all_rows)
    load_table(engine, df, "player_season_stats")


def main():
    engine = get_engine()
    ensure_schema(engine)
    ingest_player_season_stats(engine)
    print("\nIngestion complete. Run `dbt seed` then `dbt build` next.")


if __name__ == "__main__":
    main()
