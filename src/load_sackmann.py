from __future__ import annotations
import os
from pathlib import Path
import argparse
import sys
import io
import pandas as pd
from sqlalchemy import create_engine

# Configuration
DEFAULT_DATA_DIR = Path("data/raw/atp")
PLAYERS_FILE = "atp_players.csv"
RANKING_FILES = ["atp_rankings_10s.csv", "atp_rankings_20s.csv", "atp_rankings_current.csv"]

DEFAULT_YEAR_START = 2015
DEFAULT_YEAR_END = 2024

MATCH_COLS = [
    "tourney_id","tourney_name","surface","draw_size","tourney_level","tourney_date",
    "match_num","winner_id","winner_name","winner_hand","winner_ht","winner_ioc",
    "loser_id","loser_name","loser_hand","loser_ht","loser_ioc","score","best_of",
    "round","minutes","w_ace","w_df","w_svpt","w_1stIn","w_1stWon","w_2ndWon",
    "w_SvGms","w_bpSaved","w_bpFaced","l_ace","l_df","l_svpt","l_1stIn","l_1stWon",
    "l_2ndWon","l_SvGms","l_bpSaved","l_bpFaced"
]

# numeric columns in schema.sql
MATCH_INT_COLS = [
    "draw_size","match_num","winner_id","winner_ht","loser_id","loser_ht",
    "best_of","minutes","w_ace","w_df","w_svpt","w_1stIn","w_1stWon","w_2ndWon",
    "w_SvGms","w_bpSaved","w_bpFaced","l_ace","l_df","l_svpt","l_1stIn",
    "l_1stWon","l_2ndWon","l_SvGms","l_bpSaved","l_bpFaced"
]

# ---------- DB helpers ----------
def get_engine():
    pg_url = os.getenv("PG_URL")
    if not pg_url:
        print("ERROR: PG_URL env var is not set. Example:", file=sys.stderr)
        print("  export PG_URL=postgresql+psycopg2://tennis:tennis@localhost:5432/tennis", file=sys.stderr)
        sys.exit(1)
    return create_engine(pg_url, future=True)

def copy_df(df: pd.DataFrame, full_table: str):
    """
    Fast bulk load via COPY FROM STDIN.
    full_table: e.g., 'atp.players'
    """
    if df.empty:
        return
    # Ensure column order is deterministic
    cols = list(df.columns)
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False, header=False)
    csv_buf.seek(0)
    eng = get_engine()
    with eng.begin() as conn:
        # DATESTYLE ensures 'YYYY-MM-DD' is read as ISO
        conn.exec_driver_sql("SET DATESTYLE TO ISO;")
        raw = conn.connection
        with raw.cursor() as cur:
            cur.copy_expert(f"COPY {full_table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT CSV)", csv_buf)

# ---------- Loaders ----------
def load_players(data_dir: Path):
    """
    Load players from atp_players.csv with robust header detection and flexible column mapping.
    Accepts either:
      - first_name/last_name OR name_first/name_last
      - country_code OR ioc
      - height OR height_cm
    """
    p = data_dir / PLAYERS_FILE
    if not p.exists():
        raise FileNotFoundError(f"Missing file: {p}")

    # Peek first row to decide if there is a header
    peek = pd.read_csv(p, nrows=1)
    has_header = any(
        c in peek.columns
        for c in ["player_id", "first_name", "name_first", "last_name", "name_last"]
    )

    if has_header:
        df = pd.read_csv(p, dtype=str, low_memory=False)
    else:
        # Original Sackmann order: player_id, first_name, last_name, hand, birth_date, country_code, height
        src_cols = ["player_id","first_name","last_name","hand","birth_date","country_code","height"]
        df = pd.read_csv(p, header=None, names=src_cols, dtype=str, low_memory=False)

    # Normalize column names (strip spaces/lowercase for matching)
    norm = {c: c.strip().lower() for c in df.columns}
    df = df.rename(columns=norm)

    # Build a flexible source->target map
    def pick(*cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    col_player_id = pick("player_id")
    col_first     = pick("first_name", "name_first")
    col_last      = pick("last_name", "name_last")
    col_hand      = pick("hand")
    col_birth     = pick("birth_date", "dob")
    col_country   = pick("country_code", "ioc")
    col_height    = pick("height_cm", "height")

    missing = [("player_id", col_player_id), ("first/name", col_first),
               ("last/name", col_last), ("hand", col_hand), ("birth_date", col_birth),
               ("country_code/ioc", col_country)]
    truly_missing = [exp for exp, got in missing if got is None]
    if truly_missing:
        raise ValueError(f"Players CSV is missing required columns: {truly_missing}")

    # Construct output in schema order
    out = pd.DataFrame({
        "player_id": df[col_player_id],
        "name_last": df[col_last],
        "name_first": df[col_first],
        "hand": df[col_hand] if col_hand else None,
        "dob": pd.to_datetime(df[col_birth], format="%Y%m%d", errors="coerce"),
        "ioc": df[col_country],
        "height_cm": pd.to_numeric(df[col_height], errors="coerce").astype("Int64") if col_height else pd.Series([pd.NA]*len(df), dtype="Int64")
    })

    # Coerce player_id to integer (nullable) safely
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")

    # Optional: drop rows without a player_id (header rows that slipped through)
    out = out[out["player_id"].notna()]

    print(f"[players] rows={len(out)} (after cleaning)")
    copy_df(out, "atp.players")

def load_rankings(data_dir: Path, min_date: str | None = None):
    files = ["atp_rankings_10s.csv", "atp_rankings_20s.csv", "atp_rankings_current.csv"]
    frames = []
    for fname in files:
        p = data_dir / fname
        if not p.exists():
            print(f"[rankings] WARNING: missing {p}, skipping")
            continue
        df = pd.read_csv(
            p,
            header=None,
            names=["ranking_date","ranking","player_id","ranking_points"],
            dtype=str,            # read as strings; we'll coerce
            low_memory=False
        )
        df["ranking_date"] = pd.to_datetime(df["ranking_date"], format="%Y%m%d", errors="coerce")
        df.rename(columns={"ranking":"rank","ranking_points":"points"}, inplace=True)
        frames.append(df)

    if not frames:
        print("[rankings] No files found, nothing to load.")
        return

    out = pd.concat(frames, ignore_index=True)

    # optional window
    if min_date:
        out = out[out["ranking_date"] >= pd.to_datetime(min_date)]

    # coerce types
    out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["rank"]      = pd.to_numeric(out["rank"], errors="coerce").astype("Int64")
    out["points"]    = pd.to_numeric(out["points"], errors="coerce").astype("Int64")

    # remove rows missing keys
    out = out.dropna(subset=["ranking_date","player_id"])

    # >>> DEDUPE here <<<
    before = len(out)
    out = (out
           .sort_values(["ranking_date","player_id"])  # stable order
           .drop_duplicates(subset=["ranking_date","player_id"], keep="last"))
    after = len(out)
    print(f"[rankings] rows={after} (deduped {before - after})  "
          f"range=({out['ranking_date'].min()} .. {out['ranking_date'].max()})")

    # bulk copy
    copy_df(out[["ranking_date","rank","player_id","points"]], "atp.rankings")


def _coerce_numeric_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        else:
            df[c] = pd.Series([pd.NA] * len(df), dtype="Int64")
    return df

def load_matches(data_dir: Path, start_year: int, end_year: int):
    total = 0
    for year in range(start_year, end_year + 1):
        p = data_dir / f"atp_matches_{year}.csv"
        if not p.exists():
            print(f"[matches] {year}: missing file, skipping")
            continue
        df = pd.read_csv(p, low_memory=False)

        # Ensure all MATCH_COLS exist; create missing columns as NA
        for col in MATCH_COLS:
            if col not in df.columns:
                df[col] = pd.NA

        # Convert/clean types
        df["tourney_date"] = pd.to_datetime(df["tourney_date"], format="%Y%m%d", errors="coerce")
        df = _coerce_numeric_cols(df, MATCH_INT_COLS)

        # Restrict to schema columns and copy
        out = df.reindex(columns=MATCH_COLS)
        n = len(out)
        total += n
        print(f"[matches] {year}: rows={n}")
        if n:
            copy_df(out, "atp.matches")
    print(f"[matches] total rows loaded: {total}")

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Load ATP CSVs into Postgres.")
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Base folder with ATP CSVs")
    ap.add_argument("--start-year", type=int, default=DEFAULT_YEAR_START)
    ap.add_argument("--end-year", type=int, default=DEFAULT_YEAR_END)
    ap.add_argument("--min-ranking-date", type=str, default=None, help="e.g., 2015-01-01 to limit ranking rows")
    args = ap.parse_args()

    if not args.data_dir.exists():
        print(f"ERROR: data dir not found: {args.data_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Using data dir: {args.data_dir.resolve()}")
    load_players(args.data_dir)
    load_rankings(args.data_dir, min_date=args.min_ranking_date)
    load_matches(args.data_dir, args.start_year, args.end_year)
    print("Done.")

if __name__ == "__main__":
    main()