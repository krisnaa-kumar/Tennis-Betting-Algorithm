#!/usr/bin/env python3
from __future__ import annotations
import os, io, sys, argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

PG_URL = os.getenv("PG_URL")
if not PG_URL:
    print("Set PG_URL, e.g. export PG_URL=postgresql+psycopg2://tennis:tennis@localhost:5432/tennis", file=sys.stderr)
    sys.exit(1)
ENG = create_engine(PG_URL, future=True)

TML_COLS = [
    "tourney_id","tourney_name","surface","draw_size","tourney_level","tourney_date","match_num",
    "winner_id","winner_seed","winner_entry","winner_name","winner_hand","winner_ht","winner_ioc",
    "winner_age","winner_rank","winner_rank_points",
    "loser_id","loser_seed","loser_entry","loser_name","loser_hand","loser_ht","loser_ioc",
    "loser_age","loser_rank","loser_rank_points",
    "score","best_of","round","minutes",
    "w_ace","w_df","w_svpt","w_1stIn","w_1stWon","w_2ndWon","w_SvGms","w_bpSaved","w_bpFaced",
    "l_ace","l_df","l_svpt","l_1stIn","l_1stWon","l_2ndWon","l_SvGms","l_bpSaved","l_bpFaced"
]
TARGET_COLS = TML_COLS[:]

# TML IDs are alphanumeric -> keep as strings
INTS = [
    "draw_size","match_num",
    "winner_seed","winner_ht","winner_rank","winner_rank_points",
    "loser_seed","loser_ht","loser_rank","loser_rank_points",
    "best_of","minutes","w_ace","w_df","w_svpt","w_1stIn","w_1stWon","w_2ndWon","w_SvGms","w_bpSaved","w_bpFaced",
    "l_ace","l_df","l_svpt","l_1stIn","l_1stWon","l_2ndWon","l_SvGms","l_bpSaved","l_bpFaced"
]
FLOATS = ["winner_age","loser_age"]
STRS = [
    "tourney_id","tourney_name","surface","tourney_level","round","score",
    "winner_id","winner_entry","winner_name","winner_hand","winner_ioc",
    "loser_id","loser_entry","loser_name","loser_hand","loser_ioc"
]

def read_file(fp: Path) -> pd.DataFrame:
    if fp.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(fp, engine="openpyxl", dtype=str)
    else:
        df = pd.read_csv(fp, low_memory=False, dtype=str)

    # normalize headers to expected names
    import re
    def canon(s: str) -> str:
        return re.sub(r"[^a-z0-9]+","_", str(s).strip().lower()).strip("_")

    norm = {canon(c): c for c in df.columns}
    expect = {canon(c): c for c in TML_COLS}
    missing = [c for c in expect if c not in norm]
    if missing:
        # best-effort: rename if canonical names match
        rename_map = {norm[k]: expect[k] for k in expect if k in norm}
        if rename_map:
            df = df.rename(columns=rename_map)
            norm = {canon(c): c for c in df.columns}
            missing = [c for c in expect if c not in norm]
    if missing:
        raise ValueError(f"{fp.name}: missing columns (first few): {missing[:6]}")

    # keep only expected cols; type coercions
    df = df[[c for c in TML_COLS if c in df.columns]].copy()

    # dates: try YYYYMMDD, then flexible fallback
    d1 = pd.to_datetime(df["tourney_date"], format="%Y%m%d", errors="coerce")
    mask = d1.isna()
    if mask.any():
        d2 = pd.to_datetime(df.loc[mask, "tourney_date"], errors="coerce")
        d1.loc[mask] = d2
    df["tourney_date"] = d1

    for c in INTS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in FLOATS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in STRS:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()

    for c in TARGET_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    return df.reindex(columns=TARGET_COLS)

def upsert_matches(df: pd.DataFrame):
    if df.empty:
        return

    # IMPORTANT: your choice was to SKIP rows with NULL match_num to keep idempotent upserts
    key_mask = df["tourney_id"].notna() & df["tourney_date"].notna() & df["match_num"].notna()
    dropped = len(df) - int(key_mask.sum())
    if dropped:
        print(f"[tml] dropping {dropped} rows with NULL match_num or missing tourney keys")
    df = df[key_mask].copy()

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols = ", ".join(TARGET_COLS)
    updates = ", ".join(
        f"{c}=EXCLUDED.{c}"
        for c in TARGET_COLS
        if c not in ("tourney_id","tourney_date","match_num")
    )

    with ENG.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            cur.execute("CREATE TEMP TABLE tmp_tml (LIKE atp.matches INCLUDING ALL) ON COMMIT DROP;")
            cur.copy_expert(f"COPY tmp_tml ({cols}) FROM STDIN WITH (FORMAT CSV)", buf)
            cur.execute(f"""
                INSERT INTO atp.matches ({cols})
                SELECT {cols} FROM tmp_tml
                ON CONFLICT (tourney_id, tourney_date, match_num)
                DO UPDATE SET {updates};
            """)

def load_players_ref(csv_path: Path):
    # TML ATP_Database.csv
    df = pd.read_csv(csv_path, encoding="latin1", dtype=str)

    need = ["id","player","atpname","ioc","hand","backhand","height","birthdate"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"{csv_path.name}: missing column {c}")

    out = pd.DataFrame({
        "player_id": df["id"].astype(str).str.strip(),
        "player": df["player"].astype(str).str.strip(),
        "atpname": df["atpname"].astype(str).str.strip(),
        "ioc": df["ioc"].astype(str).str.strip(),
        "hand": df["hand"].astype(str).str.strip(),
        "backhand": df["backhand"].astype(str).str.strip(),
        "height_cm": pd.to_numeric(df["height"], errors="coerce").astype("Int64"),
        "birthdate": pd.to_datetime(df["birthdate"], format="%Y%m%d", errors="coerce")
    })

    # ensure base table exists
    create_sql = """
    CREATE TABLE IF NOT EXISTS atp.players_ref (
      player_id TEXT PRIMARY KEY,
      player    TEXT,
      atpname   TEXT,
      ioc       TEXT,
      hand      TEXT,
      backhand  TEXT,
      height_cm INTEGER,
      birthdate DATE
    );"""
    with ENG.begin() as conn:
        conn.exec_driver_sql(create_sql)

    # ✅ defensive cleanup to avoid dup keys in staging
    out = out[out["player_id"].notna()]
    out = out[out["player_id"].str.len() > 0]
    out = out.drop_duplicates(subset=["player_id"], keep="first")

    # stage -> temp table WITHOUT constraints (so COPY can't fail on PK)
    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols = ", ".join(out.columns)

    with ENG.begin() as conn:
        raw = conn.connection
        with raw.cursor() as cur:
            # 👇 create temp table with same columns but NO constraints
            cur.execute("CREATE TEMP TABLE tmp_players_ref AS SELECT * FROM atp.players_ref WITH NO DATA;")
            cur.copy_expert(f"COPY tmp_players_ref ({cols}) FROM STDIN WITH (FORMAT CSV)", buf)
            cur.execute(f"""
                INSERT INTO atp.players_ref ({cols})
                SELECT {cols} FROM tmp_players_ref
                ON CONFLICT (player_id) DO UPDATE SET
                  player=EXCLUDED.player,
                  atpname=EXCLUDED.atpname,
                  ioc=EXCLUDED.ioc,
                  hand=EXCLUDED.hand,
                  backhand=EXCLUDED.backhand,
                  height_cm=EXCLUDED.height_cm,
                  birthdate=EXCLUDED.birthdate;
            """)
    print(f"[players_ref] rows={len(out)} loaded")



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder", type=Path, default=Path("data/raw/atp"))
    ap.add_argument("--patterns", nargs="*", default=["*.xlsx","*.csv"])
    ap.add_argument("--players-file", type=Path, help="Path to TML ATP_Database.csv")
    args = ap.parse_args()

    # Optionally load players reference
    if args.players_file and args.players_file.exists():
        load_players_ref(args.players_file)

    # Gather match files, excluding the players file if it lives in the same folder
    files: list[Path] = []
    for pat in args.patterns:
        files.extend(sorted(args.folder.glob(pat)))
    # keep only .xlsx/.xls/.csv
    files = [p for p in files if p.suffix.lower() in (".xlsx",".xls",".csv")]
    # drop the players file if present in the list
    if args.players_file:
        try:
            pf = args.players_file.resolve()
            files = [p for p in files if p.resolve() != pf]
        except FileNotFoundError:
            pass  # players file path may be outside folder; ignore

    if not files:
        print(f"No match files found under {args.folder}", file=sys.stderr)
        sys.exit(1)

    total = 0
    for fp in files:
        df = read_file(fp)
        print(f"[tml] {fp.name}: rows={len(df)}")
        upsert_matches(df)
        total += len(df)
    print(f"[tml] total={total}")

if __name__ == "__main__":
    main()




