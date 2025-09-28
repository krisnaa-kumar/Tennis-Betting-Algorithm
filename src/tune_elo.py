#!/usr/bin/env python3
import os, sys, time, itertools
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

PG_URL = os.environ.get("PG_URL")
if not PG_URL:
    print("Set PG_URL", file=sys.stderr); sys.exit(1)
eng = create_engine(PG_URL, future=True)

def set_ks(k_g, k_f, k_m, k_a, k_b):
    with eng.begin() as con:
        con.execute(text("""
            UPDATE processed.elo_params
            SET k_g=:kg, k_f=:kf, k_m=:km, k_a=:ka, k_b=:kb
            WHERE id=1
        """), dict(kg=k_g, kf=k_f, km=k_m, ka=k_a, kb=k_b))

def rebuild():
    with eng.begin() as con:
        con.execute(text("SELECT processed.rebuild_elo();"))
        con.execute(text("REFRESH MATERIALIZED VIEW processed.player_history;"))
        con.execute(text("REFRESH MATERIALIZED VIEW processed.match_training;"))

def fetch_val_2023():
    q = """
    SELECT y, elo_diff
    FROM processed.match_training
    WHERE tourney_date >= '2023-01-01' AND tourney_date < '2024-01-01'
      AND elo_diff IS NOT NULL
    """
    return pd.read_sql(q, eng)

def elo_prob(d):  # Elo logistic
    return 1.0 / (1.0 + 10.0 ** (-d/400.0))

def logloss(y, p, eps=1e-15):
    p = np.clip(p, eps, 1-eps)
    return float(-(y*np.log(p) + (1-y)*np.log(1-p)).mean())

def main():
    # Small, sensible grid
    G = [14, 16]     # Slams
    F = [18]         # Finals
    M = [18, 20, 22] # Masters
    A = [20, 22, 24] # 500
    B = [22, 24, 26] # 250

    results = []
    for kg, kf, km, ka, kb in itertools.product(G,F,M,A,B):
        print(f"Testing K: G={kg} F={kf} M={km} A={ka} B={kb}")
        set_ks(kg,kf,km,ka,kb)
        rebuild()

        df = fetch_val_2023()
        p = elo_prob(df["elo_diff"].to_numpy())
        ll = logloss(df["y"].to_numpy(), p)
        results.append(((kg,kf,km,ka,kb), ll))
        print(f"  logloss_2023 = {ll:.5f}")

    results.sort(key=lambda x: x[1])
    print("\nBest K by 2023 logloss:")
    for (ks, ll) in results[:5]:
        print(f"K={ks}  logloss={ll:.5f}")

if __name__ == "__main__":
    main()
