-- db/features_plus.sql
-- Adds round importance, bio features (with robust height), and serve/return split history.
-- Also fixes opp-elo rolling by centering around 1500.
-- Requires: processed.sql, elo.sql, features.sql already applied.

CREATE SCHEMA IF NOT EXISTS processed;

-- Recreate only what we own
DROP MATERIALIZED VIEW IF EXISTS processed.match_training;
DROP MATERIALIZED VIEW IF EXISTS processed.recent_context;
DROP MATERIALIZED VIEW IF EXISTS processed.opp_elo_roll;
DROP MATERIALIZED VIEW IF EXISTS processed.player_bio;
DROP MATERIALIZED VIEW IF EXISTS processed.sr_roll;

-- 1) Round -> stage mapping (idempotent)
CREATE OR REPLACE FUNCTION processed.round_stage(r TEXT)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT CASE UPPER(COALESCE(r,''))
    WHEN 'R128' THEN 1
    WHEN 'R64'  THEN 2
    WHEN 'R32'  THEN 3
    WHEN 'R16'  THEN 4
    WHEN 'QF'   THEN 5
    WHEN 'SF'   THEN 6
    WHEN 'F'    THEN 7
    WHEN 'RR'   THEN 4   -- Round Robin ~ mid-stage
    WHEN 'Q1'   THEN 0
    WHEN 'Q2'   THEN 0
    WHEN 'Q3'   THEN 0
    ELSE 3              -- default ~R32
  END;
$$;

-- 2) Recent context: same surface as the immediately previous match?
CREATE MATERIALIZED VIEW processed.recent_context AS
SELECT
  b.match_id,
  b.player_id,
  CASE
    WHEN LAG(b.surface) OVER (PARTITION BY b.player_id ORDER BY b.tourney_date, b.match_id) = b.surface
    THEN 1 ELSE 0
  END AS same_surface_prev
FROM processed.player_base b;

CREATE UNIQUE INDEX IF NOT EXISTS uq_recent_context_match_player
  ON processed.recent_context(match_id, player_id);

-- 3) Opponent strength: rolling avg opponent Elo over last 10 matches (pre-match), centered
CREATE MATERIALIZED VIEW processed.opp_elo_roll AS
WITH base AS (
  SELECT
    b.player_id,
    b.match_id,
    b.tourney_date,
    ep.elo_global_pre AS opp_elo_pre
  FROM processed.player_base b
  LEFT JOIN processed.elo_prematch ep
    ON ep.match_id = b.match_id
   AND ep.player_id = b.opponent_id
),
roll AS (
  SELECT
    match_id,
    player_id,
    AVG(opp_elo_pre) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS opp_elo10
  FROM base
)
SELECT
  match_id,
  player_id,
  (opp_elo10 - 1500) AS opp_elo10_centered
FROM roll;

CREATE UNIQUE INDEX IF NOT EXISTS uq_opp_elo_roll_match_player
  ON processed.opp_elo_roll(match_id, player_id);

-- 4) Bio snapshot from players_ref (TEXT ids) with robust height
CREATE MATERIALIZED VIEW processed.player_bio AS
SELECT
  pr.player_id::text                                   AS player_id,
  NULLIF(TRIM(pr.hand),'')                             AS hand,
  CASE WHEN pr.height_cm BETWEEN 150 AND 215 THEN pr.height_cm END AS height_cm_clean,
  COALESCE(                                             -- centered fallback
    CASE WHEN pr.height_cm BETWEEN 150 AND 215 THEN pr.height_cm END,
    185
  )                                                    AS height_c
FROM atp.players_ref pr;

CREATE UNIQUE INDEX IF NOT EXISTS uq_player_bio_id
  ON processed.player_bio(player_id);

-- 5) Serve/Return split history: per-match rates -> rolling (10) overall & surface
CREATE MATERIALIZED VIEW processed.sr_roll AS
WITH per_match AS (
  SELECT
    b.player_id,
    b.match_id,
    b.tourney_date,
    b.surface,

    -- serve-side
    CASE WHEN b.player_svpt > 0
         THEN b.player_1stIn::float / b.player_svpt END                              AS fs_in_pct,
    CASE WHEN b.player_1stIn > 0
         THEN b.player_1stWon::float / b.player_1stIn END                            AS fs_won_pct,
    CASE WHEN (b.player_svpt - b.player_1stIn) > 0
         THEN b.player_2ndWon::float / NULLIF(b.player_svpt - b.player_1stIn,0) END  AS ss_won_pct,
    CASE WHEN b.player_bpFaced > 0
         THEN b.player_bpSaved::float / b.player_bpFaced END                         AS bp_save_pct,

    -- return-side (via opponent's serve)
    CASE WHEN b.opp_1stIn > 0
         THEN 1.0 - (b.opp_1stWon::float / b.opp_1stIn) END                          AS r_fs_won_pct,
    CASE WHEN (b.opp_svpt - b.opp_1stIn) > 0
         THEN 1.0 - (b.opp_2ndWon::float / NULLIF(b.opp_svpt - b.opp_1stIn,0)) END   AS r_ss_won_pct,
    CASE WHEN b.opp_bpFaced > 0
         THEN (b.opp_bpFaced - b.opp_bpSaved)::float / b.opp_bpFaced END             AS r_bp_conv_pct

  FROM processed.player_base b
),
roll AS (
  SELECT
    pm.*,

    -- overall last-10 (exclude current)
    AVG(fs_in_pct)   OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS fs_in10,
    AVG(fs_won_pct)  OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS fs_won10,
    AVG(ss_won_pct)  OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS ss_won10,
    AVG(bp_save_pct) OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS bp_save10,

    AVG(r_fs_won_pct)  OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS r_fs_won10,
    AVG(r_ss_won_pct)  OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS r_ss_won10,
    AVG(r_bp_conv_pct) OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS r_bp_conv10,

    -- same-surface last-10
    AVG(fs_in_pct)   OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS fs_in10_surface,
    AVG(fs_won_pct)  OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS fs_won10_surface,
    AVG(ss_won_pct)  OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS ss_won10_surface,
    AVG(bp_save_pct) OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS bp_save10_surface,

    AVG(r_fs_won_pct)  OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS r_fs_won10_surface,
    AVG(r_ss_won_pct)  OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS r_ss_won10_surface,
    AVG(r_bp_conv_pct) OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS r_bp_conv10_surface

  FROM per_match pm
)
SELECT match_id, player_id,
       fs_in10, fs_won10, ss_won10, bp_save10,
       r_fs_won10, r_ss_won10, r_bp_conv10,
       fs_in10_surface, fs_won10_surface, ss_won10_surface, bp_save10_surface,
       r_fs_won10_surface, r_ss_won10_surface, r_bp_conv10_surface
FROM roll;

CREATE UNIQUE INDEX IF NOT EXISTS uq_sr_roll_match_player
  ON processed.sr_roll(match_id, player_id);

-- 6) Final training table
CREATE MATERIALIZED VIEW processed.match_training AS
WITH sides AS (
  SELECT
    m.match_id, m.tourney_date, m.surface, m.tourney_level, m.round, m.best_of,
    m.winner_id, m.loser_id,
    LEAST(m.winner_id, m.loser_id)     AS p1_id,
    GREATEST(m.winner_id, m.loser_id)  AS p2_id
  FROM atp.matches m
  WHERE m.winner_id IS NOT NULL AND m.loser_id IS NOT NULL
),
p1 AS (
  SELECT DISTINCT ON (match_id, player_id)
    ph.match_id, ph.player_id,
    ph.matches_last_14d, ph.matches_last_30d, ph.days_since_last,
    ph.winrate_5, ph.winrate_10, ph.spw_10, ph.rpw_10, ph.aces_pg_10, ph.df_pg_10,
    ph.winrate_10_surface, ph.spw_10_surface, ph.rpw_10_surface,
    ph.player_rank, ph.player_rank_points, ph.player_seed, ph.player_age,
    ph.elo_global_pre, ph.elo_surface_pre
  FROM processed.player_history ph
  ORDER BY match_id, player_id
),
p2 AS (
  SELECT DISTINCT ON (match_id, player_id)
    ph.match_id, ph.player_id,
    ph.matches_last_14d, ph.matches_last_30d, ph.days_since_last,
    ph.winrate_5, ph.winrate_10, ph.spw_10, ph.rpw_10, ph.aces_pg_10, ph.df_pg_10,
    ph.winrate_10_surface, ph.spw_10_surface, ph.rpw_10_surface,
    ph.player_rank, ph.player_rank_points, ph.player_seed, ph.player_age,
    ph.elo_global_pre, ph.elo_surface_pre
  FROM processed.player_history ph
  ORDER BY match_id, player_id
),
-- H2H (from features.sql)
h2h1 AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.h2h_prematch ORDER BY match_id, player_id),
h2h2 AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.h2h_prematch ORDER BY match_id, player_id),
-- Tournament form (from features.sql)
tf1  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.tourn_form   ORDER BY match_id, player_id),
tf2  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.tourn_form   ORDER BY match_id, player_id),
-- Helpers from this file
rc1  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.recent_context ORDER BY match_id, player_id),
rc2  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.recent_context ORDER BY match_id, player_id),
oe1  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.opp_elo_roll   ORDER BY match_id, player_id),
oe2  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.opp_elo_roll   ORDER BY match_id, player_id),
pb1  AS (SELECT DISTINCT ON (player_id) player_id, hand, height_cm_clean, height_c FROM processed.player_bio ORDER BY player_id),
pb2  AS (SELECT DISTINCT ON (player_id) player_id, hand, height_cm_clean, height_c FROM processed.player_bio ORDER BY player_id),
sr1  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.sr_roll       ORDER BY match_id, player_id),
sr2  AS (SELECT DISTINCT ON (match_id, player_id) * FROM processed.sr_roll       ORDER BY match_id, player_id)

SELECT
  s.match_id, s.tourney_date, s.surface, s.tourney_level, s.round, s.best_of,
  processed.round_stage(s.round) AS round_stage,
  s.p1_id, s.p2_id,
  CASE WHEN s.winner_id = s.p1_id THEN 1 ELSE 0 END AS y,

  -- base diffs
  (p1.player_rank - p2.player_rank)               AS rank_diff,
  (p1.player_rank_points - p2.player_rank_points) AS rank_points_diff,
  (p1.player_seed - p2.player_seed)               AS seed_diff,
  (p1.player_age - p2.player_age)                 AS age_diff,
  (p1.matches_last_14d - p2.matches_last_14d)     AS matches_last_14d_diff,
  (p1.matches_last_30d - p2.matches_last_30d)     AS matches_last_30d_diff,
  (p1.days_since_last - p2.days_since_last)       AS rest_days_diff,
  (p1.winrate_5 - p2.winrate_5)                   AS winrate5_diff,
  (p1.winrate_10 - p2.winrate_10)                 AS winrate10_diff,
  (p1.spw_10 - p2.spw_10)                         AS spw10_diff,
  (p1.rpw_10 - p2.rpw_10)                         AS rpw10_diff,
  (p1.aces_pg_10 - p2.aces_pg_10)                 AS aces_pg10_diff,
  (p1.df_pg_10 - p2.df_pg_10)                     AS df_pg10_diff,
  (p1.winrate_10_surface - p2.winrate_10_surface) AS winrate10_surface_diff,
  (p1.spw_10_surface - p2.spw_10_surface)         AS spw10_surface_diff,
  (p1.rpw_10_surface - p2.rpw_10_surface)         AS rpw10_surface_diff,

  -- Elo diffs
  (p1.elo_global_pre  - p2.elo_global_pre)        AS elo_diff,
  (p1.elo_surface_pre - p2.elo_surface_pre)       AS elo_surface_diff,

  -- H2H diffs
  (COALESCE(h2h1.h2h_played_prior,0) - COALESCE(h2h2.h2h_played_prior,0)) AS h2h_played_diff,
  (COALESCE(h2h1.h2h_wins_prior,0)   - COALESCE(h2h2.h2h_wins_prior,0))   AS h2h_wins_diff,
  (COALESCE(h2h1.h2h_wins_prior::float/NULLIF(h2h1.h2h_played_prior,0),0)
   - COALESCE(h2h2.h2h_wins_prior::float/NULLIF(h2h2.h2h_played_prior,0),0)) AS h2h_winrate_diff,

  -- Tournament-form diffs (overall + surface)
  (COALESCE(tf1.wins_last5_tourn,0)      - COALESCE(tf2.wins_last5_tourn,0))      AS tourn_wins_last5_diff,
  (COALESCE(tf1.avg_wins_last5_tourn,0)  - COALESCE(tf2.avg_wins_last5_tourn,0))  AS tourn_avg_wins_last5_diff,
  (COALESCE(tf1.tourn_played_prev5,0)    - COALESCE(tf2.tourn_played_prev5,0))    AS tourn_played_prev5_diff,
  (COALESCE(tf1.wins_last5_tourn_surface,0)     - COALESCE(tf2.wins_last5_tourn_surface,0))     AS s_tourn_wins_last5_diff,
  (COALESCE(tf1.avg_wins_last5_tourn_surface,0) - COALESCE(tf2.avg_wins_last5_tourn_surface,0)) AS s_tourn_avg_wins_last5_diff,
  (COALESCE(tf1.tourn_played_prev5_surface,0)   - COALESCE(tf2.tourn_played_prev5_surface,0))   AS s_tourn_played_prev5_diff,

  -- NEW: surface continuity & centered opp-elo history
  (COALESCE(rc1.same_surface_prev,0) - COALESCE(rc2.same_surface_prev,0)) AS same_surface_prev_diff,
  (COALESCE(oe1.opp_elo10_centered,0) - COALESCE(oe2.opp_elo10_centered,0)) AS opp_elo10_diff,

  -- NEW: bio features (robust height)
  (pb1.height_c - pb2.height_c) AS height_diff,
  ((COALESCE(pb1.hand,'') LIKE 'L%')::int - (COALESCE(pb2.hand,'') LIKE 'L%')::int) AS lefty_matchup_diff,
  CASE WHEN (COALESCE(pb1.hand,'') LIKE 'L%') <> (COALESCE(pb2.hand,'') LIKE 'L%') THEN 1 ELSE 0 END AS lefty_vs_righty,

  -- NEW: serve/return split history (overall last-10)
  (COALESCE(sr1.fs_in10,0)   - COALESCE(sr2.fs_in10,0))   AS fs_in10_diff,
  (COALESCE(sr1.fs_won10,0)  - COALESCE(sr2.fs_won10,0))  AS fs_won10_diff,
  (COALESCE(sr1.ss_won10,0)  - COALESCE(sr2.ss_won10,0))  AS ss_won10_diff,
  (COALESCE(sr1.bp_save10,0) - COALESCE(sr2.bp_save10,0)) AS bp_save10_diff,

  (COALESCE(sr1.r_fs_won10,0)  - COALESCE(sr2.r_fs_won10,0))  AS r_fs_won10_diff,
  (COALESCE(sr1.r_ss_won10,0)  - COALESCE(sr2.r_ss_won10,0))  AS r_ss_won10_diff,
  (COALESCE(sr1.r_bp_conv10,0) - COALESCE(sr2.r_bp_conv10,0)) AS r_bp_conv10_diff,

  -- NEW: serve/return split history (same-surface last-10)
  (COALESCE(sr1.fs_in10_surface,0)   - COALESCE(sr2.fs_in10_surface,0))   AS fs_in10_surface_diff,
  (COALESCE(sr1.fs_won10_surface,0)  - COALESCE(sr2.fs_won10_surface,0))  AS fs_won10_surface_diff,
  (COALESCE(sr1.ss_won10_surface,0)  - COALESCE(sr2.ss_won10_surface,0))  AS ss_won10_surface_diff,
  (COALESCE(sr1.bp_save10_surface,0) - COALESCE(sr2.bp_save10_surface,0)) AS bp_save10_surface_diff,

  (COALESCE(sr1.r_fs_won10_surface,0)  - COALESCE(sr2.r_fs_won10_surface,0))  AS r_fs_won10_surface_diff,
  (COALESCE(sr1.r_ss_won10_surface,0)  - COALESCE(sr2.r_ss_won10_surface,0))  AS r_ss_won10_surface_diff,
  (COALESCE(sr1.r_bp_conv10_surface,0) - COALESCE(sr2.r_bp_conv10_surface,0)) AS r_bp_conv10_surface_diff

FROM sides s
JOIN p1  ON p1.match_id = s.match_id AND p1.player_id = s.p1_id
JOIN p2  ON p2.match_id = s.match_id AND p2.player_id = s.p2_id
LEFT JOIN h2h1 ON h2h1.match_id = s.match_id AND h2h1.player_id = s.p1_id
LEFT JOIN h2h2 ON h2h2.match_id = s.match_id AND h2h2.player_id = s.p2_id
LEFT JOIN tf1  ON tf1.match_id  = s.match_id AND tf1.player_id  = s.p1_id
LEFT JOIN tf2  ON tf2.match_id  = s.match_id AND tf2.player_id  = s.p2_id
LEFT JOIN rc1  ON rc1.match_id  = s.match_id AND rc1.player_id  = s.p1_id
LEFT JOIN rc2  ON rc2.match_id  = s.match_id AND rc2.player_id  = s.p2_id
LEFT JOIN oe1  ON oe1.match_id  = s.match_id AND oe1.player_id  = s.p1_id
LEFT JOIN oe2  ON oe2.match_id  = s.match_id AND oe2.player_id  = s.p2_id
LEFT JOIN pb1  ON pb1.player_id = s.p1_id
LEFT JOIN pb2  ON pb2.player_id = s.p2_id
LEFT JOIN sr1  ON sr1.match_id  = s.match_id AND sr1.player_id  = s.p1_id
LEFT JOIN sr2  ON sr2.match_id  = s.match_id AND sr2.player_id  = s.p2_id;

CREATE INDEX IF NOT EXISTS idx_match_training_date ON processed.match_training (tourney_date);

