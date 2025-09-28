-- db/features.sql
-- Extra pre-match features: Head-to-head and recent tournament form.
-- Run AFTER: processed.player_base, processed.player_history (from elo.sql)

CREATE SCHEMA IF NOT EXISTS processed;

-- 0) Drop downstream first so we can safely recreate upstream views
DROP MATERIALIZED VIEW IF EXISTS processed.match_training;
DROP MATERIALIZED VIEW IF EXISTS processed.h2h_prematch;
DROP MATERIALIZED VIEW IF EXISTS processed.tourn_form;

-- 1) Head-to-head prematch features (per match, per player)
CREATE MATERIALIZED VIEW processed.h2h_prematch AS
WITH seq AS (
  SELECT
    b.match_id,
    b.player_id,
    b.opponent_id,
    b.tourney_date,
    COUNT(*)        OVER (
      PARTITION BY b.player_id, b.opponent_id
      ORDER BY b.tourney_date, b.match_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS h2h_played_prior,
    SUM(b.is_win::int) OVER (
      PARTITION BY b.player_id, b.opponent_id
      ORDER BY b.tourney_date, b.match_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS h2h_wins_prior,
    (b.tourney_date - LAG(b.tourney_date) OVER (
       PARTITION BY b.player_id, b.opponent_id
       ORDER BY b.tourney_date, b.match_id
     ))::int AS days_since_last_h2h
  FROM processed.player_base b
)
SELECT
  match_id,
  player_id,
  opponent_id,
  h2h_played_prior,
  h2h_wins_prior,
  CASE WHEN h2h_played_prior > 0 THEN days_since_last_h2h END AS days_since_last_h2h
FROM seq;

-- One row per (match, player)
CREATE UNIQUE INDEX IF NOT EXISTS uq_h2h_prematch_match_player
  ON processed.h2h_prematch (match_id, player_id);

-- 2) Recent tournament form (last 5 tournaments, pre-current tournament)
CREATE MATERIALIZED VIEW processed.tourn_form AS
WITH tourn AS (
  SELECT
    b.player_id,
    b.tourney_id,
    b.tourney_date,
    b.surface,
    SUM(b.is_win::int) AS wins_in_tourney
  FROM processed.player_base b
  GROUP BY 1,2,3,4
),
roll AS (
  SELECT
    t.*,
    -- last 5 tournaments overall
    SUM(wins_in_tourney) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS wins_last5_tourn,
    AVG(wins_in_tourney) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS avg_wins_last5_tourn,
    COUNT(*) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS tourn_played_prev5,

    -- last 5 tournaments on the same surface
    SUM(wins_in_tourney) OVER (
      PARTITION BY player_id, surface
      ORDER BY tourney_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS wins_last5_tourn_surface,
    AVG(wins_in_tourney) OVER (
      PARTITION BY player_id, surface
      ORDER BY tourney_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS avg_wins_last5_tourn_surface,
    COUNT(*) OVER (
      PARTITION BY player_id, surface
      ORDER BY tourney_date
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS tourn_played_prev5_surface
  FROM tourn t
)
SELECT
  pb.match_id,
  pb.player_id,
  r.wins_last5_tourn,
  r.avg_wins_last5_tourn,
  r.tourn_played_prev5,
  r.wins_last5_tourn_surface,
  r.avg_wins_last5_tourn_surface,
  r.tourn_played_prev5_surface
FROM processed.player_base pb
JOIN roll r
  ON r.player_id    = pb.player_id
 AND r.tourney_id   = pb.tourney_id
 AND r.tourney_date = pb.tourney_date
 AND COALESCE(r.surface,'') = COALESCE(pb.surface,'');

-- One row per (match, player)
CREATE UNIQUE INDEX IF NOT EXISTS uq_tourn_form_match_player
  ON processed.tourn_form (match_id, player_id);

-- 3) Rebuild the enriched training table with new features and Elo diffs
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
h2h1 AS (
  SELECT DISTINCT ON (match_id, player_id)
    match_id, player_id, h2h_played_prior, h2h_wins_prior, days_since_last_h2h
  FROM processed.h2h_prematch
  ORDER BY match_id, player_id
),
h2h2 AS (
  SELECT DISTINCT ON (match_id, player_id)
    match_id, player_id, h2h_played_prior, h2h_wins_prior, days_since_last_h2h
  FROM processed.h2h_prematch
  ORDER BY match_id, player_id
),
tf1 AS (
  SELECT DISTINCT ON (match_id, player_id)
    match_id, player_id,
    wins_last5_tourn, avg_wins_last5_tourn, tourn_played_prev5,
    wins_last5_tourn_surface, avg_wins_last5_tourn_surface, tourn_played_prev5_surface
  FROM processed.tourn_form
  ORDER BY match_id, player_id
),
tf2 AS (
  SELECT DISTINCT ON (match_id, player_id)
    match_id, player_id,
    wins_last5_tourn, avg_wins_last5_tourn, tourn_played_prev5,
    wins_last5_tourn_surface, avg_wins_last5_tourn_surface, tourn_played_prev5_surface
  FROM processed.tourn_form
  ORDER BY match_id, player_id
)
SELECT
  s.match_id, s.tourney_date, s.surface, s.tourney_level, s.round, s.best_of,
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
  (COALESCE(h1.h2h_played_prior,0) - COALESCE(h2.h2h_played_prior,0)) AS h2h_played_diff,
  (COALESCE(h1.h2h_wins_prior,0)   - COALESCE(h2.h2h_wins_prior,0))   AS h2h_wins_diff,
  (COALESCE(h1.h2h_wins_prior::float/NULLIF(h1.h2h_played_prior,0),0)
   - COALESCE(h2.h2h_wins_prior::float/NULLIF(h2.h2h_played_prior,0),0)) AS h2h_winrate_diff,
  (COALESCE(h1.days_since_last_h2h,0) - COALESCE(h2.days_since_last_h2h,0)) AS h2h_days_since_last_diff,

  -- Tournament-form diffs (overall)
  (COALESCE(t1.wins_last5_tourn,0)      - COALESCE(t2.wins_last5_tourn,0))      AS tourn_wins_last5_diff,
  (COALESCE(t1.avg_wins_last5_tourn,0)  - COALESCE(t2.avg_wins_last5_tourn,0))  AS tourn_avg_wins_last5_diff,
  (COALESCE(t1.tourn_played_prev5,0)    - COALESCE(t2.tourn_played_prev5,0))    AS tourn_played_prev5_diff,

  -- Tournament-form diffs (surface)
  (COALESCE(t1.wins_last5_tourn_surface,0)     - COALESCE(t2.wins_last5_tourn_surface,0))     AS s_tourn_wins_last5_diff,
  (COALESCE(t1.avg_wins_last5_tourn_surface,0) - COALESCE(t2.avg_wins_last5_tourn_surface,0)) AS s_tourn_avg_wins_last5_diff,
  (COALESCE(t1.tourn_played_prev5_surface,0)   - COALESCE(t2.tourn_played_prev5_surface,0))   AS s_tourn_played_prev5_diff
FROM sides s
JOIN p1 ON p1.match_id = s.match_id AND p1.player_id = s.p1_id
JOIN p2 ON p2.match_id = s.match_id AND p2.player_id = s.p2_id
LEFT JOIN h2h1 h1 ON h1.match_id = s.match_id AND h1.player_id = s.p1_id
LEFT JOIN h2h2 h2 ON h2.match_id = s.match_id AND h2.player_id = s.p2_id
LEFT JOIN tf1  t1 ON t1.match_id = s.match_id AND t1.player_id = s.p1_id
LEFT JOIN tf2  t2 ON t2.match_id = s.match_id AND t2.player_id = s.p2_id;

-- Helpful index
CREATE INDEX IF NOT EXISTS idx_match_training_date ON processed.match_training (tourney_date);
