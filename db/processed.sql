-- db/processed.sql
-- Build a leakage-safe feature layer on top of atp.player_match_rows

CREATE SCHEMA IF NOT EXISTS processed;

-- 1) Base: stable ordering to use in windows
DROP MATERIALIZED VIEW IF EXISTS processed.player_base;
CREATE MATERIALIZED VIEW processed.player_base AS
SELECT
  pmr.*,
  ROW_NUMBER() OVER (
    PARTITION BY pmr.player_id
    ORDER BY pmr.tourney_date, pmr.match_id
  ) AS rn
FROM atp.player_match_rows pmr;

-- 2) Pre-match player features (history only, exclude current)
DROP MATERIALIZED VIEW IF EXISTS processed.player_history;
CREATE MATERIALIZED VIEW processed.player_history AS
WITH base AS (
  SELECT
    b.match_id,
    b.player_id,
    b.opponent_id,
    b.tourney_date,
    b.surface,
    b.tourney_level,
    b.round,
    b.best_of,
    b.is_win::int AS is_win,

    -- TML pre-match attributes (already known before match)
    b.player_rank,
    b.player_rank_points,
    b.player_seed,
    b.player_age,

    -- Per-match rates (for current match only; used for rolling over past matches)
    CASE WHEN b.player_svpt > 0
         THEN (b.player_1stWon + b.player_2ndWon)::float / b.player_svpt
         ELSE NULL END AS spw_match,  -- serve points won

    CASE WHEN b.opp_svpt > 0
         THEN 1.0 - (b.opp_1stWon + b.opp_2ndWon)::float / b.opp_svpt
         ELSE NULL END AS rpw_match,  -- return points won

    CASE WHEN b.player_SvGms > 0
         THEN b.player_ace::float / b.player_SvGms
         ELSE NULL END AS aces_pg_match,

    CASE WHEN b.player_SvGms > 0
         THEN b.player_df::float / b.player_SvGms
         ELSE NULL END AS df_pg_match,

    -- Rest: days since last match (can be 0 when same-date earlier round; conservative)
    (b.tourney_date - LAG(b.tourney_date) OVER (
        PARTITION BY b.player_id
        ORDER BY b.tourney_date, b.match_id
     ))::int AS days_since_last

  FROM processed.player_base b
),

-- Rolling helpers (exclude current: 1 PRECEDING)
roll AS (
  SELECT
    base.*,

    AVG(is_win) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS winrate_5,

    AVG(is_win) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS winrate_10,

    AVG(spw_match) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS spw_10,

    AVG(rpw_match) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rpw_10,

    AVG(aces_pg_match) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS aces_pg_10,

    AVG(df_pg_match) OVER (
      PARTITION BY player_id
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS df_pg_10,

    -- Surface-aware versions
    AVG(is_win) OVER (
      PARTITION BY player_id, surface
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS winrate_10_surface,

    AVG(spw_match) OVER (
      PARTITION BY player_id, surface
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS spw_10_surface,

    AVG(rpw_match) OVER (
      PARTITION BY player_id, surface
      ORDER BY tourney_date, match_id
      ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
    ) AS rpw_10_surface
  FROM base
)

SELECT
  r.match_id,
  r.player_id,
  r.opponent_id,
  r.tourney_date,
  r.surface,
  r.tourney_level,
  r.round,
  r.best_of,

  -- workload windows: prior matches in time windows (strictly before date; conservative)
  (
    SELECT COUNT(*) FROM processed.player_base pb
    WHERE pb.player_id = r.player_id
      AND pb.tourney_date < r.tourney_date
      AND pb.tourney_date >= r.tourney_date - INTERVAL '14 days'
  )::int AS matches_last_14d,

  (
    SELECT COUNT(*) FROM processed.player_base pb
    WHERE pb.player_id = r.player_id
      AND pb.tourney_date < r.tourney_date
      AND pb.tourney_date >= r.tourney_date - INTERVAL '30 days'
  )::int AS matches_last_30d,

  r.days_since_last,

  -- rolling form
  r.winrate_5,
  r.winrate_10,
  r.spw_10,
  r.rpw_10,
  r.aces_pg_10,
  r.df_pg_10,

  -- surface-aware rolling
  r.winrate_10_surface,
  r.spw_10_surface,
  r.rpw_10_surface,

  -- known pre-match attributes from TML
  r.player_rank,
  r.player_rank_points,
  r.player_seed,
  r.player_age

FROM roll r;

-- 3) Match-level training set (difference features, canonical order)
DROP MATERIALIZED VIEW IF EXISTS processed.match_training;
CREATE MATERIALIZED VIEW processed.match_training AS
WITH sides AS (
  SELECT
    m.match_id,
    m.tourney_date,
    m.surface,
    m.tourney_level,
    m.round,
    m.best_of,
    m.winner_id,
    m.loser_id,
    LEAST(m.winner_id, m.loser_id)  AS p1_id,
    GREATEST(m.winner_id, m.loser_id) AS p2_id
  FROM atp.matches m
),
p1 AS (
  SELECT
    ph.match_id, ph.player_id,
    ph.matches_last_14d, ph.matches_last_30d, ph.days_since_last,
    ph.winrate_5, ph.winrate_10, ph.spw_10, ph.rpw_10, ph.aces_pg_10, ph.df_pg_10,
    ph.winrate_10_surface, ph.spw_10_surface, ph.rpw_10_surface,
    ph.player_rank, ph.player_rank_points, ph.player_seed, ph.player_age
  FROM processed.player_history ph
),
p2 AS (
  SELECT
    ph.match_id, ph.player_id,
    ph.matches_last_14d, ph.matches_last_30d, ph.days_since_last,
    ph.winrate_5, ph.winrate_10, ph.spw_10, ph.rpw_10, ph.aces_pg_10, ph.df_pg_10,
    ph.winrate_10_surface, ph.spw_10_surface, ph.rpw_10_surface,
    ph.player_rank, ph.player_rank_points, ph.player_seed, ph.player_age
  FROM processed.player_history ph
)
SELECT
  s.match_id, s.tourney_date, s.surface, s.tourney_level, s.round, s.best_of,
  s.p1_id, s.p2_id,
  CASE WHEN s.winner_id = s.p1_id THEN 1 ELSE 0 END AS y,

  -- differences (p1 - p2) for model inputs
  (p1.player_rank - p2.player_rank)                         AS rank_diff,
  (p1.player_rank_points - p2.player_rank_points)           AS rank_points_diff,
  (p1.player_seed - p2.player_seed)                         AS seed_diff,
  (p1.player_age - p2.player_age)                           AS age_diff,

  (p1.matches_last_14d - p2.matches_last_14d)               AS matches_last_14d_diff,
  (p1.matches_last_30d - p2.matches_last_30d)               AS matches_last_30d_diff,
  (p1.days_since_last - p2.days_since_last)                 AS rest_days_diff,

  (p1.winrate_5 - p2.winrate_5)                             AS winrate5_diff,
  (p1.winrate_10 - p2.winrate_10)                           AS winrate10_diff,
  (p1.spw_10 - p2.spw_10)                                   AS spw10_diff,
  (p1.rpw_10 - p2.rpw_10)                                   AS rpw10_diff,
  (p1.aces_pg_10 - p2.aces_pg_10)                           AS aces_pg10_diff,
  (p1.df_pg_10 - p2.df_pg_10)                               AS df_pg10_diff,

  (p1.winrate_10_surface - p2.winrate_10_surface)           AS winrate10_surface_diff,
  (p1.spw_10_surface - p2.spw_10_surface)                   AS spw10_surface_diff,
  (p1.rpw_10_surface - p2.rpw_10_surface)                   AS rpw10_surface_diff

FROM sides s
JOIN p1 ON p1.match_id = s.match_id AND p1.player_id = s.p1_id
JOIN p2 ON p2.match_id = s.match_id AND p2.player_id = s.p2_id;
