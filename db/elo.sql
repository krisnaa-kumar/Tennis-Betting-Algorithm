-- db/elo.sql

CREATE SCHEMA IF NOT EXISTS processed;

-- Drop in dependency order
DROP MATERIALIZED VIEW IF EXISTS processed.match_training;
DROP MATERIALIZED VIEW IF EXISTS processed.player_history;

-- Elo state tables (TEXT ids)
CREATE TABLE IF NOT EXISTS processed.elo_state (
  player_id TEXT NOT NULL,
  surface   TEXT NOT NULL,
  elo       NUMERIC NOT NULL,
  PRIMARY KEY (player_id, surface)
);

CREATE TABLE IF NOT EXISTS processed.elo_prematch (
  match_id        BIGINT NOT NULL,
  player_id       TEXT   NOT NULL,
  elo_global_pre  NUMERIC NOT NULL,
  elo_surface_pre NUMERIC NOT NULL,
  PRIMARY KEY (match_id, player_id)
);

-- K-factor by level
CREATE OR REPLACE FUNCTION processed.elo_k(level TEXT)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  CASE level
    WHEN 'G' THEN RETURN 16;
    WHEN 'F' THEN RETURN 18;
    WHEN 'M' THEN RETURN 20;
    WHEN 'A' THEN RETURN 22;
    WHEN 'B' THEN RETURN 24;
    ELSE RETURN 20;
  END CASE;
END;
$$;

-- Full Elo rebuild (TEXT winner/loser ids)
CREATE OR REPLACE FUNCTION processed.rebuild_elo()
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;
  surf TEXT;
  g_w NUMERIC; g_l NUMERIC; s_w NUMERIC; s_l NUMERIC;
  exp_w_g NUMERIC; exp_w_s NUMERIC;
  k NUMERIC;
BEGIN
  TRUNCATE TABLE processed.elo_prematch;
  DELETE FROM processed.elo_state;

  FOR r IN
    SELECT match_id, tourney_date, surface, tourney_level, winner_id, loser_id
    FROM atp.matches
    WHERE winner_id IS NOT NULL AND loser_id IS NOT NULL
    ORDER BY tourney_date, match_id
  LOOP
    surf := COALESCE(r.surface, 'Unknown');

    SELECT elo INTO g_w FROM processed.elo_state WHERE player_id=r.winner_id AND surface='ALL';
    IF NOT FOUND THEN g_w := 1500; INSERT INTO processed.elo_state VALUES (r.winner_id, 'ALL', g_w); END IF;

    SELECT elo INTO g_l FROM processed.elo_state WHERE player_id=r.loser_id AND surface='ALL';
    IF NOT FOUND THEN g_l := 1500; INSERT INTO processed.elo_state VALUES (r.loser_id, 'ALL', g_l); END IF;

    SELECT elo INTO s_w FROM processed.elo_state WHERE player_id=r.winner_id AND surface=surf;
    IF NOT FOUND THEN s_w := 1500; INSERT INTO processed.elo_state VALUES (r.winner_id, surf, s_w); END IF;

    SELECT elo INTO s_l FROM processed.elo_state WHERE player_id=r.loser_id AND surface=surf;
    IF NOT FOUND THEN s_l := 1500; INSERT INTO processed.elo_state VALUES (r.loser_id, surf, s_l); END IF;

    -- snapshot pre-match ratings
    INSERT INTO processed.elo_prematch(match_id, player_id, elo_global_pre, elo_surface_pre)
    VALUES (r.match_id, r.winner_id, g_w, s_w)
    ON CONFLICT (match_id, player_id) DO UPDATE
      SET elo_global_pre  = EXCLUDED.elo_global_pre,
          elo_surface_pre = EXCLUDED.elo_surface_pre;

    INSERT INTO processed.elo_prematch(match_id, player_id, elo_global_pre, elo_surface_pre)
    VALUES (r.match_id, r.loser_id, g_l, s_l)
    ON CONFLICT (match_id, player_id) DO UPDATE
      SET elo_global_pre  = EXCLUDED.elo_global_pre,
          elo_surface_pre = EXCLUDED.elo_surface_pre;

    -- expected scores & updates
    exp_w_g := 1.0 / (1.0 + POWER(10.0, (g_l - g_w) / 400.0));
    exp_w_s := 1.0 / (1.0 + POWER(10.0, (s_l - s_w) / 400.0));
    k := processed.elo_k(NULLIF(r.tourney_level,''));

    g_w := g_w + k * (1.0 - exp_w_g);
    g_l := g_l + k * (0.0 - (1.0 - exp_w_g));
    s_w := s_w + k * (1.0 - exp_w_s);
    s_l := s_l + k * (0.0 - (1.0 - exp_w_s));

    UPDATE processed.elo_state SET elo=g_w WHERE player_id=r.winner_id AND surface='ALL';
    UPDATE processed.elo_state SET elo=g_l WHERE player_id=r.loser_id  AND surface='ALL';
    UPDATE processed.elo_state SET elo=s_w WHERE player_id=r.winner_id AND surface=surf;
    UPDATE processed.elo_state SET elo=s_l WHERE player_id=r.loser_id  AND surface=surf;
  END LOOP;
END;
$$;

-- Rebuild player_history INCLUDING Elo
CREATE MATERIALIZED VIEW processed.player_history AS
WITH base AS (
  SELECT
    b.match_id, b.player_id, b.opponent_id, b.tourney_date, b.surface,
    b.tourney_level, b.round, b.best_of, b.is_win::int AS is_win,
    b.player_rank, b.player_rank_points, b.player_seed, b.player_age,

    CASE WHEN b.player_svpt > 0
         THEN (b.player_1stWon + b.player_2ndWon)::float / b.player_svpt END AS spw_match,
    CASE WHEN b.opp_svpt > 0
         THEN 1.0 - (b.opp_1stWon + b.opp_2ndWon)::float / b.opp_svpt END AS rpw_match,
    CASE WHEN b.player_SvGms > 0 THEN b.player_ace::float / b.player_SvGms END AS aces_pg_match,
    CASE WHEN b.player_SvGms > 0 THEN b.player_df::float  / b.player_SvGms END AS df_pg_match,

    (b.tourney_date - LAG(b.tourney_date)
       OVER (PARTITION BY b.player_id ORDER BY b.tourney_date, b.match_id))::int AS days_since_last
  FROM processed.player_base b
),
roll AS (
  SELECT
    base.*,
    AVG(is_win)        OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 5  PRECEDING AND 1 PRECEDING) AS winrate_5,
    AVG(is_win)        OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS winrate_10,
    AVG(spw_match)     OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS spw_10,
    AVG(rpw_match)     OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS rpw_10,
    AVG(aces_pg_match) OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS aces_pg_10,
    AVG(df_pg_match)   OVER (PARTITION BY player_id ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS df_pg_10,

    AVG(is_win)    OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS winrate_10_surface,
    AVG(spw_match) OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS spw_10_surface,
    AVG(rpw_match) OVER (PARTITION BY player_id, surface ORDER BY tourney_date, match_id ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) AS rpw_10_surface
  FROM base
)
SELECT
  r.match_id, r.player_id, r.opponent_id, r.tourney_date, r.surface,
  r.tourney_level, r.round, r.best_of,

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

  r.winrate_5, r.winrate_10, r.spw_10, r.rpw_10, r.aces_pg_10, r.df_pg_10,
  r.winrate_10_surface, r.spw_10_surface, r.rpw_10_surface,

  r.player_rank, r.player_rank_points, r.player_seed, r.player_age,

  ep.elo_global_pre, ep.elo_surface_pre
FROM roll r
LEFT JOIN processed.elo_prematch ep
  ON ep.match_id = r.match_id AND ep.player_id = r.player_id::text;

-- Final training table with Elo diffs
CREATE MATERIALIZED VIEW processed.match_training AS
WITH sides AS (
  SELECT
    m.match_id, m.tourney_date, m.surface, m.tourney_level, m.round, m.best_of,
    m.winner_id, m.loser_id,
    LEAST(m.winner_id, m.loser_id)    AS p1_id,
    GREATEST(m.winner_id, m.loser_id) AS p2_id
  FROM atp.matches m
  WHERE m.winner_id IS NOT NULL AND m.loser_id IS NOT NULL
),
p1 AS (
  SELECT ph.match_id, ph.player_id,
         ph.matches_last_14d, ph.matches_last_30d, ph.days_since_last,
         ph.winrate_5, ph.winrate_10, ph.spw_10, ph.rpw_10, ph.aces_pg_10, ph.df_pg_10,
         ph.winrate_10_surface, ph.spw_10_surface, ph.rpw_10_surface,
         ph.player_rank, ph.player_rank_points, ph.player_seed, ph.player_age,
         ph.elo_global_pre, ph.elo_surface_pre
  FROM processed.player_history ph
),
p2 AS (
  SELECT ph.match_id, ph.player_id,
         ph.matches_last_14d, ph.matches_last_30d, ph.days_since_last,
         ph.winrate_5, ph.winrate_10, ph.spw_10, ph.rpw_10, ph.aces_pg_10, ph.df_pg_10,
         ph.winrate_10_surface, ph.spw_10_surface, ph.rpw_10_surface,
         ph.player_rank, ph.player_rank_points, ph.player_seed, ph.player_age,
         ph.elo_global_pre, ph.elo_surface_pre
  FROM processed.player_history ph
)
SELECT
  s.match_id, s.tourney_date, s.surface, s.tourney_level, s.round, s.best_of,
  s.p1_id, s.p2_id,
  CASE WHEN s.winner_id = s.p1_id THEN 1 ELSE 0 END AS y,

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

  (p1.elo_global_pre - p2.elo_global_pre)         AS elo_diff,
  (p1.elo_surface_pre - p2.elo_surface_pre)       AS elo_surface_diff
FROM sides s
JOIN p1 ON p1.match_id = s.match_id AND p1.player_id = s.p1_id
JOIN p2 ON p2.match_id = s.match_id AND p2.player_id = s.p2_id;






