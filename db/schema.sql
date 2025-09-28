DROP SCHEMA IF EXISTS atp CASCADE;
CREATE SCHEMA atp;

--- Matches which are yearly ---
CREATE TABLE atp.matches(
    match_id BIGSERIAL PRIMARY KEY, --- made up key just for convenience

    tourney_id TEXT, ---  check this
    tourney_name TEXT,
    surface TEXT,
    draw_size INTEGER,
    tourney_level TEXT,
    tourney_date DATE,

    match_num INTEGER,

    winner_id TEXT,
    winner_seed INTEGER,
    winner_entry TEXT,
    winner_name TEXT,
    winner_hand TEXT,
    winner_ht INTEGER,
    winner_ioc TEXT,
    winner_age REAL,
    winner_rank INTEGER,
    winner_rank_points INTEGER,

    loser_id TEXT,
    loser_seed INTEGER,
    loser_entry TEXT,
    loser_name TEXT,
    loser_hand TEXT,
    loser_ht INTEGER,
    loser_ioc TEXT,
    loser_age REAL,
    loser_rank INTEGER,
    loser_rank_points INTEGER,
 
    score TEXT, --- check this
    best_of INTEGER,
    round TEXT,
    minutes INTEGER,

    w_ace INTEGER,
    w_df INTEGER,
    w_svpt INTEGER,
    w_1stIn           INTEGER,
    w_1stWon          INTEGER,
    w_2ndWon          INTEGER,
    w_SvGms           INTEGER,
    w_bpSaved         INTEGER,
    w_bpFaced         INTEGER,

    l_ace             INTEGER,
    l_df              INTEGER,
    l_svpt            INTEGER,
    l_1stIn           INTEGER,
    l_1stWon          INTEGER,
    l_2ndWon          INTEGER,
    l_SvGms           INTEGER,
    l_bpSaved         INTEGER,
    l_bpFaced         INTEGER
);

--- indexes for filters later
CREATE INDEX idx_matches_date ON atp.matches(tourney_date);
CREATE INDEX idx_matches_surface    ON atp.matches(surface);
CREATE INDEX idx_matches_winner     ON atp.matches(winner_id);
CREATE INDEX idx_matches_loser      ON atp.matches(loser_id);
CREATE INDEX idx_matches_level      ON atp.matches(tourney_level);
CREATE INDEX idx_matches_round      ON atp.matches(round);

CREATE UNIQUE INDEX uniq_matches_triplet ON atp.matches (tourney_id, tourney_date, match_num);

CREATE TABLE atp.players (
  player_id   INTEGER PRIMARY KEY,
  name_last   TEXT,
  name_first  TEXT,
  hand        TEXT,
  dob         DATE,
  ioc         TEXT,
  height_cm   INTEGER
);

CREATE TABLE atp.rankings (
  ranking_date DATE NOT NULL,
  rank         INTEGER,
  player_id    INTEGER NOT NULL,
  points       INTEGER,
  PRIMARY KEY (ranking_date, player_id)
);

-----
-- Optional: a convenient VIEW that normalizes a player-centric row
-- (winner/loser unified; handy for feature engineering)
-- includes rank/seed/age
-- =========================
CREATE OR REPLACE VIEW atp.player_match_rows AS
SELECT
  m.match_id, m.tourney_date, m.tourney_id, m.tourney_name, m.surface,
  m.tourney_level, m.round, m.best_of, m.minutes,

  m.winner_id     AS player_id,
  m.loser_id      AS opponent_id,
  TRUE            AS is_win,

  m.winner_seed   AS player_seed,
  m.winner_entry  AS player_entry,
  m.winner_age    AS player_age,
  m.winner_rank   AS player_rank,
  m.winner_rank_points AS player_rank_points,

  m.loser_seed    AS opp_seed,
  m.loser_entry   AS opp_entry,
  m.loser_age     AS opp_age,
  m.loser_rank    AS opp_rank,
  m.loser_rank_points  AS opp_rank_points,

  m.w_ace         AS player_ace,
  m.w_df          AS player_df,
  m.w_svpt        AS player_svpt,
  m.w_1stIn       AS player_1stIn,
  m.w_1stWon      AS player_1stWon,
  m.w_2ndWon      AS player_2ndWon,
  m.w_SvGms       AS player_SvGms,
  m.w_bpSaved     AS player_bpSaved,
  m.w_bpFaced     AS player_bpFaced,

  m.l_ace         AS opp_ace,
  m.l_df          AS opp_df,
  m.l_svpt        AS opp_svpt,
  m.l_1stIn       AS opp_1stIn,
  m.l_1stWon      AS opp_1stWon,
  m.l_2ndWon      AS opp_2ndWon,
  m.l_SvGms       AS opp_SvGms,
  m.l_bpSaved     AS opp_bpSaved,
  m.l_bpFaced     AS opp_bpFaced

FROM atp.matches m
UNION ALL
SELECT
  m.match_id, m.tourney_date, m.tourney_id, m.tourney_name, m.surface,
  m.tourney_level, m.round, m.best_of, m.minutes,

  -- loser as player 
  m.loser_id      AS player_id,
  m.winner_id     AS opponent_id,
  FALSE           AS is_win,

  m.loser_seed    AS player_seed,
  m.loser_entry   AS player_entry,
  m.loser_age     AS player_age,
  m.loser_rank    AS player_rank,
  m.loser_rank_points AS player_rank_points,

  m.winner_seed   AS opp_seed,
  m.winner_entry  AS opp_entry,
  m.winner_age    AS opp_age,
  m.winner_rank   AS opp_rank,
  m.winner_rank_points  AS opp_rank_points,

  m.l_ace         AS player_ace,
  m.l_df          AS player_df,
  m.l_svpt        AS player_svpt,
  m.l_1stIn       AS player_1stIn,
  m.l_1stWon      AS player_1stWon,
  m.l_2ndWon      AS player_2ndWon,
  m.l_SvGms       AS player_SvGms,
  m.l_bpSaved     AS player_bpSaved,
  m.l_bpFaced     AS player_bpFaced,

  m.w_ace         AS opp_ace,
  m.w_df          AS opp_df,
  m.w_svpt        AS opp_svpt,
  m.w_1stIn       AS opp_1stIn,
  m.w_1stWon      AS opp_1stWon,
  m.w_2ndWon      AS opp_2ndWon,
  m.w_SvGms       AS opp_SvGms,
  m.w_bpSaved     AS opp_bpSaved,
  m.w_bpFaced     AS opp_bpFaced

FROM atp.matches m;