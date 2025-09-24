CREATE SCHEMA IF NOT EXISTS atp;

CREATE TABLE IF NOT EXISTS atp.players (
    player_id INTEGER PRIMARY KEY,
    name_last TEXT,
    name_first TEXT,
    hand TEXT,
    dob DATE, --- converting to yyymmdd
    ioc TEXT, --- country code
    height INTEGER
);

CREATE INDEX IF NOT EXISTS idx_players_ioc ON atp.players(ioc);

--- rankings ---

CREATE TABLE IF NOT EXISTS atp.rankings (
    ranking_date DATE NOT NULL,
    rank INTEGER,
    player_id INTEGER NOT NULL,
    points INTEGER,
    PRIMARY KEY (ranking_date, player_id)
);

CREATE INDEX IF NOT EXISTS idx_rankings_player ON atp.players(player_id);
CREATE INDEX IF NOT EXISTS idx_rankings_date ON atp.rankings(ranking_date);

--- Matches which are yearly ---
CREATE TABLE IF NOT EXISTS atp.matches(
    match_id BIGSERIAL PRIMARY KEY, --- made up key just for convenience

    tourney_id TEXT, ---  check this
    tourney_name TEXT,
    surface TEXT,
    draw_size INTEGER,
    tourney_level TEXT,
    tourney_date DATE,

    match_num INTEGER,

    winner_id INTEGER,
    winner_name TEXT,
    winner_hand TEXT,
    winner_ht INTEGER,
    winner_ioc TEXT,
    --- winner_age

    loser_id INTEGER,
    loser_name TEXT,
    loser_hand TEXT,
    loser_ht INTEGER,
    loser_ioc TEXT,
    --- loser_age

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
CREATE INDEX IF NOT EXISTS idx_matches_date ON atp.matches(tourney_date);
CREATE INDEX IF NOT EXISTS idx_matches_surface    ON atp.matches(surface);
CREATE INDEX IF NOT EXISTS idx_matches_winner     ON atp.matches(winner_id);
CREATE INDEX IF NOT EXISTS idx_matches_loser      ON atp.matches(loser_id);
CREATE INDEX IF NOT EXISTS idx_matches_level      ON atp.matches(tourney_level);
CREATE INDEX IF NOT EXISTS idx_matches_round      ON atp.matches(round);

-----
-- Optional: a convenient VIEW that normalizes a player-centric row
-- (winner/loser unified; handy for feature engineering)
-- =========================
CREATE OR REPLACE VIEW atp.player_match_rows AS
SELECT
  m.match_id,
  m.tourney_date,
  m.tourney_id,
  m.tourney_name,
  m.surface,
  m.tourney_level,
  m.round,
  m.best_of,
  m.minutes,
  m.winner_id     AS player_id,
  m.loser_id      AS opponent_id,
  TRUE            AS is_win,
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
  m.match_id,
  m.tourney_date,
  m.tourney_id,
  m.tourney_name,
  m.surface,
  m.tourney_level,
  m.round,
  m.best_of,
  m.minutes,
  m.loser_id      AS player_id,
  m.winner_id     AS opponent_id,
  FALSE           AS is_win,
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