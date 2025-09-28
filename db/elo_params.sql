CREATE SCHEMA IF NOT EXISTS processed;

CREATE TABLE IF NOT EXISTS processed.elo_params (
    id int PRIMARY KEY DEFAULT 1,
    k_g numeric NOT NULL DEFAULT 16,
    k_f numeric NOT NULL DEFAULT 18,
    k_m numeric NOT NULL DEFAULT 20,
    k_a numeric NOT NULL DEFAULT 22,
    k_b numeric NOT NULL DEFAULT 24
);
INSERT INTO processed.elo_params(id) VALUES (1)
ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE FUNCTION processed.elo_k(level TEXT)
RETURNS numeric
LANGUAGE sql
STABLE
AS $$
  SELECT CASE level
    WHEN 'G' THEN k_g
    WHEN 'F' THEN k_f
    WHEN 'M' THEN k_m
    WHEN 'A' THEN k_a
    WHEN 'B' THEN k_b
    ELSE k_m
  END
  FROM processed.elo_params WHERE id=1
$$;

