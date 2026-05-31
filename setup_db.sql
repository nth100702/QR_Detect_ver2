-- ============================================================
-- setup_db.sql
-- Run once to create DB for Hikvision QR Scanner
--
-- Usage:
--   psql -U postgres -f setup_db.sql
-- ============================================================


-- 1. Create user and database
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'qruser') THEN
        CREATE USER qruser WITH PASSWORD 'qrpassword123';
        RAISE NOTICE 'User qruser created';
    ELSE
        RAISE NOTICE 'User qruser already exists, skipping';
    END IF;
END
$$;

SELECT 'CREATE DATABASE qrscanner OWNER qruser'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'qrscanner'
)\gexec


-- 2. Connect to database
\c qrscanner


-- 3. Set timezone
ALTER DATABASE qrscanner
SET timezone TO 'Asia/Ho_Chi_Minh';


-- 4. Create cameras table
CREATE TABLE IF NOT EXISTS cameras (
    id          SERIAL PRIMARY KEY,
    name        TEXT    NOT NULL,
    shift       TEXT    NOT NULL CHECK (shift IN ('morning', 'night')),
    nvr_channel INTEGER NOT NULL UNIQUE,
    status      TEXT    NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active', 'deleted'))
);


-- 5. Create qr_scans table
CREATE TABLE IF NOT EXISTS qr_scans (
    id          SERIAL PRIMARY KEY,
    cam_id      INT         NOT NULL REFERENCES cameras(id),
    qr_value    TEXT        NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    chunk_start TIMESTAMPTZ NOT NULL,
    chunk_end   TIMESTAMPTZ NOT NULL,
    clip_file   TEXT,
    shift       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_scan UNIQUE (cam_id, qr_value, chunk_start)
);


-- 6. Create indexes
CREATE INDEX IF NOT EXISTS idx_qr_value
ON qr_scans (qr_value);

CREATE INDEX IF NOT EXISTS idx_detected_at
ON qr_scans (cam_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_date
ON qr_scans (
    ((detected_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date)
);


-- 7. Seed cameras
INSERT INTO cameras (id, name, shift, nvr_channel) VALUES
    (1, 'Ban OB14-L', 'morning', 1),
    (2, 'Ban OB2-L',  'morning', 5)
ON CONFLICT DO NOTHING;


-- 8. Reset sequence
SELECT setval(
    'cameras_id_seq',
    (SELECT MAX(id) FROM cameras)
);


-- 9. Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES
IN SCHEMA public TO qruser;

GRANT ALL PRIVILEGES ON ALL SEQUENCES
IN SCHEMA public TO qruser;


-- 10. Verify result
SELECT '=== cameras ===' AS info;

SELECT id, name, shift, nvr_channel, status
FROM cameras
ORDER BY id;


SELECT '=== qr_scans (empty initially) ===' AS info;

SELECT COUNT(*) AS total_scans
FROM qr_scans;


SELECT '=== constraints ===' AS info;

SELECT conname, contype
FROM pg_constraint
WHERE conrelid = 'qr_scans'::regclass
ORDER BY conname;


SELECT '=== timezone ===' AS info;

SHOW timezone;