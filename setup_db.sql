-- ============================================================
-- setup_db.sql
-- Chạy 1 lần để tạo toàn bộ DB cho Hikvision QR Scanner
--
-- Cách chạy (CMD):
--   psql -U postgres -f setup_db.sql
-- ============================================================


-- ── 1. Tạo user và database ──────────────────────────────────
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
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'qrscanner')\gexec


-- ── 2. Kết nối vào database vừa tạo ─────────────────────────
\c qrscanner


-- ── 3. Tạo bảng cameras ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS cameras (
    id          SERIAL PRIMARY KEY,
    name        TEXT    NOT NULL,
    shift       TEXT    NOT NULL CHECK (shift IN ('morning', 'night')),
    nvr_channel INTEGER NOT NULL UNIQUE,
    status      TEXT    NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'deleted'))
);


-- ── 4. Tạo bảng qr_scans ─────────────────────────────────────
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
    UNIQUE (cam_id, qr_value, chunk_start)
);

CREATE INDEX IF NOT EXISTS idx_qr_value    ON qr_scans (qr_value);
CREATE INDEX IF NOT EXISTS idx_detected_at ON qr_scans (cam_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_date        ON qr_scans (DATE(detected_at));


-- ── 5. Seed cameras (2 cam demo, khớp với CHANNEL_MAP trong config.py) ──
INSERT INTO cameras (id, name, shift, nvr_channel) VALUES
    (1, 'Camera 01 - Cổng vào', 'morning', 1),
    (5, 'Camera 05 - Kho hàng', 'morning', 5)
ON CONFLICT DO NOTHING;

-- Reset sequence để SERIAL tiếp tục đúng sau khi insert thủ công
SELECT setval('cameras_id_seq', (SELECT MAX(id) FROM cameras));


-- ── 6. Cấp quyền cho qruser ──────────────────────────────────
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO qruser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO qruser;


-- ── 7. Kiểm tra kết quả ──────────────────────────────────────
SELECT '=== cameras ===' AS info;
SELECT id, name, shift, nvr_channel, status FROM cameras ORDER BY id;

SELECT '=== qr_scans (rỗng lúc đầu) ===' AS info;
SELECT COUNT(*) AS total_scans FROM qr_scans;