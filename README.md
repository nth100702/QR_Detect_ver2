# HikVision QR Detect

Hệ thống tự động detect QR code từ footage NVR camera Hikvision, lưu kết quả vào PostgreSQL và cung cấp dashboard tra cứu.

---

## Yêu cầu

- Docker & Docker Compose
- Hikvision NVR có HCNetSDK (file `.dll` / `.so`)
- VPS hoặc máy chủ Linux (4 vCPU khuyến nghị)
- FFmpeg với VAAPI (Linux) hoặc CUDA/d3d11va (Windows)

---

## Cài đặt

### 1. Clone và chuẩn bị SDK

```bash
git clone <repo>
cd QR_ver2
mkdir -p sdk/lib
# Copy HCNetSDK.dll (Windows) hoặc libHCNetSDK.so (Linux) vào sdk/lib/
```

### 2. Tạo file `.env`

```env
# NVR
NVR_HOST=192.168.1.100
NVR_PORT=8000
NVR_USER=admin
NVR_PASS=your_password

# Database
POSTGRES_DB=qrdb
POSTGRES_USER=qruser
POSTGRES_PASSWORD=your_db_password
DATABASE_URL=postgresql://qruser:your_db_password@postgres:5432/qrdb

# Redis
REDIS_URL=redis://redis:6379/0

# API
API_SECRET=your_admin_secret
VIEWER_SECRET=your_viewer_secret
API_BASE_URL=http://localhost:8000

# Thư mục tạm (optional, mặc định ./temp_videos và ./temp_clips)
TEMP_VIDEO_DIR=/app/temp_videos
TEMP_CLIP_DIR=/app/temp_clips
```

### 3. Khởi động

```bash
docker compose up -d
```

API chạy tại `http://localhost:8000`. Dashboard tại `http://localhost:8000/dashboard`.

---

## Cấu hình chính

| Biến | File | Mặc định | Mô tả |
|------|------|----------|-------|
| `DETECT_WORKERS` | `core/config.py` | `3` | Số FFmpeg process detect song song |
| `NVR_CHANNELS` | `core/config.py` | `4` | Số DL worker tải footage song song |
| `SEGMENT_HOURS` | `core/config.py` | `0.5` | Độ dài mỗi chunk tải về (giờ) |
| `OUTPUT_WIDTH` | `scanner/detect_fast.py` | `1920` | Chiều rộng frame khi detect |
| `SAMPLE_INTERVAL` | `scanner/detect_fast.py` | `0.2` | Giây giữa 2 frame liên tiếp (= 5fps) |
| `QR_COOLDOWN_SEC` | `scanner/detect_fast.py` | `240` | Thời gian dedup cùng 1 QR (giây) |

---

## Cron tự động

Mỗi ngày lúc **19:00 ICT**, hệ thống tự động tải và detect toàn bộ footage **8:00–19:00** của ngày hôm đó cho tất cả camera active.

Nếu job hôm trước chưa xong, ngày mới được xếp vào hàng chờ và chạy backfill tự động sau khi job hiện tại hoàn thành — không chạy song song.

---

## Trigger thủ công

### Qua Dashboard (Admin)

Vào tab **Worker Status** → chọn ngày, giờ, camera → **Trigger**.

### Qua API

```bash
# Trigger scan cho 1 hoặc nhiều camera
curl -X POST http://localhost:8000/jobs \
  -H "X-Secret: your_admin_secret" \
  -H "Content-Type: application/json" \
  -d '{
    "cam_ids": [1, 2, 3],
    "date": "2026-06-15",
    "start_time": "08:00",
    "end_time": "19:00"
  }'
```

---

## Deploy lên VPS

```bash
# Build và push image
docker build -t nth100702/qr-api:v2 -f api/Dockerfile .
docker push nth100702/qr-api:v2

# Trên VPS
docker compose pull
docker compose up -d
```

Xem log:
```bash
docker logs -f qr_api
```

---

## Cấu trúc thư mục

```
QR_ver2/
├── api/
│   ├── main.py          # FastAPI app, cron scheduler, scan_date_bulk pipeline
│   ├── Dockerfile
│   └── static/
│       └── dashboard.html
├── scanner/
│   ├── detect_fast.py   # FFmpeg hwaccel pipe + zxingcpp (pipeline chính)
│   └── detect.py        # Wrapper async, POST scan lên API
├── core/
│   ├── config.py        # Biến cấu hình từ .env
│   ├── models.py        # is_valid_qr(), get_shift()
│   └── nvr.py           # HikVision NVR client (HCNetSDK)
├── db/
│   └── init.sql         # Schema PostgreSQL
├── sdk/lib/             # HCNetSDK binary (không commit)
├── docker-compose.yml
├── requirements.txt
├── ARCHITECTURE.md      # Giải thích các quyết định kỹ thuật
└── README.md
```

---

## Xem thêm

Chi tiết về các quyết định kỹ thuật (tại sao FFmpeg thay OpenCV, tại sao 1920px, cron guard...) xem tại [ARCHITECTURE.md](ARCHITECTURE.md).
