# Architecture & Logic

Giải thích các quyết định kỹ thuật trong hệ thống detect QR từ footage NVR camera.

---

## 1. Tại sao không dùng OpenCV để decode video

Pipeline cũ dùng `cv2.VideoCapture` với pattern BURST=2 SKIP=3 (đọc 2 frame, bỏ 3, lặp lại).

**Vấn đề:** Camera NVR dùng codec HEVC (H.265) với long-GOP (Group of Pictures ~60 frame). Khi gọi `cap.grab()` để skip frame, OpenCV vẫn phải decode toàn bộ GOP để tìm đúng frame đó — không thực sự skip. Benchmark thực tế trên file 30 phút:

- T_decode = 84% tổng thời gian
- T_resize = 7%
- T_zxing = 10%

Tăng SKIP không giúp ích vì bottleneck là decode, không phải số frame zxing phải xử lý. Kết quả: ~30 phút/segment → 924 segment × 3 workers = 154 giờ cho 1 ngày footage.

**Giải pháp:** Dùng FFmpeg thay OpenCV. FFmpeg xử lý GOP natively, hỗ trợ hwaccel (NVDEC/VAAPI), và có filter `fps=1/N` chạy trong pipeline trước khi data ra Python — Python chỉ nhận frame đã sample, không block bởi decode.

---

## 2. FFmpeg pipe thay vì decode file

`detect_video_fast()` mở subprocess FFmpeg, nhận rawvideo qua `pipe:1` (stdout):

```
FFmpeg → [fps filter] → [scale filter] → rawvideo bytes → Python (numpy) → zxingcpp
```

Lợi ích so với decode file:
- Không cần write file trung gian
- `fps=1/0.2` = 5 frame/giây chạy trong FFmpeg, Python không thấy frame bị skip
- Scale cũng chạy trong FFmpeg (hoặc GPU nếu có CUDA)

---

## 3. Hardware acceleration chain

`_detect_hwaccel()` probe theo thứ tự ưu tiên:

```
cuda → d3d11va → qsv → vaapi → software (fallback)
```

**Quan trọng:** Probe bằng cách decode thực sự 1 frame từ file thực, không chỉ kiểm tra `-hwaccels`. Lý do: trên VPS không có GPU, `ffmpeg -hwaccels` vẫn liệt kê `cuda` nhưng thực tế fail khi decode → 0 frame output → 0 QR detected. Lỗi này từng xảy ra sau lần deploy đầu.

Kết quả hwaccel ảnh hưởng đến pixel format output:
- `cuda` / `qsv`: output `nv12` (không thể output gray trực tiếp sau hwdownload)
- `d3d11va` / `software` / `vaapi`: output `gray`

---

## 4. NV12 và Y-plane

CUDA/QSV chỉ hỗ trợ `hwdownload` sang `nv12` hoặc `yuv420p`, không hỗ trợ `gray`.

NV12 layout trong bộ nhớ:
```
[Y plane: W × H bytes]  ← grayscale
[UV plane: W × H/2 bytes interleaved]
```

Python chỉ lấy `raw[:W*H]` là đủ — đó là Y-plane = grayscale, dùng trực tiếp cho zxingcpp.

---

## 5. Tại sao OUTPUT_WIDTH = 1920

Camera nguồn là 2688×1520. Test thực tế trên cùng 1 segment 30 phút:

| Width | QR detected |
|-------|-------------|
| 640   | 5           |
| 1344  | 5           |
| 1920  | 19          |

640 và 1344 miss nhiều QR vì một số QR nhỏ hoặc ở xa, downscale quá mạnh làm mất detail. 1920px giữ đủ resolution để zxingcpp decode được tất cả QR trong frame.

---

## 6. Tại sao SAMPLE_INTERVAL = 0.2 (5 frame/giây)

QR trong thực tế xuất hiện tối thiểu ~1-2 giây (người dùng đưa lên camera). Với fps=5, mỗi QR sẽ xuất hiện trong ít nhất 5-10 frame → miss rate gần 0%.

fps=2 (interval=0.5) từng bỏ sót QR chỉ xuất hiện 1 giây. fps=5 tương đương với pattern BURST=2 SKIP=3 ở video 25fps gốc.

---

## 7. QR_COOLDOWN_SEC = 4 phút

Sau khi detect 1 QR, không lưu lại QR đó trong vòng 4 phút. Mục đích: dedup — nếu QR xuất hiện liên tục trong 30 giây, chỉ lưu 1 lần.

**Tại sao 4 phút, không phải 4 giờ (giá trị cũ):** Thực tế có tình huống nhiều QR khác nhau được đưa lên cùng 1 camera liên tiếp trong vòng vài phút (nhiều user). Cooldown 4 giờ block tất cả QR sau lần đầu → miss scan. 4 phút đủ để dedup noise nhưng vẫn capture multi-scan trong cùng session.

---

## 8. Pipeline download → detect trong scan_date_bulk

```
dl_queue → [NVR_CHANNELS DL workers] → detect_queue → [DETECT_WORKERS detect workers]
```

**Round-robin interleave:** Thay vì xử lý tuần tự từng cam (cam1 hết rồi mới cam2), các segment được sắp xếp theo slot thời gian:

```
cam1-08:00, cam2-08:00, cam3-08:00, ..., cam1-08:30, cam2-08:30, ...
```

Lý do: NVR từ chối download concurrent cho cùng 1 channel. Round-robin đảm bảo 2 DL worker không bao giờ tải cùng 1 cam_id cùng lúc.

**Back-pressure:** `detect_queue` có `maxsize = DETECT_WORKERS × 3`. DL worker sẽ block khi queue đầy, tránh tải quá nhiều file vào disk khi detect chưa kịp xử lý.

---

## 9. Tại sao DETECT_WORKERS = 3 (không phải 4)

VPS có 4 vCPU. VAAPI decode dùng 1 core cho FFmpeg process + overhead Python. Giữ 1 core cho API server và download worker. 3 detect workers = 3 FFmpeg processes chạy song song, mỗi cái dùng ~1 core.

---

## 10. Cron guard và backfill

Cron trigger lúc 19:00 mỗi ngày xử lý footage ngày hôm đó (8:00-19:00). Với VAAPI ~20 phút/segment và 924 segment, 1 job mất ~3 ngày.

**Tại sao trigger 19:00:** Footage 8:00-19:00 của ngày hôm đó vừa đủ có mặt trên NVR lúc 19:00 (segment cuối 18:30-19:00 vừa xong). Trigger sớm hơn 20:00 giúp job bắt đầu sớm hơn 1 tiếng, tích lũy qua nhiều ngày sẽ có ý nghĩa.

**Vấn đề:** Nếu job ngày N chưa xong thì 19:00 ngày N+1, cron trigger job mới cho ngày N+1 → 2 job chạy song song → tranh 3 detect workers → cả 2 chậm hơn → ngày N+2 lại overlap, cộng dồn.

**Giải pháp (`_daily_scan_worker` + `_daily_scan_running`):**
- Flag `_daily_scan_running` đảm bảo chỉ 1 job chạy tại 1 thời điểm
- Nếu job đang chạy, ngày mới được push vào `_pending_scan_dates` (FIFO queue)
- Sau khi job xong, worker tự drain queue và chạy backfill tuần tự

```
19h N:   job N bắt đầu      (_daily_scan_running = True)
19h N+1: job N còn chạy     → enqueue "N+1" vào _pending_scan_dates
19h N+2: job N còn chạy     → enqueue "N+2" vào _pending_scan_dates
job N xong → backfill N+1 → backfill N+2 → _daily_scan_running = False
```

---

## 11. Segment 30 phút, không phải file nguyên ngày

Tải footage theo chunk 30 phút thay vì 1 file 11 tiếng vì:

1. **Disk**: file 11h HEVC 2688×1520 ≈ 10-20GB/cam × 42 cam = không khả thi
2. **Pipeline**: download và detect chạy song song — segment xong download là detect ngay, không đợi toàn bộ ngày
3. **Fault tolerance**: nếu download fail ở giữa chừng chỉ mất 30 phút, không phải toàn bộ ngày
4. **NVR limit**: Hikvision thường giới hạn thời lượng per clip request

---

## 12. Scan được lưu ở đâu

`detect_video_fast()` trả về `list[dict]` — chỉ tồn tại trong RAM của detect worker thread, không có intermediate storage.

Sau khi detect xong 1 segment, `process_video()` trong `detect.py` POST từng scan lên `/scans` API ngay lập tức. Nếu POST fail thì scan đó bị mất (không có retry queue hiện tại).

File video tạm bị xóa sau khi detect xong (`delete_after=True`).
