"""
scanner/detect_fast.py
─────────────────────────────────────────────────────────────────────────────
Pipeline tối ưu cho HEVC long-GOP:

  FFmpeg (hwaccel NVDEC/QSV/CPU) → select + scale filter → rawvideo pipe
  → Python nhận frame nhỏ đã sample → zxingcpp

Ưu điểm vs OpenCV VideoCapture:
  - FFmpeg tự xử lý GOP boundary, không cần grab() loop ở Python
  - hwaccel decode (NVDEC/QSV) giảm T_decode 5–15×
  - scale filter chạy trong FFmpeg (CUDA nếu có) → Python nhận frame nhỏ hơn
  - Python chỉ làm zxingcpp, không bị block bởi decode

Yêu cầu:
  - ffmpeg trong PATH (build có --enable-cuda-nvcc hoặc --enable-libmfx)
  - pip install zxingcpp
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator

import numpy as np
import zxingcpp

from core.models import is_valid_qr, get_shift
from core.config import API_BASE_URL, API_SECRET

logger = logging.getLogger(__name__)

# ── Sampling ─────────────────────────────────────────────────────────────────
# Camera 2688×1520, QR xuất hiện ~2 giây:
#   SAMPLE_INTERVAL=0.5 → 2 frame/giây → miss rate ~0% với QR ≥ 1.5s
#   SAMPLE_INTERVAL=1.0 → 1 frame/giây → miss rate cao nếu QR xuất hiện 2s
SAMPLE_INTERVAL = 0.2   # giây / frame output — tương đương BURST=2 SKIP=3 ở 25fps

# OUTPUT_WIDTH: camera nguồn 2688px.
# Test thực tế trên seg_1630_1700:
#   640px  → 5 QR  (miss nhiều)
#   1344px → 5 QR  (vẫn miss, không đủ resolution)
#   1920px → 19 QR (match OpenCV 1920px, nhanh hơn 2.6×)
OUTPUT_WIDTH = 1920

# Sau khi detect được QR, quét lùi N giây để tìm timestamp chính xác hơn
LOOKBACK_FRAMES = 3     # số frame lookback sau khi bắt được QR lần đầu

# Cooldown dedup
QR_COOLDOWN_SEC = 4 * 3600

# ── Hardware acceleration ─────────────────────────────────────────────────────
# Thứ tự ưu tiên trên Windows: cuda > d3d11va (decode-only, tốt) > qsv > software
# d3d11va decode-only nhanh hơn software nhưng không cần NVIDIA GPU
_HWACCEL_PRIORITY = ["cuda", "d3d11va", "qsv", "vaapi", ""]

_headers = {"X-Secret": API_SECRET, "Content-Type": "application/json"}


def _detect_hwaccel(probe_file: Path | None = None) -> str:
    """
    Tìm hwaccel tốt nhất available bằng cách thử decode thực sự.
    Nếu probe_file được cung cấp, thử decode 1 frame từ file đó để xác nhận.
    Fallback về software nếu không có gì hoạt động.
    """
    try:
        listed = subprocess.check_output(
            ["ffmpeg", "-hwaccels"], stderr=subprocess.STDOUT, timeout=5
        ).decode(errors="ignore")
    except Exception:
        return ""

    # Thứ tự ưu tiên trên Windows: cuda > d3d11va > qsv > software
    candidates = [hw for hw in _HWACCEL_PRIORITY if hw and hw in listed]

    if not probe_file:
        result = candidates[0] if candidates else ""
        logger.info(f"[hwaccel] Selected (no probe): {result or 'software'}")
        return result

    for hw in candidates:
        cmd, pix_fmt = _build_ffmpeg_cmd(probe_file, hw, 5.0, 320)
        # Lấy đúng 1 frame để test
        test_cmd = cmd[:-1] + ["-frames:v", "1"] + [cmd[-1]]
        try:
            r = subprocess.run(
                test_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15
            )
            if r.returncode == 0 and len(r.stdout) > 0:
                logger.info(f"[hwaccel] Probe OK: {hw}")
                return hw
            else:
                logger.warning(f"[hwaccel] Probe failed for {hw}: {r.stderr[-200:].decode(errors='ignore')}")
        except Exception as e:
            logger.warning(f"[hwaccel] Probe exception for {hw}: {e}")

    logger.info("[hwaccel] All hwaccels failed probe, falling back to software")
    return ""


def _build_ffmpeg_cmd(
    video_path: Path,
    hwaccel: str,
    sample_interval: float,
    output_width: int,
) -> tuple[list[str], str]:
    """
    Xây lệnh FFmpeg pipe rawvideo ra stdout.
    Trả về (cmd, pix_fmt) — pix_fmt cần để tính frame_bytes và extract gray.

    Filter graph:
      fps=1/SAMPLE_INTERVAL  → 1 frame mỗi N giây
      scale=WIDTH:-2         → giữ AR, bội 2
      format=...             → pix_fmt phù hợp với hwaccel

    hwdownload sau CUDA/QSV chỉ hỗ trợ nv12 / yuv420p, không hỗ trợ gray.
    Python extract Y-plane (= gray) từ nv12: frame[:height] là Y-plane.

    Software decode output trực tiếp gray → đơn giản hơn.
    """
    fps_filter = f"fps=1/{sample_interval}"

    if hwaccel == "cuda":
        # scale_cuda → hwdownload → nv12; Python lấy Y-plane
        vf = f"{fps_filter},scale_cuda={output_width}:-2,hwdownload,format=nv12"
        cmd = [
            "ffmpeg", "-loglevel", "error",
            "-hwaccel", "cuda",
            "-hwaccel_output_format", "cuda",
            "-i", str(video_path),
            "-vf", vf,
            "-f", "rawvideo", "-pix_fmt", "nv12", "pipe:1",
        ]
        return cmd, "nv12"

    elif hwaccel == "qsv":
        # vpp_qsv → hwdownload → nv12
        vf = f"{fps_filter},vpp_qsv=w={output_width}:h=-2,hwdownload,format=nv12"
        cmd = [
            "ffmpeg", "-loglevel", "error",
            "-hwaccel", "qsv",
            "-hwaccel_output_format", "qsv",
            "-i", str(video_path),
            "-vf", vf,
            "-f", "rawvideo", "-pix_fmt", "nv12", "pipe:1",
        ]
        return cmd, "nv12"

    elif hwaccel == "d3d11va":
        # d3d11va decode-only (Windows); scale trên CPU
        vf = f"{fps_filter},scale={output_width}:-2,format=gray"
        cmd = [
            "ffmpeg", "-loglevel", "error",
            "-hwaccel", "d3d11va",
            "-i", str(video_path),
            "-vf", vf,
            "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1",
        ]
        return cmd, "gray"

    else:
        # Software decode
        vf = f"{fps_filter},scale={output_width}:-2,format=gray"
        cmd = [
            "ffmpeg", "-loglevel", "error",
            "-i", str(video_path),
            "-vf", vf,
            "-f", "rawvideo", "-pix_fmt", "gray", "pipe:1",
        ]
        return cmd, "gray"


def _get_video_resolution(video_path: Path) -> tuple[int, int]:
    """Lấy width/height bằng ffprobe để tính output frame size."""
    try:
        out = subprocess.check_output([
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            str(video_path),
        ], timeout=10).decode().strip()
        w, h = map(int, out.split(","))
        return w, h
    except Exception as e:
        logger.warning(f"ffprobe failed: {e}, assuming 1920×1080")
        return 1920, 1080


def _get_video_duration(video_path: Path) -> float:
    """Lấy duration (giây) bằng ffprobe."""
    try:
        out = subprocess.check_output([
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(video_path),
        ], timeout=10).decode().strip()
        return float(out)
    except Exception:
        return 0.0


def _frame_reader(
    video_path: Path,
    hwaccel: str,
    sample_interval: float,
    output_width: int,
    cancel_event: threading.Event | None,
) -> Generator[tuple[int, float, np.ndarray], None, None]:
    """
    Generator: yield (frame_index, timestamp_sec, gray_uint8_ndarray).

    Xử lý 2 pix_fmt output:
      gray  → 1 byte/pixel, reshape (H, W) trực tiếp
      nv12  → 1.5 byte/pixel (Y plane = H×W, UV plane = H/2×W interleaved)
               → chỉ lấy Y plane (first H*W bytes) = grayscale

    Timestamp: frame_idx * sample_interval tính từ đầu video (relative).
    chunk_start được cộng vào ở caller để ra wall-clock time.
    """
    src_w, src_h = _get_video_resolution(video_path)

    # Output height giữ AR, làm tròn lên bội 2
    out_h = int(round(src_h * output_width / src_w / 2)) * 2
    out_h = max(out_h, 2)

    cmd, pix_fmt = _build_ffmpeg_cmd(video_path, hwaccel, sample_interval, output_width)

    if pix_fmt == "nv12":
        # nv12: Y (W×H bytes) + UV (W×H/2 bytes) = W×H×1.5
        frame_bytes = output_width * out_h * 3 // 2
        y_bytes     = output_width * out_h       # chỉ cần Y-plane
    else:
        # gray: W×H bytes
        frame_bytes = output_width * out_h
        y_bytes     = frame_bytes

    logger.debug(
        f"FFmpeg: {' '.join(cmd)}\n"
        f"  output: {output_width}×{out_h} {pix_fmt} "
        f"({frame_bytes} bytes/frame, y_bytes={y_bytes})"
    )

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=frame_bytes * 8,
    )

    frame_idx = 0
    try:
        while True:
            if cancel_event and cancel_event.is_set():
                break

            raw = proc.stdout.read(frame_bytes)
            if len(raw) < frame_bytes:
                break

            # Extract Y-plane (grayscale)
            gray = np.frombuffer(raw[:y_bytes], dtype=np.uint8).reshape((out_h, output_width))
            ts_sec = frame_idx * sample_interval
            yield frame_idx, ts_sec, gray
            frame_idx += 1
    finally:
        proc.stdout.close()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def detect_video_fast(
    video_path: Path,
    cam_id: int,
    chunk_start: datetime,
    chunk_end: datetime,
    cancel_event: threading.Event | None = None,
    hwaccel: str | None = None,
    sample_interval: float = SAMPLE_INTERVAL,
    output_width: int = OUTPUT_WIDTH,
) -> list[dict]:
    """
    Detect QR trong video bằng FFmpeg hwaccel + filter graph.

    Thay thế cho _detect_video() trong detect.py.
    Trả về list[dict] scan giống format cũ.
    """
    if hwaccel is None:
        hwaccel = _detect_hwaccel(probe_file=video_path)

    duration = _get_video_duration(video_path)
    expected_frames = int(duration / sample_interval) + 1

    logger.info(
        f"[CAM {cam_id}] {video_path.name} | {duration:.0f}s | "
        f"hwaccel={hwaccel or 'cpu'} | sample={sample_interval}s "
        f"→ ~{expected_frames} frames"
    )

    scans: list[dict] = []
    last_seen: dict[str, datetime] = {}  # qr_value → last detected_at

    t0 = time.perf_counter()
    sampled = 0

    for frame_idx, ts_sec, gray_frame in _frame_reader(
        video_path, hwaccel, sample_interval, output_width, cancel_event
    ):
        if cancel_event and cancel_event.is_set():
            logger.warning(f"[CAM {cam_id}] detect cancelled at frame {frame_idx}")
            break

        sampled += 1
        detected_at = chunk_start + timedelta(seconds=ts_sec)

        try:
            results = zxingcpp.read_barcodes(
                gray_frame, formats=zxingcpp.BarcodeFormat.QRCode
            )
        except Exception as e:
            logger.debug(f"[CAM {cam_id}] zxing error frame {frame_idx}: {e}")
            continue

        for r in results:
            qr = r.text
            if not qr or not is_valid_qr(qr):
                continue

            prev = last_seen.get(qr)
            if prev is not None and (detected_at - prev).total_seconds() < QR_COOLDOWN_SEC:
                continue

            last_seen[qr] = detected_at
            shift = get_shift(detected_at)

            scans.append({
                "cam_id":      cam_id,
                "qr_value":    qr,
                "detected_at": detected_at.isoformat(),
                "chunk_start": chunk_start.isoformat(),
                "chunk_end":   chunk_end.isoformat(),
                "shift":       shift,
            })
            logger.info(
                f"[CAM {cam_id}] QR: {qr} @ {detected_at:%H:%M:%S} "
                f"(frame {frame_idx}, {shift})"
            )

    elapsed = time.perf_counter() - t0
    logger.info(
        f"[CAM {cam_id}] {video_path.name} done | "
        f"{sampled} frames in {elapsed:.1f}s "
        f"({sampled/elapsed:.1f} fps effective) | {len(scans)} QR(s)"
    )
    return scans


# ─────────────────────────────────────────────────────────────────────────────
# Bulk batch runner dùng multiprocessing.Pool
# ─────────────────────────────────────────────────────────────────────────────

def _worker_fn(args: tuple) -> tuple[str, list[dict], float]:
    """
    Hàm chạy trong subprocess worker.
    Trả về (video_path_str, scans, elapsed_sec).
    """
    (
        video_path_str,
        cam_id,
        chunk_start_iso,
        chunk_end_iso,
        hwaccel,
        sample_interval,
        output_width,
    ) = args

    video_path  = Path(video_path_str)
    chunk_start = datetime.fromisoformat(chunk_start_iso)
    chunk_end   = datetime.fromisoformat(chunk_end_iso)

    t0 = time.perf_counter()
    scans = detect_video_fast(
        video_path, cam_id, chunk_start, chunk_end,
        hwaccel=hwaccel,
        sample_interval=sample_interval,
        output_width=output_width,
    )
    elapsed = time.perf_counter() - t0
    return video_path_str, scans, elapsed


def run_bulk(
    segments: list[dict],
    num_workers: int = 6,
    hwaccel: str | None = None,
    sample_interval: float = SAMPLE_INTERVAL,
    output_width: int = OUTPUT_WIDTH,
) -> list[dict]:
    """
    Chạy detect song song trên nhiều segment.

    segments: list of {
        "video_path": str,
        "cam_id": int,
        "chunk_start": str (ISO),
        "chunk_end": str (ISO),
    }

    Trả về list[dict] tổng hợp tất cả scans.
    """
    if hwaccel is None:
        hwaccel = _detect_hwaccel()

    task_args = [
        (
            seg["video_path"],
            seg["cam_id"],
            seg["chunk_start"],
            seg["chunk_end"],
            hwaccel,
            sample_interval,
            output_width,
        )
        for seg in segments
    ]

    all_scans: list[dict] = []
    t0 = time.perf_counter()
    done = 0

    # spawn context để tránh fork-deadlock trên Linux với CUDA
    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=num_workers) as pool:
        for video_path_str, scans, elapsed in pool.imap_unordered(_worker_fn, task_args):
            all_scans.extend(scans)
            done += 1
            total_elapsed = time.perf_counter() - t0
            remaining = len(segments) - done
            avg = total_elapsed / done
            eta_h = remaining * avg / 3600
            logger.info(
                f"[BULK] {done}/{len(segments)} done | "
                f"last={elapsed:.1f}s | ETA {eta_h:.1f}h"
            )

    total = time.perf_counter() - t0
    logger.info(
        f"[BULK] Finished {len(segments)} segments in "
        f"{total/3600:.2f}h | {len(all_scans)} total QRs"
    )
    return all_scans


# ─────────────────────────────────────────────────────────────────────────────
# Timeline builder: merge scan list → khoảng có / không có QR
# ─────────────────────────────────────────────────────────────────────────────

def build_timeline(scans: list[dict], gap_threshold_sec: float = 60.0) -> list[dict]:
    """
    Gom các scan gần nhau (< gap_threshold_sec) thành 1 khoảng QR liên tục.
    Trả về list khoảng:
      {"qr_value": str, "start": ISO, "end": ISO, "duration_sec": float}
    """
    if not scans:
        return []

    sorted_scans = sorted(scans, key=lambda s: s["detected_at"])
    timeline: list[dict] = []
    cur = dict(sorted_scans[0])
    cur_end = cur["detected_at"]

    for scan in sorted_scans[1:]:
        dt_prev = datetime.fromisoformat(cur_end)
        dt_cur  = datetime.fromisoformat(scan["detected_at"])
        gap = (dt_cur - dt_prev).total_seconds()

        if scan["qr_value"] == cur["qr_value"] and gap <= gap_threshold_sec:
            cur_end = scan["detected_at"]
        else:
            dt_start = datetime.fromisoformat(cur["detected_at"])
            dt_end   = datetime.fromisoformat(cur_end)
            timeline.append({
                "qr_value":    cur["qr_value"],
                "cam_id":      cur["cam_id"],
                "start":       cur["detected_at"],
                "end":         cur_end,
                "duration_sec": (dt_end - dt_start).total_seconds(),
                "shift":       cur.get("shift"),
            })
            cur     = dict(scan)
            cur_end = scan["detected_at"]

    dt_start = datetime.fromisoformat(cur["detected_at"])
    dt_end   = datetime.fromisoformat(cur_end)
    timeline.append({
        "qr_value":    cur["qr_value"],
        "cam_id":      cur["cam_id"],
        "start":       cur["detected_at"],
        "end":         cur_end,
        "duration_sec": (dt_end - dt_start).total_seconds(),
        "shift":       cur.get("shift"),
    })

    return timeline


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark / diagnostic: tách T_decode, T_resize, T_zxing
# ─────────────────────────────────────────────────────────────────────────────

def benchmark_phases(video_path: Path, max_frames: int = 500):
    """
    Đo riêng từng phase bằng cách mở VideoCapture độc lập cho mỗi phase.
    Tránh lỗi seek HEVC và cache warmup khi dùng chung cap.

    Mỗi phase đọc từ frame 0 trên cap mới → số đo tuyệt đối, không phải delta.
    T_resize và T_zxing được tính bằng cách timed từng op bên trong loop.
    """
    import cv2

    BURST, SKIP, CYCLE = 2, 3, 5

    def _open(path: Path) -> cv2.VideoCapture:
        c = cv2.VideoCapture(str(path))
        if not c.isOpened():
            raise RuntimeError(f"Cannot open: {path}")
        return c

    print(f"\n{'='*60}")
    print(f"Benchmark phases: {video_path.name} (first {max_frames} frames)")
    print(f"{'='*60}")

    # ── Phase 1: grab()-only — decode floor ──────────────────────────────────
    cap1 = _open(video_path)
    t0 = time.perf_counter()
    n1 = 0
    while n1 < max_frames:
        if not cap1.grab():
            break
        n1 += 1
    t_grab_total = time.perf_counter() - t0
    cap1.release()
    t_decode_per = t_grab_total / max(n1, 1) * 1000  # ms/frame

    # ── Phase 2: grab()+retrieve() theo CYCLE; đo retrieve overhead riêng ────
    cap2 = _open(video_path)
    t_retrieve_accum = 0.0
    n2 = sampled2 = 0
    while n2 < max_frames:
        if n2 % CYCLE >= BURST:
            if not cap2.grab():
                break
        else:
            tr0 = time.perf_counter()
            ret, _ = cap2.read()
            t_retrieve_accum += time.perf_counter() - tr0
            if not ret:
                break
            sampled2 += 1
        n2 += 1
    cap2.release()
    # retrieve cost = total_retrieve - grab cost for those sampled frames
    t_retrieve_per = (
        t_retrieve_accum / max(sampled2, 1) * 1000
        - t_decode_per  # subtract the grab that read() includes
    )

    # ── Phase 3: timed resize+cvtColor per sampled frame ─────────────────────
    cap3 = _open(video_path)
    t_resize_accum = 0.0
    n3 = sampled3 = 0
    while n3 < max_frames:
        if n3 % CYCLE >= BURST:
            if not cap3.grab():
                break
        else:
            ret, frame = cap3.read()
            if not ret:
                break
            tr0 = time.perf_counter()
            h, w = frame.shape[:2]
            if w > 960:
                frame = cv2.resize(frame, (960, int(h * 960 / w)))
            cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            t_resize_accum += time.perf_counter() - tr0
            sampled3 += 1
        n3 += 1
    cap3.release()
    t_resize_per = t_resize_accum / max(sampled3, 1) * 1000

    # ── Phase 4: timed zxingcpp per sampled frame ─────────────────────────────
    cap4 = _open(video_path)
    t_zxing_accum = 0.0
    n4 = sampled4 = 0
    while n4 < max_frames:
        if n4 % CYCLE >= BURST:
            if not cap4.grab():
                break
        else:
            ret, frame = cap4.read()
            if not ret:
                break
            h, w = frame.shape[:2]
            if w > 960:
                frame = cv2.resize(frame, (960, int(h * 960 / w)))
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            tz0 = time.perf_counter()
            zxingcpp.read_barcodes(gray, formats=zxingcpp.BarcodeFormat.QRCode)
            t_zxing_accum += time.perf_counter() - tz0
            sampled4 += 1
        n4 += 1
    cap4.release()
    t_zxing_per = t_zxing_accum / max(sampled4, 1) * 1000

    # ── Extrapolate to 30-min segment ─────────────────────────────────────────
    total_frames_30min  = 45_000
    total_sampled_30min = total_frames_30min // CYCLE * BURST  # 18,000

    t_decode_30min = t_decode_per  * total_frames_30min  / 1000 / 60
    t_resize_30min = t_resize_per  * total_sampled_30min / 1000 / 60
    t_zxing_30min  = t_zxing_per   * total_sampled_30min / 1000 / 60
    t_total_30min  = t_decode_30min + t_resize_30min + t_zxing_30min

    pct = lambda v: v / max(t_total_30min, 0.001) * 100

    print(f"\n  Frames measured : {n1} grab / {sampled3} sampled (BURST={BURST} SKIP={SKIP})")
    print(f"")
    print(f"  T_decode  /frame  = {t_decode_per:6.2f} ms  → 30min ≈ {t_decode_30min:5.1f} min  ({pct(t_decode_30min):3.0f}%)")
    print(f"  T_retrieve/sample = {t_retrieve_per:6.2f} ms  (overhead của retrieve vs grab)")
    print(f"  T_resize  /sample = {t_resize_per:6.2f} ms  → 30min ≈ {t_resize_30min:5.1f} min  ({pct(t_resize_30min):3.0f}%)")
    print(f"  T_zxing   /sample = {t_zxing_per:6.2f} ms  → 30min ≈ {t_zxing_30min:5.1f} min  ({pct(t_zxing_30min):3.0f}%)")
    print(f"  {'─'*53}")
    print(f"  T_total estimated ≈ {t_total_30min:5.1f} min / segment")
    print()

    dec_pct = pct(t_decode_30min)
    zx_pct  = pct(t_zxing_30min)

    if dec_pct > 60:
        print(f"  → DECODE chiếm {dec_pct:.0f}% — bottleneck chính.")
        print(f"     Dùng FFmpeg hwaccel (cuda/qsv). Tăng SKIP sẽ KHÔNG giúp.")
        if t_total_30min < 5:
            print(f"     NOTE: segment ngắn hơn 30min hoặc decode nhanh hơn dự kiến —")
            print(f"     kiểm tra lại FPS thực tế và codec bằng ffprobe.")
    elif zx_pct > 40:
        print(f"  → ZXING chiếm {zx_pct:.0f}% — giảm OUTPUT_WIDTH hoặc dùng ROI.")
    else:
        print(f"  → RESIZE chiếm phần lớn — giảm OUTPUT_WIDTH.")

    print()
    print(f"  Scaling estimate (sau hwaccel ~5× decode speedup):")
    t_fast = t_decode_30min / 5 + t_resize_30min + t_zxing_30min
    for w in [3, 6, 9, 18]:
        eta_h = 924 * t_fast / w / 60
        print(f"    {w:>3} workers → {t_fast:.1f} min/seg → 924 segs = {eta_h:.1f}h")

    return {
        "t_decode_ms":       t_decode_per,
        "t_retrieve_ms":     t_retrieve_per,
        "t_resize_ms":       t_resize_per,
        "t_zxing_ms":        t_zxing_per,
        "t_total_30min_est": t_total_30min,
        "frames_measured":   n1,
        "sampled_measured":  sampled3,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import json
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Fast QR detect via FFmpeg hwaccel pipe"
    )
    subp = parser.add_subparsers(dest="cmd")

    # detect: chạy detect 1 file
    p_det = subp.add_parser("detect", help="Detect QR trong 1 file video")
    p_det.add_argument("video", help="Đường dẫn file MP4")
    p_det.add_argument("--cam-id", type=int, default=0)
    p_det.add_argument("--start", default="2024-01-01T00:00:00", help="chunk_start ISO")
    p_det.add_argument("--end",   default="2024-01-01T00:30:00", help="chunk_end ISO")
    p_det.add_argument("--hwaccel", default=None, help="cuda|qsv|vaapi|'' (auto)")
    p_det.add_argument("--sample", type=float, default=SAMPLE_INTERVAL)
    p_det.add_argument("--width",  type=int,   default=OUTPUT_WIDTH)

    # bench: benchmark phases
    p_bench = subp.add_parser("bench", help="Đo bottleneck từng phase")
    p_bench.add_argument("video", help="Đường dẫn file MP4")
    p_bench.add_argument("--frames", type=int, default=500)

    # bulk: chạy bulk từ JSON file
    p_bulk = subp.add_parser("bulk", help="Bulk detect từ JSON segment list")
    p_bulk.add_argument("json_file", help="JSON file chứa list segment")
    p_bulk.add_argument("--workers", type=int, default=6)
    p_bulk.add_argument("--hwaccel", default=None)
    p_bulk.add_argument("--sample",  type=float, default=SAMPLE_INTERVAL)
    p_bulk.add_argument("--width",   type=int,   default=OUTPUT_WIDTH)
    p_bulk.add_argument("--out",     default="scans_out.json")

    args = parser.parse_args()

    if args.cmd == "bench":
        benchmark_phases(Path(args.video), max_frames=args.frames)

    elif args.cmd == "detect":
        scans = detect_video_fast(
            video_path=Path(args.video),
            cam_id=args.cam_id,
            chunk_start=datetime.fromisoformat(args.start),
            chunk_end=datetime.fromisoformat(args.end),
            hwaccel=args.hwaccel,
            sample_interval=args.sample,
            output_width=args.width,
        )
        timeline = build_timeline(scans)
        print(f"\n{len(scans)} scan(s), {len(timeline)} timeline range(s)")
        for t in timeline:
            print(f"  {t['start']} → {t['end']}  ({t['duration_sec']:.0f}s)  {t['qr_value']}")

    elif args.cmd == "bulk":
        with open(args.json_file, encoding="utf-8") as f:
            segments = json.load(f)
        all_scans = run_bulk(
            segments,
            num_workers=args.workers,
            hwaccel=args.hwaccel,
            sample_interval=args.sample,
            output_width=args.width,
        )
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(all_scans, f, ensure_ascii=False, indent=2)
        print(f"\nTotal: {len(all_scans)} QR(s) → {args.out}")
        timeline = build_timeline(all_scans)
        print(f"Timeline: {len(timeline)} range(s)")

    else:
        parser.print_help()
        sys.exit(1)
