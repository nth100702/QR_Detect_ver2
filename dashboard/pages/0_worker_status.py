"""
dashboard/pages/0_worker_status.py
Xem trạng thái realtime của worker_scheduled:
  - Đang tải ngày nào, ca nào
  - Từng camera: đang download chunk mấy, đang detect, hay đã xong
  - Auto-refresh mỗi 5 giây khi worker đang chạy (dùng st_autorefresh)
  - Controls: Tạm dừng / Tiếp tục / Cancel tất cả task
"""

import os
import httpx
import streamlit as st
from dotenv import load_dotenv
from pathlib import Path

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

API = os.environ.get("API_BASE_URL", "http://localhost:8000")
H   = {"X-Secret": os.environ.get("API_SECRET", "")}

st.set_page_config(page_title="Worker Status", page_icon="⚙️", layout="wide")
st.title("⚙️ Worker Status")

# ── Màu sắc / icon theo trạng thái ──────────────────────────────
STATUS_ICON = {
    "waiting":     "⏳",
    "downloading": "📥",
    "detecting":   "🔍",
    "done":        "✅",
    "error":       "❌",
    "running":     "🔄",
}
STATUS_COLOR = {
    "waiting":     "gray",
    "downloading": "blue",
    "detecting":   "orange",
    "done":        "green",
    "error":       "red",
    "running":     "blue",
}

def badge(status: str) -> str:
    icon  = STATUS_ICON.get(status, "❓")
    color = STATUS_COLOR.get(status, "gray")
    return f":{color}[{icon} {status.upper()}]"


# ── HTTP helpers ─────────────────────────────────────────────────
def fetch_status() -> dict:
    try:
        with httpx.Client(base_url=API, timeout=5) as c:
            r = c.get("/worker/status", headers=H)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_queue() -> dict:
    try:
        with httpx.Client(base_url=API, timeout=5) as c:
            r = c.get("/scan/queue", headers=H)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        return {"error": str(e)}


def post_action(endpoint: str) -> tuple[bool, str]:
    """Gửi POST đến endpoint, trả về (ok, message)."""
    try:
        with httpx.Client(base_url=API, timeout=10) as c:
            r = c.post(endpoint, headers=H)
            r.raise_for_status()
            return True, r.json().get("msg", "OK")
    except httpx.HTTPStatusError as e:
        return False, f"HTTP {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return False, str(e)


# ── Controls: Pause / Resume / Cancel ───────────────────────────
def render_controls(queue_info: dict):
    """Thanh điều khiển worker — Tạm dừng / Tiếp tục / Cancel."""
    paused       = queue_info.get("paused", False)
    active_tasks = queue_info.get("active_tasks", 0)
    pending      = queue_info.get("queue_pending", 0)

    st.subheader("🎛️ Điều khiển Worker")

    # Thông tin queue
    m1, m2, m3 = st.columns(3)
    m1.metric("Task đang chạy", active_tasks)
    m2.metric("Job đang chờ queue", pending)
    m3.metric(
        "Trạng thái trigger",
        "🔴 Tạm dừng" if paused else "🟢 Đang nhận",
    )

    if queue_info.get("error"):
        st.warning(f"Không lấy được queue status: {queue_info['error']}")
        return

    st.write("")  # khoảng cách nhỏ

    b1, b2, b3, _ = st.columns([1, 1, 1.4, 3])

    # Pause / Resume
    if not paused:
        if b1.button("⏸️ Tạm dừng", use_container_width=True, type="secondary"):
            ok, msg = post_action("/scan/pause")
            (st.success if ok else st.error)(msg)
            st.rerun()
    else:
        if b1.button("▶️ Tiếp tục", use_container_width=True, type="primary"):
            ok, msg = post_action("/scan/resume")
            (st.success if ok else st.error)(msg)
            st.rerun()

    # Cancel — chỉ bật nếu có task đang chạy
    cancel_disabled = active_tasks == 0
    with b2:
        if st.button(
            "🛑 Cancel tất cả",
            use_container_width=True,
            type="secondary",
            disabled=cancel_disabled,
            help="Không có task nào đang chạy" if cancel_disabled else
                 f"Cancel ngay {active_tasks} task đang chạy",
        ):
            ok, msg = post_action("/scan/cancel")
            (st.success if ok else st.error)(msg)
            st.rerun()

    # Làm mới thủ công
    if b3.button("🔄 Làm mới ngay", use_container_width=True):
        st.rerun()

    st.divider()


# ── Render worker + camera status ────────────────────────────────
def render_worker(data: dict):
    if "error" in data:
        st.error(f"Không lấy được trạng thái worker: {data['error']}")
        return

    shift   = data.get("shift")
    cameras = data.get("cameras", {})

    if not shift:
        st.info("Worker chưa chạy ca nào. Trạng thái sẽ hiển thị khi worker bắt đầu.")
        return

    status      = shift.get("status", "?")
    shift_name  = shift.get("name", "?")
    target_date = shift.get("target_date", "?")
    started_at  = shift.get("started_at", "?")
    finished_at = shift.get("finished_at")
    total_scans = shift.get("total_scans", 0)

    # Header ca
    col_left, col_right = st.columns([3, 1])
    with col_left:
        st.subheader(
            f"{badge(status)}  Ca **{shift_name.upper()}** — footage ngày **{target_date}**"
        )
        st.caption(
            f"Bắt đầu: {started_at}"
            + (f"  |  Kết thúc: {finished_at}" if finished_at else "")
        )
    with col_right:
        if status == "done":
            st.metric("Tổng QR ghi DB", total_scans)

    st.divider()

    # Từng camera
    if not cameras:
        st.info("Chưa có dữ liệu camera.")
        return

    for cam_id_str, cam in cameras.items():
        cam_status    = cam.get("status", "waiting")
        chunks_total  = cam.get("chunks_total", 0)
        chunks_done   = cam.get("chunks_done", 0)
        current_chunk = cam.get("current_chunk")
        scans_found   = cam.get("scans_found", 0)
        updated_at    = cam.get("updated_at", "")

        with st.container(border=True):
            c1, c2, c3 = st.columns([2, 4, 2])

            c1.markdown(f"**CAM {cam_id_str}**")
            c1.markdown(badge(cam_status))

            with c2:
                if cam_status == "downloading" and chunks_total > 0:
                    pct = chunks_done / chunks_total
                    st.progress(
                        pct,
                        text=f"Chunk {chunks_done}/{chunks_total}"
                        + (f"  ({current_chunk})" if current_chunk else ""),
                    )
                elif cam_status == "detecting":
                    st.progress(1.0, text="Đang detect QR từ video...")
                elif cam_status == "done":
                    st.progress(1.0, text=f"Hoàn tất — {scans_found} QR tìm được")
                elif cam_status == "error":
                    st.error(f"Lỗi: {current_chunk or 'unknown'}")
                elif cam_status == "waiting":
                    st.caption("Đang chờ đến lượt download...")

            c3.caption(f"Cập nhật lúc\n{updated_at}")


# ── Auto-refresh ─────────────────────────────────────────────────
def maybe_autorefresh(worker_running: bool):
    """
    Chỉ auto-refresh khi worker đang chạy.
    Ưu tiên dùng streamlit-autorefresh (không block); fallback thông báo.
    """
    if not worker_running:
        return

    if HAS_AUTOREFRESH:
        # Refresh mỗi 5 giây, đặt ở đầu để không bị block bởi sleep
        st_autorefresh(interval=5_000, key="worker_refresh")
        st.caption("🔄 Tự động làm mới mỗi 5 giây...")
    else:
        # Fallback: hướng dẫn cài package
        st.info(
            "⚠️ Cài `streamlit-autorefresh` để bật tự động làm mới:\n"
            "```\npip install streamlit-autorefresh\n```\n"
            "Hiện tại hãy dùng nút **🔄 Làm mới ngay** bên trên."
        )


# ── Main ─────────────────────────────────────────────────────────
worker_data = fetch_status()
queue_info  = fetch_queue()

# Xác định worker đang chạy để quyết định auto-refresh
shift_status    = (worker_data.get("shift") or {}).get("status", "")
worker_running  = shift_status == "running"

# Auto-refresh đặt TRƯỚC khi render để tránh vòng lặp blocking
maybe_autorefresh(worker_running)

# Controls
render_controls(queue_info)

# Worker + camera status
st.subheader("📊 Trạng thái Worker")
render_worker(worker_data)