"""
dashboard/pages/2_search_by_qr.py
- Mặc định: hiển thị tất cả QR trong ngày hôm nay
- Tìm theo QR: lọc từ toàn bộ DB
- Click vào dòng → hiện chi tiết + nút tải footage cho scan đó
"""

import os
import time
import httpx
import streamlit as st
from datetime import date

API_BASE = os.environ.get("API_BASE_URL", "http://localhost:8000")
HEADERS  = {"X-Secret": os.environ.get("API_SECRET", "change_me_to_any_random_string")}
POLL_INTERVAL = 3

st.set_page_config(page_title="Tìm QR", page_icon="🔍", layout="wide")
st.title("🔍 Tìm kiếm QR")

# ─── Form tìm kiếm ──────────────────────────────────────────────
with st.form("search_form"):
    col1, col2, col3 = st.columns([3, 1, 1])
    qr_input    = col1.text_input("Mã QR (để trống = xem tất cả trong ngày)", placeholder="SPXVN...")
    date_input  = col2.date_input("Ngày (chỉ dùng khi không nhập QR)", value=date.today())
    limit_input = col3.selectbox("Số dòng", [50, 100, 200, 500], index=1)
    submitted   = st.form_submit_button("🔍 Tìm")

if submitted:
    st.session_state["search_qr"]      = qr_input.strip()
    st.session_state["search_date"]    = str(date_input)
    st.session_state["search_limit"]   = limit_input
    st.session_state["search_results"] = None
    st.session_state["selected_scan"]  = None
    st.session_state["active_job"]     = None
    st.session_state["job_done"]       = False

# ─── Init session state lần đầu ─────────────────────────────────
if "search_results" not in st.session_state:
    st.session_state["search_qr"]      = ""
    st.session_state["search_date"]    = str(date.today())
    st.session_state["search_limit"]   = 100
    st.session_state["search_results"] = None
    st.session_state["selected_scan"]  = None
    st.session_state["active_job"]     = None
    st.session_state["job_done"]       = False

# ─── Gọi API ────────────────────────────────────────────────────
if st.session_state.get("search_results") is None:
    qr    = st.session_state["search_qr"]
    d     = st.session_state["search_date"]
    limit = st.session_state["search_limit"]

    if qr:
        url        = f"/scans?qr_value={qr}&limit={limit}"
        mode_label = f"QR chứa `{qr}` — toàn bộ DB"
    else:
        url        = f"/scans?date={d}&limit={limit}"
        mode_label = f"Tất cả QR ngày `{d}`"

    with st.spinner("Đang tải dữ liệu..."):
        try:
            with httpx.Client(base_url=API_BASE, timeout=10) as client:
                r = client.get(url, headers=HEADERS)
                r.raise_for_status()
                body = r.json()
                st.session_state["search_results"] = body.get("data", [])
                st.session_state["search_total"]   = body.get("total", 0)
                st.session_state["mode_label"]     = mode_label
        except httpx.HTTPError as e:
            st.error(f"Lỗi API: {e}")
            st.stop()

# ─── Hiển thị kết quả ───────────────────────────────────────────
results     = st.session_state.get("search_results", [])
total_in_db = st.session_state.get("search_total", 0)
mode_label  = st.session_state.get("mode_label", "")

st.caption(mode_label)

col_a, col_b, col_c = st.columns(3)
col_a.metric("Tổng trong DB",  total_in_db)
col_b.metric("Đang hiển thị",  len(results))
col_c.metric("Đã có clip",     sum(1 for r in results if r.get("clip_file")))

if not results:
    st.info("Không có dữ liệu.")
    st.stop()

# ─── Bảng kết quả — có thể chọn dòng ───────────────────────────
st.markdown("**👆 Click vào dòng để xem chi tiết và tải footage**")

table_data = [
    {
        "ID":        r["id"],
        "Camera":    r.get("cam_name", r["cam_id"]),
        "QR":        r["qr_value"],
        "Thời điểm": r["detected_at"][:19].replace("T", " "),
        "Ca":        r.get("shift", ""),
        "Clip":      "✅" if r.get("clip_file") else "—",
    }
    for r in results
]

event = st.dataframe(
    table_data,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
)

# ─── Xử lý dòng được chọn ───────────────────────────────────────
selected_rows = event.selection.get("rows", [])

if not selected_rows:
    st.info("Chọn một dòng trong bảng để xem chi tiết và tải footage.")
    st.stop()

selected_idx  = selected_rows[0]
selected_scan = results[selected_idx]

# Reset job nếu chọn dòng khác
if st.session_state.get("selected_scan") != selected_scan["id"]:
    st.session_state["selected_scan"] = selected_scan["id"]
    st.session_state["active_job"]    = None
    st.session_state["job_done"]      = False

# ─── Chi tiết scan được chọn ────────────────────────────────────
st.divider()
st.subheader("📋 Chi tiết scan")

c1, c2, c3, c4 = st.columns(4)
c1.metric("ID",      selected_scan["id"])
c2.metric("Camera",  selected_scan.get("cam_name", selected_scan["cam_id"]))
c3.metric("Ca",      selected_scan.get("shift", "—"))
c4.metric("Clip",    "✅ Có" if selected_scan.get("clip_file") else "❌ Chưa có")

st.code(selected_scan["qr_value"], language=None)
st.caption(f"Thời điểm detect: {selected_scan['detected_at'][:19].replace('T', ' ')}")

# ─── Nếu đã có clip ─────────────────────────────────────────────
if selected_scan.get("clip_file"):
    st.success(f"✅ Clip đã có: `{selected_scan['clip_file']}`")
    st.stop()

# ─── Chưa có clip → hiện nút tải ────────────────────────────────
st.divider()
st.subheader("📥 Tải footage on-demand")
st.write("Scan này chưa có clip. Nhấn nút để tải footage ±2 phút từ NVR.")

if not st.session_state.get("active_job"):
    if st.button("🚀 Tải footage cho scan này", type="primary"):
        try:
            with httpx.Client(base_url=API_BASE, timeout=15) as client:
                resp = client.post(
                    "/jobs",
                    json={
                        "qr_value": selected_scan["qr_value"],
                        "records": [
                            {
                                "scan_id":     selected_scan["id"],
                                "cam_id":      selected_scan["cam_id"],
                                "detected_at": selected_scan["detected_at"],
                            }
                        ],
                    },
                    headers=HEADERS,
                )
                resp.raise_for_status()
                job = resp.json()
                st.session_state["active_job"] = job["job_id"]
                st.session_state["job_done"]   = False
                st.rerun()
        except httpx.HTTPError as e:
            st.error(f"Lỗi tạo job: {e}")
            st.stop()

# ─── Polling job status ───────────────────────────────────────────
job_id = st.session_state.get("active_job")
if job_id and not st.session_state.get("job_done"):
    status_box   = st.empty()
    progress_bar = st.progress(0)

    for _ in range(200):
        try:
            with httpx.Client(base_url=API_BASE, timeout=10) as client:
                jr = client.get(f"/jobs/{job_id}", headers=HEADERS)
                jr.raise_for_status()
                job_data = jr.json()
        except httpx.HTTPError as e:
            status_box.error(f"Lỗi poll job: {e}")
            break

        status = job_data.get("status", "unknown")
        total  = job_data.get("total", 1)
        done   = job_data.get("done", 0)
        pct    = int(done / total * 100) if total > 0 else 0

        progress_bar.progress(pct)
        status_box.info(f"Job `{job_id[:8]}...` | {status} | {done}/{total} clips ({pct}%)")

        if status == "done":
            clips = job_data.get("clip_files", [])
            st.success(f"✅ Hoàn tất! {len(clips)} clip đã tải về server.")
            st.session_state["job_done"]       = True
            st.session_state["search_results"] = None
            st.rerun()

        if status == "error":
            st.error(f"❌ Job lỗi: {job_data.get('error_msg', '')}")
            break

        time.sleep(POLL_INTERVAL)
    else:
        st.warning("Hết timeout polling. Job vẫn đang chạy — vui lòng reload trang.")