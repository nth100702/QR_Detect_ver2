"""
dashboard/pages/5_scan_trigger.py
Trigger scan playback thủ công — chọn cam, ngày, giờ.
Hiển thị lịch sử scan đã trigger trong session.
"""

import os
import httpx
import streamlit as st
from datetime import date, time, timedelta

API = os.environ.get("API_BASE_URL", "http://localhost:8000")
H   = {"X-Secret": os.environ.get("API_SECRET", "change_me_to_any_random_string"), "Content-Type": "application/json"}

st.set_page_config(page_title="Scan Trigger", page_icon="🎯", layout="wide")
st.title("🎯 Trigger Scan Thủ Công")
st.caption("Scan playback bất kỳ ngày + khoảng giờ nào — không cần chờ cron 7h/20h.")

# ── Init session ─────────────────────────────────────────────────
if "trigger_history" not in st.session_state:
    st.session_state.trigger_history = []


# ── Fetch cameras ────────────────────────────────────────────────
@st.cache_data(ttl=30)
def get_cams():
    with httpx.Client(base_url=API, timeout=5) as c:
        r = c.get("/cameras", headers=H)
        r.raise_for_status()
        return [cam for cam in r.json().get("data", []) if cam["status"] == "active"]

try:
    cameras = get_cams()
except Exception as e:
    st.error(f"Không lấy được danh sách camera: {e}")
    st.stop()

if not cameras:
    st.warning("Chưa có camera. Thêm camera tại trang 📷 Quản lý Camera.")
    st.stop()

cam_options = {
    f"CAM {c['id']} — {c['name']} (NVR ch {c['nvr_channel']})": c["id"]
    for c in cameras
}

# ── Form trigger ─────────────────────────────────────────────────
with st.form("trigger_form", clear_on_submit=False):
    st.subheader("Cấu hình scan")

    selected_labels = st.multiselect(
        "Chọn camera",
        options=list(cam_options.keys()),
        default=list(cam_options.keys()),
        help="Giữ Ctrl để chọn nhiều camera",
    )

    col1, col2, col3 = st.columns(3)
    sel_date   = col1.date_input("Ngày footage", value=date.today() - timedelta(days=1))
    start_time = col2.time_input("Từ giờ",   value=time(8, 0))
    end_time   = col3.time_input("Đến giờ",  value=time(19, 0))

    # Preset nhanh
    st.caption("⚡ Preset nhanh — chọn xong nhấn trigger:")
    p1, p2, p3, p4 = st.columns(4)
    preset_full    = p1.form_submit_button("Cả ngày (8h–19h)")
    preset_morning = p2.form_submit_button("Buổi sáng (8h–12h)")
    preset_afternoon = p3.form_submit_button("Buổi chiều (13h–17h)")
    preset_custom  = p4.form_submit_button("🚀 Trigger", type="primary")

    submitted = preset_full or preset_morning or preset_afternoon or preset_custom

    # Ghi đè thời gian nếu dùng preset
    if preset_full:
        start_time = time(8, 0);  end_time = time(19, 0)
    elif preset_morning:
        start_time = time(8, 0);  end_time = time(12, 0)
    elif preset_afternoon:
        start_time = time(13, 0); end_time = time(17, 0)

if submitted:
    errors = []
    if not selected_labels:
        errors.append("Chọn ít nhất 1 camera.")
    if start_time >= end_time:
        errors.append("Giờ bắt đầu phải trước giờ kết thúc.")

    if errors:
        for e in errors:
            st.error(e)
        st.stop()

    cam_ids = [cam_options[l] for l in selected_labels]
    payload = {
        "cam_ids":    cam_ids,
        "date":       str(sel_date),
        "start_time": start_time.strftime("%H:%M"),
        "end_time":   end_time.strftime("%H:%M"),
    }

    try:
        with httpx.Client(base_url=API, timeout=10) as c:
            r = c.post("/scan/trigger", json=payload, headers=H)
            r.raise_for_status()
    except Exception as e:
        st.error(f"Lỗi gửi trigger: {e}")
        st.stop()

    entry = {
        "cam_ids":    cam_ids,
        "date":       str(sel_date),
        "start_time": start_time.strftime("%H:%M"),
        "end_time":   end_time.strftime("%H:%M"),
        "sent_at":    __import__("datetime").datetime.now().strftime("%H:%M:%S"),
    }
    st.session_state.trigger_history.insert(0, entry)

    st.success(
        f"✅ Job đã gửi đến worker!\n\n"
        f"**Cams:** {cam_ids} | **Ngày:** {sel_date} | "
        f"**Giờ:** {start_time.strftime('%H:%M')} – {end_time.strftime('%H:%M')}"
    )
    st.info("Theo dõi tiến độ tại trang **⚙️ Worker Status**.")

# ── Lịch sử trigger trong session ────────────────────────────────
if st.session_state.trigger_history:
    st.divider()
    st.subheader("📋 Lịch sử trigger (session này)")

    for i, entry in enumerate(st.session_state.trigger_history):
        with st.container(border=True):
            c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 1])
            c1.write(f"🕐 {entry['sent_at']}")
            c2.write(f"📅 {entry['date']}")
            c3.write(f"⏱ {entry['start_time']} – {entry['end_time']}")
            c4.write(f"📷 Cam: {entry['cam_ids']}")

            # Re-trigger
            if c5.button("🔁", key=f"retry_{i}", help="Trigger lại"):
                try:
                    with httpx.Client(base_url=API, timeout=10) as c_:
                        r = c_.post("/scan/trigger", json={
                            "cam_ids":    entry["cam_ids"],
                            "date":       entry["date"],
                            "start_time": entry["start_time"],
                            "end_time":   entry["end_time"],
                        }, headers=H)
                        r.raise_for_status()
                    st.toast(f"✅ Đã trigger lại {entry['date']} {entry['start_time']}–{entry['end_time']}")
                except Exception as e:
                    st.error(f"Lỗi: {e}")

    if st.button("🗑 Xóa lịch sử"):
        st.session_state.trigger_history = []
        st.rerun()