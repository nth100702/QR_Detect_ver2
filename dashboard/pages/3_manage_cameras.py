"""
dashboard/pages/4_manage_cameras.py
Quản lý camera: xem, thêm, chỉnh sửa tên/ca, xóa.
"""

import os
import httpx
import streamlit as st

API = os.environ.get("API_BASE_URL", "http://localhost:8000")
H   = {"X-Secret": os.environ.get("API_SECRET", "change_me_to_any_random_string"), "Content-Type": "application/json"}

st.set_page_config(page_title="Quản lý Camera", page_icon="📷", layout="wide")
st.title("📷 Quản lý Camera")


def get_cams():
    with httpx.Client(base_url=API, timeout=10) as c:
        r = c.get("/cameras", headers=H)
        r.raise_for_status()
        return r.json().get("data", [])


def call_api(method: str, path: str, json=None):
    with httpx.Client(base_url=API, timeout=10) as c:
        r = getattr(c, method)(path, json=json, headers=H)
        r.raise_for_status()
        return r.json()


# ── Init session state ───────────────────────────────────────────
if "editing_cam" not in st.session_state:
    st.session_state.editing_cam = None   # cam_id đang edit, None = không edit

# ── Danh sách camera ─────────────────────────────────────────────
st.subheader("Danh sách camera")

try:
    cameras = get_cams()
except Exception as e:
    st.error(f"Không lấy được danh sách camera: {e}")
    st.stop()

if not cameras:
    st.info("Chưa có camera nào.")
else:
    # Header
    hc = st.columns([1, 3, 2, 2, 2, 2])
    for col, label in zip(hc, ["ID", "Tên", "Ca", "NVR Channel", "Trạng thái", "Thao tác"]):
        col.markdown(f"**{label}**")
    st.divider()

    for cam in cameras:
        cid = cam["id"]
        is_editing = st.session_state.editing_cam == cid

        if is_editing:
            # ── Hàng đang edit ────────────────────────────────────
            with st.form(key=f"edit_form_{cid}"):
                ec = st.columns([1, 3, 2, 2, 2, 2])
                ec[0].write(f"**{cid}**")
                new_name  = ec[1].text_input("", value=cam["name"],        label_visibility="collapsed")
                new_shift = ec[2].selectbox("", ["morning", "night"],
                                            index=0 if cam["shift"] == "morning" else 1,
                                            label_visibility="collapsed")
                new_ch    = ec[3].number_input("", value=cam["nvr_channel"],
                                               min_value=1, max_value=64,
                                               label_visibility="collapsed")
                ec[4].write(cam["status"])

                btn_col1, btn_col2 = ec[5].columns(2)
                save   = btn_col1.form_submit_button("💾", help="Lưu")
                cancel = btn_col2.form_submit_button("✖", help="Hủy")

            if save:
                if not new_name.strip():
                    st.error("Tên không được rỗng.")
                else:
                    try:
                        call_api("put", f"/cameras/{cid}", json={
                            "name": new_name.strip(),
                            "shift": new_shift,
                            "nvr_channel": new_ch,
                        })
                        st.success(f"✅ Đã cập nhật CAM {cid}")
                        st.session_state.editing_cam = None
                        st.rerun()
                    except Exception as e:
                        st.error(f"Lỗi: {e}")

            if cancel:
                st.session_state.editing_cam = None
                st.rerun()

        else:
            # ── Hàng bình thường ─────────────────────────────────
            rc = st.columns([1, 3, 2, 2, 2, 2])
            rc[0].write(cid)
            rc[1].write(cam["name"])
            rc[2].write("🌅 Sáng" if cam["shift"] == "morning" else "🌙 Tối")
            rc[3].write(cam["nvr_channel"])

            if cam["status"] == "active":
                rc[4].success("active")
            else:
                rc[4].error("deleted")

            if cam["status"] == "active":
                btn1, btn2 = rc[5].columns(2)
                if btn1.button("✏️", key=f"edit_{cid}", help="Chỉnh sửa"):
                    st.session_state.editing_cam = cid
                    st.rerun()
                if btn2.button("🗑️", key=f"del_{cid}", help="Xóa"):
                    try:
                        call_api("delete", f"/cameras/{cid}")
                        st.success(f"Đã xóa CAM {cid}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Lỗi: {e}")

st.divider()

# ── Thêm camera mới ──────────────────────────────────────────────
st.subheader("➕ Thêm camera mới")

with st.form("add_cam", clear_on_submit=True):
    c1, c2, c3 = st.columns([3, 2, 2])
    name  = c1.text_input("Tên camera *", placeholder="Camera 01 - Cổng vào")
    shift = c2.selectbox("Ca làm việc", ["morning", "night"],
                         format_func=lambda x: "🌅 Sáng (7h)" if x == "morning" else "🌙 Tối (20h)")
    ch    = c3.number_input("NVR Channel *", min_value=1, max_value=64, value=1)

    submitted = st.form_submit_button("➕ Thêm Camera", type="primary")

if submitted:
    if not name.strip():
        st.error("Tên camera không được rỗng.")
    else:
        try:
            result = call_api("post", "/cameras", json={
                "name": name.strip(),
                "shift": shift,
                "nvr_channel": ch,
            })
            st.success(f"✅ Đã thêm **{name.strip()}** — ID: {result['id']}")
            st.rerun()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 422:
                st.error("NVR Channel đã tồn tại. Mỗi channel chỉ được gán cho 1 camera.")
            else:
                st.error(f"Lỗi: {e}")
        except Exception as e:
            st.error(f"Lỗi: {e}")