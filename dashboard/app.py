"""
dashboard/app.py
Streamlit dashboard — chạy: streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(
    page_title="Hikvision QR Dashboard",
    page_icon="📷",
    layout="wide",
)

st.title("📷 Hikvision QR Scanner Dashboard")
st.markdown("""
Hệ thống audit log QR từ camera NVR Hikvision.

Điều hướng qua menu bên trái:
- 📊 **Overview** — thống kê tổng quan
- 🔍 **Tìm QR** — tra cứu + tải footage on-demand
- 🎥 **Chi tiết Camera** — lịch sử từng cam
- ⚙️ **Quản lý Camera** — thêm / xóa camera
""")