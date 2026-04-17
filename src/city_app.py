"""
city_app.py — DEPRECATED as of Phase 1 (April 2026).

All citywide functionality is now served through app.py. This standalone app
is retained only as a historical reference and will be removed in Phase 2.

To run the citywide dashboard, launch:
    streamlit run src/app.py

The citywide tab is the default landing page in app.py.
"""

from __future__ import annotations
__version__ = "DEPRECATED-2026-04-15"

import streamlit as st

st.set_page_config(page_title="eBus Scheduler — Deprecated", page_icon="⚠️", layout="wide")

st.markdown(
    """
    # ⚠️ city_app.py is deprecated

    As of **April 2026 (Phase 1)**, all citywide functionality has been consolidated into `app.py`.

    ### To run the scheduler:
    ```bash
    streamlit run src/app.py
    ```

    The citywide dashboard is now the default landing page. All features that were previously
    in this app — route uploading, fleet rebalancing, per-route drill-down, and mode selection
    — are available in the unified `app.py`.

    ### Why this change?
    - Single deployment artifact (simpler Streamlit Cloud setup)
    - Unified session state across single-route and citywide views
    - Reduced maintenance surface (~500 lines removed)
    - Shared styling and components

    ### For developers:
    This file will be removed entirely in Phase 2 (Weeks 3–6). If you are currently using
    `streamlit run src/city_app.py`, please update your deployment to use `app.py`.
    """
)

st.stop()
