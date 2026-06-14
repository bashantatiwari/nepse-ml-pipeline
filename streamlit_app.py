import streamlit as st
import pandas as pd
import numpy as np
import os
import json
import joblib
from pathlib import Path
from datetime import datetime, timedelta

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NABIL Stock ML Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { background-color: #0f1117; }

    .metric-card {
        background: linear-gradient(135deg, #1a1d2e 0%, #16213e 100%);
        border: 1px solid #2a2d3e;
        border-radius: 12px;
        padding: 20px 24px;
        text-align: center;
    }
    .metric-label {
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #6b7280;
        margin-bottom: 8px;
    }
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2rem;
        font-weight: 700;
        color: #e2e8f0;
    }
    .metric-delta-up   { font-size: 0.85rem; color: #34d399; }
    .metric-delta-down { font-size: 0.85rem; color: #f87171; }

    .section-title {
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #4f6ef7;
        margin-bottom: 4px;
    }
    .page-title {
        font-size: 1.8rem;
        font-weight: 700;
        color: #e2e8f0;
        margin-bottom: 2px;
    }
    .page-sub {
        font-size: 0.95rem;
        color: #6b7280;
        margin-bottom: 24px;
    }
    .tag {
        display: inline-block;
        background: #1e3a5f;
        color: #60a5fa;
        border-radius: 6px;
        padding: 2px 10px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .status-ok   { color: #34d399; font-weight: 600; }
    .status-warn { color: #fbbf24; font-weight: 600; }
    .divider { border-top: 1px solid #2a2d3e; margin: 24px 0; }
</style>
""", unsafe_allow_html=True)

# ── Data helpers ──────────────────────────────────────────────────────────────
DATA_DIR = Path("data")
PROCESSED_CSV = DATA_DIR / "processed" / "nabil_features.csv"
RAW_CSV       = DATA_DIR / "raw" / "NABIL.csv"
MODELS_DIR    = Path("models")

@st.cache_data(ttl=300)
def load_processed():
    if PROCESSED_CSV.exists():
        df = pd.read_csv(PROCESSED_CSV, parse_dates=["published_date"])
        df = df.sort_values("published_date").reset_index(drop=True)
        return df
    return pd.DataFrame()

@st.cache_data(ttl=300)
def load_raw():
    p = RAW_CSV
    if not p.exists():
        # try alternate locations
        alts = list(DATA_DIR.rglob("NABIL.csv"))
        if alts:
            p = alts[0]
        else:
            return pd.DataFrame()
    df = pd.read_csv(p)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    if "published_date" in df.columns:
        df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")
    df = df.sort_values("published_date").reset_index(drop=True)
    return df

def latest_model_info():
    """Scan models/ for the most recent .pkl and return basic info."""
    if not MODELS_DIR.exists():
        return None
    pkls = sorted(MODELS_DIR.glob("*.joblib"), key=os.path.getmtime, reverse=True)
    if not pkls:
        return None
    p = pkls[0]
    return {
        "name": p.stem,
        "size_kb": round(p.stat().st_size / 1024, 1),
        "trained_at": datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
    }

@st.cache_resource
def load_model_and_metadata():
    """Loads the trained model + its feature list for live predictions."""
    model_path = MODELS_DIR / "best_model.joblib"
    meta_path = MODELS_DIR / "model_metadata.json"
    if not model_path.exists() or not meta_path.exists():
        return None, [], "unknown"
    model = joblib.load(model_path)
    with open(meta_path) as f:
        meta = json.load(f)
    return model, meta.get("features", []), meta.get("model_name", "unknown")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 📈 NABIL ML Pipeline")
    st.markdown("---")
    page = st.radio(
        "Navigate",
        ["🏠 Overview", "📊 Predictions", "🔬 Monitoring"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown('<div class="section-title">Data Source</div>', unsafe_allow_html=True)
    st.markdown('<span class="tag">MariaDB ColumnStore</span>', unsafe_allow_html=True)
    st.markdown('<span class="tag">Redis</span>', unsafe_allow_html=True)
    st.markdown("---")
    st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

# ── Load data ─────────────────────────────────────────────────────────────────
df_proc = load_processed()
df_raw  = load_raw()
model   = latest_model_info()
ml_model, ml_features, ml_model_name = load_model_and_metadata()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "🏠 Overview":
    st.markdown('<div class="section-title">Nepal Stock Exchange · NABIL Bank</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-title">ML Pipeline Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">ColumnStore-backed price prediction & monitoring</div>', unsafe_allow_html=True)

    # KPI row
    col1, col2, col3, col4 = st.columns(4)

    raw_rows  = len(df_raw)  if not df_raw.empty  else 0
    proc_rows = len(df_proc) if not df_proc.empty else 0

    latest_close = None
    delta_str    = ""
    if not df_raw.empty and "close" in df_raw.columns:
        latest_close = df_raw["close"].iloc[-1]
        if len(df_raw) > 1:
            prev = df_raw["close"].iloc[-2]
            pct  = (latest_close - prev) / prev * 100
            sign = "▲" if pct >= 0 else "▼"
            cls  = "metric-delta-up" if pct >= 0 else "metric-delta-down"
            delta_str = f'<div class="{cls}">{sign} {abs(pct):.2f}% vs prev</div>'

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Raw Price Rows</div>
            <div class="metric-value">{raw_rows:,}</div>
        </div>""", unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Processed Features</div>
            <div class="metric-value">{proc_rows:,}</div>
        </div>""", unsafe_allow_html=True)

    with col3:
        val = f"{latest_close:,.1f}" if latest_close else "—"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Latest Close (NPR)</div>
            <div class="metric-value">{val}</div>
            {delta_str}
        </div>""", unsafe_allow_html=True)

    with col4:
        m_name = model["name"] if model else "—"
        m_time = model["trained_at"] if model else "—"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Model</div>
            <div class="metric-value" style="font-size:1rem;padding-top:6px;">{m_name}</div>
            <div style="color:#6b7280;font-size:0.75rem;">Trained {m_time}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Price chart
    if not df_raw.empty and "close" in df_raw.columns:
        st.markdown("#### NABIL Closing Price — Full History")
        chart_df = df_raw[["published_date", "close"]].dropna().set_index("published_date")
        st.line_chart(chart_df, height=300, use_container_width=True)
    else:
        st.info("No price data found. Run the ingestion pipeline first.")

    # Pipeline status
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown("#### Pipeline Component Status")
    c1, c2, c3, c4 = st.columns(4)
    components = [
        ("ColumnStore", raw_rows > 0),
        ("Feature Eng.", proc_rows > 0),
        ("Model", model is not None),
        ("Airflow", True),  # always show as running since we're inside the stack
    ]
    for col, (name, ok) in zip([c1, c2, c3, c4], components):
        status = '<span class="status-ok">● Running</span>' if ok else '<span class="status-warn">● Pending</span>'
        col.markdown(f"**{name}**<br>{status}", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: PREDICTIONS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📊 Predictions":
    st.markdown('<div class="section-title">Model Output</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-title">Price Predictions</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Predicted vs actual NABIL closing prices</div>', unsafe_allow_html=True)

    if df_proc.empty:
        st.warning("No processed data found. Run the pipeline first: trigger `weekly_training_dag` in Airflow.")
    else:
        # Date range filter
        min_date = df_proc["published_date"].min().date()
        max_date = df_proc["published_date"].max().date()
        col1, col2 = st.columns(2)
        with col1:
            start = st.date_input("From", value=max_date - timedelta(days=180), min_value=min_date, max_value=max_date)
        with col2:
            end = st.date_input("To", value=max_date, min_value=min_date, max_value=max_date)

        mask = (df_proc["published_date"].dt.date >= start) & (df_proc["published_date"].dt.date <= end)
        df_view = df_proc[mask].copy()

        if df_view.empty:
            st.info("No data in selected range.")
        else:
            # Actual close chart
            st.markdown("#### Actual Close Price")
            close_df = df_view[["published_date", "close"]].set_index("published_date")
            st.line_chart(close_df, height=250, use_container_width=True)

            # Real model predictions (trained model applied to each row's features)
            df_view_pred = df_view.copy()
            has_model = ml_model is not None and all(f in df_view_pred.columns for f in ml_features)
            if has_model:
                feat_df = df_view_pred[ml_features]
                valid_mask = feat_df.notna().all(axis=1)
                df_view_pred.loc[valid_mask, "model_pred_next_close"] = ml_model.predict(feat_df[valid_mask])

            if has_model and "next_close" in df_view_pred.columns:
                st.markdown(f"#### Model Prediction vs Actual Next-Day Close ({ml_model_name})")
                pred_df = df_view_pred[["published_date", "next_close", "model_pred_next_close"]].dropna().set_index("published_date")
                pred_df.columns = ["Actual Next Close", "Model Predicted Next Close"]
                st.line_chart(pred_df, height=250, use_container_width=True)

                # Real model accuracy metrics
                st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
                st.markdown("#### Prediction Accuracy (Model vs Actual)")
                diff = pred_df["Actual Next Close"] - pred_df["Model Predicted Next Close"]
                mae  = diff.abs().mean()
                rmse = np.sqrt((diff**2).mean())
                mape = (diff.abs() / pred_df["Actual Next Close"]).mean() * 100

                m1, m2, m3 = st.columns(3)
                m1.metric("MAE (NPR)", f"{mae:.2f}")
                m2.metric("RMSE (NPR)", f"{rmse:.2f}")
                m3.metric("MAPE", f"{mape:.2f}%")
            elif not has_model:
                st.info("Trained model not found — showing historical data only. Run `weekly_training_dag` to train a model.")

            # Latest prediction (live model inference on the most recent row)
            st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
            st.markdown("#### Latest Data Point")
            last = df_view_pred.iloc[-1]
            l1, l2, l3 = st.columns(3)
            l1.metric("Date", str(last["published_date"].date()))
            l2.metric("Close (NPR)", f"{last['close']:,.2f}" if pd.notna(last.get("close")) else "—")
            if pd.notna(last.get("model_pred_next_close")):
                delta = last["model_pred_next_close"] - last["close"]
                l3.metric("Predicted Next Close", f"{last['model_pred_next_close']:,.2f}", delta=f"{delta:+.2f}")
            else:
                l3.metric("Predicted Next Close", "—")

            # Raw data table
            with st.expander("View raw data table"):
                st.dataframe(df_view.tail(50), use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: MONITORING
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔬 Monitoring":
    st.markdown('<div class="section-title">MLOps</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-title">Model Monitoring</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Data quality, feature drift, and model health</div>', unsafe_allow_html=True)

    if df_proc.empty:
        st.warning("No processed data available yet.")
    else:
        # Data quality
        st.markdown("#### Data Quality")
        total   = len(df_proc)
        missing = df_proc.isnull().sum().sum()
        complete = round((1 - missing / (total * len(df_proc.columns))) * 100, 1)

        q1, q2, q3 = st.columns(3)
        q1.metric("Total Records", f"{total:,}")
        q2.metric("Missing Values", f"{missing:,}")
        q3.metric("Completeness", f"{complete}%")

        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        # Feature distributions
        st.markdown("#### Feature Distributions")
        numeric_cols = df_proc.select_dtypes(include=[np.number]).columns.tolist()
        exclude = ["next_close", "target_change", "target_pct_change"]
        feature_cols = [c for c in numeric_cols if c not in exclude]

        if feature_cols:
            selected = st.selectbox("Select feature", feature_cols, index=feature_cols.index("close") if "close" in feature_cols else 0)
            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown(f"**{selected} — Time Series**")
                ts = df_proc[["published_date", selected]].dropna().set_index("published_date")
                st.line_chart(ts, height=200, use_container_width=True)
            with col_b:
                st.markdown(f"**{selected} — Distribution**")
                hist_vals = df_proc[selected].dropna().values
                hist_df = pd.DataFrame({"value": hist_vals})
                st.bar_chart(hist_df["value"].value_counts(bins=30).sort_index(), height=200)

        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        # Rolling stats
        st.markdown("#### Rolling Statistics (30-day window)")
        if "close" in df_proc.columns:
            roll_df = df_proc[["published_date", "close"]].set_index("published_date").copy()
            roll_df["Rolling Mean"] = roll_df["close"].rolling(30).mean()
            roll_df["Rolling Std"]  = roll_df["close"].rolling(30).std()
            roll_df = roll_df.dropna()
            st.line_chart(roll_df[["Rolling Mean", "Rolling Std"]], height=220, use_container_width=True)

        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        # Model info
        st.markdown("#### Model Registry")
        if model:
            st.markdown(f"""
            | Field | Value |
            |---|---|
            | Model name | `{model['name']}` |
            | File size | {model['size_kb']} KB |
            | Last trained | {model['trained_at']} |
            | MLflow UI | [http://localhost:5000](http://localhost:5000) |
            """)
        else:
            st.info("No model file found in `models/`. Run `weekly_training_dag` to train.")

        # Pipeline links
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        st.markdown("#### Quick Links")
        c1, c2 = st.columns(2)
        c1.markdown("🔗 [Airflow UI](http://localhost:8080) — Trigger & monitor DAGs")
        c2.markdown("🔗 [MLflow UI](http://localhost:5000) — Experiment tracking")
