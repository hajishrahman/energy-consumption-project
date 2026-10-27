import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Energy Demand Dashboard", layout="wide")
ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

st.title("⚡ Energy Consumption & Demand Prediction")
st.markdown("Interactive dashboard analyzing 2M+ household power records using PySpark.")

tab1, tab2, tab3 = st.tabs(["📊 Overview", "🏆 Peak Analysis", "🤖 ML Forecast"])

with tab1:
    st.header("Consumption Trends")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Hourly Profile (Average)")
        hourly_csv = REPORTS / "hourly_profile.csv"
        if hourly_csv.exists():
            df = pd.read_csv(hourly_csv)
            st.line_chart(df.set_index("hour")["avg_kw"])
        else:
            st.warning("hourly_profile.csv not found.")
            
    with col2:
        st.subheader("Daily Trends (Subset)")
        daily_path = ROOT / "data" / "gold" / "daily_summary"
        if daily_path.exists():
            # Read first 60 days directly using pandas to avoid Spark overhead
            df_daily = pd.read_parquet(daily_path, columns=["date", "avg_power_kw"]).head(60)
            st.line_chart(df_daily.set_index("date")["avg_power_kw"])
        else:
            st.warning("Gold daily_summary not found.")

with tab2:
    st.header("Peak Demand Analysis")
    peak_csv = REPORTS / "top_peak_days.csv"
    if peak_csv.exists():
        df_peaks = pd.read_csv(peak_csv)
        st.dataframe(df_peaks, use_container_width=True)
        st.bar_chart(df_peaks.set_index("date")["peak_power_kw"])
    else:
        st.warning("top_peak_days.csv not found.")

with tab3:
    st.header("Demand Prediction Model")
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Actual vs Predicted (Test Set)")
        plot_path = REPORTS / "prediction_plot.png"
        if plot_path.exists():
            st.image(str(plot_path), use_column_width=True)
        else:
            st.warning("prediction_plot.png not found.")
            
    with col2:
        st.subheader("Model Metrics")
        metrics_path = REPORTS / "model_metrics.txt"
        if metrics_path.exists():
            st.text(metrics_path.read_text())
            
        st.subheader("Feature Importance")
        feat_path = REPORTS / "feature_importance.txt"
        if feat_path.exists():
            st.text(feat_path.read_text())