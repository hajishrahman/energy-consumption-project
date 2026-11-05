import streamlit as st
import pandas as pd
from pathlib import Path

# 1. Page Config (Adds icon to browser tab)
st.set_page_config(
    page_title="Energy Demand Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"

# 2. Header
st.title("⚡ Energy Consumption & Demand Prediction")
st.markdown("An end-to-end Big Data pipeline analyzing **2M+ household power records** using **PySpark**.")

# 3. Sidebar (Clean look)
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg", width=100)
    st.info("Built with PySpark, Airflow, and Scikit-Learn")
    st.divider()
    st.write("**Developer:** HAJISH & HARSHVARDHAN")
    st.write("**Dataset:** UCI Household Power")

# 4. KPI Metrics Cards (Eye-catching numbers)
st.subheader("Key Performance Indicators")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Records", "2.07M", "Processed")
col2.metric("Valid (Silver)", "2.04M", "Clean")
col3.metric("Model MAE", "0.33 kW", "-0.02", delta_color="inverse")
col4.metric("Highest Peak", "11.12 kW", "Feb 2009")

st.divider()

# 5. Tabs
tab1, tab2, tab3 = st.tabs(["OVERVIEW", "PEAK ANALYSIS", "ML FORECAST"])

with tab1:
    st.header("Consumption Trends")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Hourly Profile (Average)")
        hourly_csv = REPORTS / "hourly_profile.csv"
        if hourly_csv.exists():
            df = pd.read_csv(hourly_csv)
            st.line_chart(df.set_index("hour")["avg_kw"], color="#1f77b4")
        else:
            st.warning("hourly_profile.csv not found.")
            
    with col2:
        st.subheader("Daily Trends (First 60 Days)")
        daily_path = ROOT / "data" / "gold" / "daily_summary"
        if daily_path.exists():
            df_daily = pd.read_parquet(daily_path, columns=["date", "avg_power_kw"]).head(60)
            st.line_chart(df_daily.set_index("date")["avg_power_kw"], color="#ff7f0e")
        else:
            st.warning("Gold daily_summary not found.")

with tab2:
    st.header("Peak Demand Analysis")
    st.markdown("The top 10 days with the highest energy spikes.")
    peak_csv = REPORTS / "top_peak_days.csv"
    if peak_csv.exists():
        df_peaks = pd.read_csv(peak_csv)
        
        # Styled dataframe
        st.dataframe(df_peaks.style.highlight_max(subset=["peak_power_kw"], color="#ff4b4b"), 
                     use_container_width=True)
        
        st.bar_chart(df_peaks.set_index("date")["peak_power_kw"], color="#d62728")
    else:
        st.warning("top_peak_days.csv not found.")

with tab3:
    st.header("Demand Prediction Model")
    st.markdown("Random Forest Regressor forecasting the next hour's power consumption.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Actual vs Predicted (Test Set)")
        plot_path = REPORTS / "prediction_plot.png"
        if plot_path.exists():
            st.image(str(plot_path), use_container_width=True)
        else:
            st.warning("prediction_plot.png not found.")
            
    with col2:
        st.subheader("Model Metrics")
        metrics_path = REPORTS / "model_metrics.txt"
        if metrics_path.exists():
            st.code(metrics_path.read_text(), language="text")
            
        st.divider()
        
        st.subheader("Feature Importance")
        feat_path = REPORTS / "feature_importance.txt"
        if feat_path.exists():
            st.code(feat_path.read_text(), language="text")