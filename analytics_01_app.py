# Set up Snowpark active session boilerplate (this works inside Snowflake Streamlit environment)
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except ImportError:
    # Fallback for local testing if needed, though instructions assume running inside Snowflake
    session = None

import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="Workforce Signal", layout="wide", initial_sidebar_state="expanded")

# --- Custom CSS for Styling ---
st.markdown("""
<style>
    .metric-card {
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #0F172A;
    }
    .metric-label {
        font-size: 0.875rem;
        text-transform: uppercase;
        color: #64748B;
        letter-spacing: 0.05em;
        font-weight: 600;
    }
    .metric-high-risk {
        color: #EF4444;
    }
</style>
""", unsafe_allow_html=True)

st.title("🛡️ The Invisible Workforce Signal")
st.markdown("Identify high-value employees at risk of leaving BEFORE they hand in their resignation. "
            "Our proprietary model factors in tenure, performance, and career stagnation.")

# --- DATA FETCHING AND SCORING ALGORITHM ---
# We write the scoring logic directly in SQL so it executes optimally in the Snowflake compute engine.
@st.cache_data(show_spinner=False)
def load_data():
    if session is None:
        # Mocking generic error for local runs to remind users to run in Snowflake
        st.error("Snowflake session not found. Please run this app inside Snowflake Streamlit.")
        return pd.DataFrame()
        
    query = """
    SELECT 
        employee_id as "ID",
        employee_name as "Name",
        department as "Department",
        manager_id as "Manager ID",
        tenure_years as "Tenure (Years)",
        performance_rating as "Performance Rating (1-5)",
        months_since_promotion as "Months Since Promo",
        
        -- PROPRIETARY SCORING ALGORITHM:
        -- Base idea: High performers who are stagnating (high months_since_promotion) are flight risks.
        -- We multiply lack of promotion by their performance rating to strongly penalize ignoring top talent.
        ROUND(((months_since_promotion * 1.5) * (performance_rating / 3.0) + (10 - tenure_years)), 1) AS "Risk Score",
        
        -- TIERING ALGORITHM using SQL CASE:
        CASE 
            WHEN ((months_since_promotion * 1.5) * (performance_rating / 3.0) + (10 - tenure_years)) >= 45 THEN 'High'
            WHEN ((months_since_promotion * 1.5) * (performance_rating / 3.0) + (10 - tenure_years)) >= 25 THEN 'Medium'
            ELSE 'Low'
        END AS "Risk Tier"
        
    FROM HACKATHON.DATA.HR_ATTRITION
    """
    df = session.sql(query).to_pandas()
    return df

df = load_data()

if not df.empty:
    # --- SIDEBAR FILTERS ---
    st.sidebar.header("Filter Roster")
    departments = ["All"] + sorted(df["Department"].unique().tolist())
    selected_dept = st.sidebar.selectbox("Select Department:", departments)
    
    # Filter dataset
    if selected_dept != "All":
        filtered_df = df[df["Department"] == selected_dept]
    else:
        filtered_df = df
        
    # --- TOP KPI CARDS ---
    total_emp = len(filtered_df)
    high_risk_count = len(filtered_df[filtered_df["Risk Tier"] == "High"])
    avg_score = filtered_df["Risk Score"].mean()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Employees</div>
            <div class="metric-value">{total_emp}</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">High Flight Risk Employees</div>
            <div class="metric-value metric-high-risk">{high_risk_count}</div>
        </div>
        """, unsafe_allow_html=True)
        
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Avg Department Risk Score</div>
            <div class="metric-value">{avg_score:.1f}</div>
        </div>
        """, unsafe_allow_html=True)
        
    st.markdown("---")
    
    # --- VISUALIZATIONS ---
    st.subheader("Risk Tier Distribution")
    
    # Using Altair for a polished bar chart
    tier_counts = filtered_df["Risk Tier"].value_counts().reset_index()
    tier_counts.columns = ["Risk Tier", "Employee Count"]
    
    # Ensure correct sort order for visualization
    tier_order = ["High", "Medium", "Low"]
    color_scale = alt.Scale(domain=tier_order, range=["#EF4444", "#F59E0B", "#10B981"])
    
    bar_chart = alt.Chart(tier_counts).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X("Risk Tier:N", sort=tier_order, title=""),
        y=alt.Y("Employee Count:Q", title="Number of Employees"),
        color=alt.Color("Risk Tier:N", scale=color_scale, legend=None),
        tooltip=["Risk Tier", "Employee Count"]
    ).properties(
        height=350
    ).configure_axis(
        grid=False,
        labelFontSize=12,
        titleFontSize=14
    ).configure_view(
        strokeWidth=0
    )
    
    st.altair_chart(bar_chart, use_container_width=True)

    # --- FORENSIC DATA TABLE ---
    st.subheader("High Risk Action List")
    st.markdown("These employees score highly for flight risk based on tenure, top-performance, and prolonged lack of promotion.")
    
    # Sort by explicitly focusing on High risk, then by raw score descending
    display_df = filtered_df.sort_values(by="Risk Score", ascending=False).head(50)
    
    # Simple highlighting function
    def highlight_risk(val):
        color = '#FEE2E2' if val == 'High' else '#FEF3C7' if val == 'Medium' else '#D1FAE5'
        text_col = '#991B1B' if val == 'High' else '#92400E' if val == 'Medium' else '#065F46'
        return f'background-color: {color}; color: {text_col}; font-weight: bold;'
    
    # Apply styler to dataframe
    styled_df = display_df.style.map(highlight_risk, subset=['Risk Tier'])
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Risk Score": st.column_config.ProgressColumn(
                "Risk Score",
                help="Higher score = higher attrition risk",
                format="%.1f",
                min_value=0,
                max_value=100,
            ),
        }
    )
