import streamlit as st
import pandas as pd
import json
try:
    import _snowflake
except ImportError:
    _snowflake = None

try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except ImportError:
    session = None

# ---------------------------------------------------------------------------
# INLINE SEMANTIC MODEL
# Cortex Analyst reads this YAML to understand table structure and generate SQL.
# No stage upload required — passed directly in the API request body.
# ---------------------------------------------------------------------------
SEMANTIC_MODEL = """
name: covid_epidemiological_intelligence
description: "Semantic model for COVID-19 epidemiological data enabling natural-language SQL generation across outbreak trends, forecasts, and demographic context."

tables:
  - name: COVID_FEATURES
    description: "7-day smoothed COVID-19 confirmed case counts and doubling ratios per country and date."
    base_table:
      database: HACKATHON
      schema: DATA
      table: COVID_FEATURES
    dimensions:
      - name: country
        expr: COUNTRY
        description: "Country name (e.g. United States, India, Brazil, France, Japan, Italy, United Kingdom, Germany, Spain, Canada, South Korea)"
        type: string
        unique: false
      - name: date
        expr: DATE
        description: "Reporting date of the observation"
        type: date
    measures:
      - name: smoothed_daily_cases
        expr: SMOOTHED_CASES
        description: "7-day rolling average of confirmed daily COVID-19 cases"
        default_aggregation: avg
      - name: doubling_ratio
        expr: DOUBLING_RATIO
        description: "Ratio of current smoothed cases vs 7 days ago. Values above 1.15 = HIGH RISK, 1.02-1.15 = MODERATE, below 1.02 = LOW RISK"
        default_aggregation: avg

  - name: POLICY_CORRELATION_MATRIX
    description: "COVID epidemiological features enriched with population and demographic context for per-capita analysis."
    base_table:
      database: HACKATHON
      schema: DATA
      table: POLICY_CORRELATION_MATRIX
    dimensions:
      - name: country
        expr: COUNTRY
        description: "Country name"
        type: string
      - name: date
        expr: DATE
        description: "Reporting date"
        type: date
      - name: region
        expr: REGION
        description: "World region: North America, Europe, Asia, South America"
        type: string
      - name: income_group
        expr: INCOME_GROUP
        description: "World Bank income classification: High Income, Upper Middle Income, Lower Middle Income"
        type: string
    measures:
      - name: cases_per_million
        expr: CASES_PER_MILLION
        description: "Smoothed daily confirmed cases per million population — normalised burden metric for fair cross-country comparison"
        default_aggregation: avg
      - name: population
        expr: POPULATION
        description: "Country population"
        default_aggregation: max
      - name: doubling_ratio
        expr: DOUBLING_RATIO
        description: "Case doubling ratio vs 7 days ago"
        default_aggregation: avg

  - name: COVID_MODEL_METRICS
    description: "SNOWFLAKE.ML.FORECAST model evaluation metrics (MAPE, WAPE) per country."
    base_table:
      database: HACKATHON
      schema: DATA
      table: COVID_MODEL_METRICS
    dimensions:
      - name: series
        expr: SERIES
        description: "Country name (matches COVID_FEATURES.COUNTRY)"
        type: string
    measures:
      - name: mean_mape
        expr: MEAN_MAPE
        description: "Mean Absolute Percentage Error of the forecast model. Lower is better. Expressed as a decimal (0.05 = 5%)."
        default_aggregation: avg
      - name: mean_wape
        expr: MEAN_WAPE
        description: "Mean Weighted Absolute Percentage Error"
        default_aggregation: avg
"""

# ---------------------------------------------------------------------------
# PAGE SETUP
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Cortex BI Terminal", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
    .stApp { background-color: #0E1117; color: #E2E8F0; }
    .sql-panel {
        background-color: #1E293B;
        border-left: 4px solid #38BDF8;
        padding: 15px;
        font-family: monospace;
        color: #7DD3FC;
        border-radius: 4px;
        margin: 10px 0;
        font-size: 0.88em;
        white-space: pre-wrap;
        word-break: break-word;
    }
    .summary-box {
        background-color: #0F172A;
        border: 1px solid #334155;
        padding: 15px;
        border-radius: 8px;
        margin-top: 15px;
        color: #CBD5E1;
    }
    .analyst-text {
        background-color: #172033;
        border-left: 3px solid #6366F1;
        padding: 12px 16px;
        border-radius: 4px;
        margin: 8px 0;
        color: #A5B4FC;
        font-style: italic;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

st.title("🏛️ Cortex Analyst: COVID Intelligence Terminal")
st.markdown(
    "Query the **COVID-19 Epidemiological Dataset** in plain English. "
    "Powered by Snowflake **Cortex Analyst** (semantic SQL generation) "
    "and **Arctic LLM** (narrative summarization)."
)

with st.expander("💡 Example questions to try", expanded=False):
    st.markdown("""
- *Which country had the highest cases per million last month?*
- *Show the doubling ratio trend for India*
- *Compare forecast model MAPE accuracy across all countries*
- *Which countries in Asia are currently at high risk?*
- *What is the total smoothed case count for Europe by month?*
- *Which high-income countries have a doubling ratio above 1.15?*
""")

st.markdown("---")

# ---------------------------------------------------------------------------
# BACKEND FUNCTIONS
# ---------------------------------------------------------------------------
def query_cortex_analyst(prompt: str) -> dict:
    """
    Call Snowflake Cortex Analyst REST API with an inline semantic model.
    Returns {"sql": str, "analyst_text": str, "success": bool, "simulated": bool}
    """
    request_body = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
        "semantic_model": SEMANTIC_MODEL,
    }

    if session is None or _snowflake is None:
        # Local dev simulation — returns a plausible SQL for demo purposes
        return {
            "sql": (
                "-- Cortex Analyst simulation (deploy in Snowflake for live SQL)\n"
                "SELECT COUNTRY, ROUND(AVG(DOUBLING_RATIO), 4) AS AVG_DOUBLING_RATIO\n"
                "FROM HACKATHON.DATA.COVID_FEATURES\n"
                "GROUP BY COUNTRY\n"
                "ORDER BY AVG_DOUBLING_RATIO DESC;"
            ),
            "analyst_text": "Showing average doubling ratio per country across all recorded dates.",
            "success": True,
            "simulated": True,
        }

    try:
        response = _snowflake.send_snow_api_request(
            "POST",
            "/api/v2/cortex/analyst/message",
            {},       # headers
            {},       # params
            request_body,
            {},       # request_type_config
            30000,    # timeout_ms
        )

        if response["status"] < 400:
            content = json.loads(response["content"])
            sql, analyst_text = "", ""
            for item in content.get("message", {}).get("content", []):
                if item.get("type") == "sql":
                    sql = item.get("statement", "")
                elif item.get("type") == "text":
                    analyst_text = item.get("text", "")
            return {"sql": sql, "analyst_text": analyst_text, "success": True, "simulated": False}
        else:
            return {"sql": "", "analyst_text": f"API Error {response['status']}", "success": False, "simulated": False}

    except Exception as e:
        return {"sql": "", "analyst_text": str(e), "success": False, "simulated": False}


def execute_sql(sql: str) -> pd.DataFrame:
    """Execute Cortex Analyst-generated SQL and return a DataFrame."""
    if not sql or session is None:
        return pd.DataFrame()
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})


def generate_arctic_summary(user_prompt: str, data_json: str) -> str:
    """Summarize query results using Cortex COMPLETE (snowflake-arctic)."""
    if session is None:
        return (
            "*(Arctic simulation)*: The retrieved epidemiological data directly answers "
            "your query. Review the table above for country-level detail."
        )

    system_prompt = (
        f"You are an epidemiology data analyst reviewing database query results.\n"
        f"The user asked: \"{user_prompt}\"\n"
        f"The database returned this data: {data_json[:2000]}\n"
        f"Write a professional 2-sentence executive summary that directly answers their question "
        f"using ONLY the numbers present in the data. Do not invent or estimate any figures."
    )

    try:
        df = session.sql(
            f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', $${system_prompt}$$) AS R"
        ).to_pandas()
        return df["R"][0]
    except Exception:
        return "*(Arctic fallback)*: Data retrieved successfully. See the table above for detailed results."


# ---------------------------------------------------------------------------
# CHAT STATE
# ---------------------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": (
                "Cortex Analyst Terminal active. Semantic model loaded for COVID-19 "
                "epidemiological data across 11 countries. What would you like to query?"
            ),
            "type": "text",
        }
    ]

# ---------------------------------------------------------------------------
# CHAT RENDERING LOOP
# ---------------------------------------------------------------------------
for msg in st.session_state.messages:
    avatar = "🏛️" if msg["role"] == "assistant" else "👤"
    with st.chat_message(msg["role"], avatar=avatar):
        if msg["type"] == "text":
            st.markdown(msg["content"])
        elif msg["type"] == "data":
            if msg.get("analyst_text"):
                st.markdown(f'<div class="analyst-text">📐 {msg["analyst_text"]}</div>', unsafe_allow_html=True)
            with st.expander("🔍 Generated SQL (Cortex Analyst)", expanded=False):
                st.markdown(f'<div class="sql-panel">{msg["sql"]}</div>', unsafe_allow_html=True)
            st.dataframe(msg["data"], width="stretch", hide_index=True)
            st.markdown(
                f'<div class="summary-box"><strong>🤖 Arctic Insights:</strong> {msg["summary"]}</div>',
                unsafe_allow_html=True,
            )

# ---------------------------------------------------------------------------
# USER INPUT HANDLER
# ---------------------------------------------------------------------------
user_prompt = st.chat_input("Query the COVID epidemiological data in natural language...")

if user_prompt:
    # Store and render user message
    st.session_state.messages.append({"role": "user", "content": user_prompt, "type": "text"})
    with st.chat_message("user", avatar="👤"):
        st.markdown(user_prompt)

    with st.chat_message("assistant", avatar="🏛️"):

        # Step 1: Cortex Analyst — semantic SQL generation
        with st.spinner("Cortex Analyst interpreting semantic model and generating SQL..."):
            result = query_cortex_analyst(user_prompt)

        if result["success"] and result["sql"]:
            sql_query = result["sql"]
            analyst_text = result["analyst_text"]

            if analyst_text:
                st.markdown(f'<div class="analyst-text">📐 {analyst_text}</div>', unsafe_allow_html=True)

            with st.expander("🔍 Generated SQL (Cortex Analyst)", expanded=True):
                st.markdown(f'<div class="sql-panel">{sql_query}</div>', unsafe_allow_html=True)

            if result.get("simulated"):
                st.caption("Simulation mode — paste into Snowflake for live results")

            # Step 2: Execute SQL
            with st.spinner("Executing against Snowflake..."):
                result_df = execute_sql(sql_query)

            st.dataframe(result_df, width="stretch", hide_index=True)

            # Step 3: Arctic summary
            with st.spinner("Synthesizing executive summary with Arctic..."):
                data_json = result_df.to_json(orient="records")
                summary = generate_arctic_summary(user_prompt, data_json)

            st.markdown(
                f'<div class="summary-box"><strong>🤖 Arctic Insights:</strong> {summary}</div>',
                unsafe_allow_html=True,
            )

            # Persist to chat history
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "sql": sql_query,
                    "analyst_text": analyst_text,
                    "data": result_df,
                    "summary": summary,
                    "type": "data",
                }
            )

        else:
            error_text = (
                f"Cortex Analyst could not generate SQL for that query. "
                f"Reason: {result['analyst_text']}. "
                f"Try rephrasing — e.g. 'Show cases per million for Japan'."
            )
            st.warning(error_text)
            st.session_state.messages.append(
                {"role": "assistant", "content": error_text, "type": "text"}
            )
