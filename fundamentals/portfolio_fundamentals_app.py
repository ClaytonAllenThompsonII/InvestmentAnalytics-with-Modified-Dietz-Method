import streamlit as st

# Import subpages
from pages.income_statement import render_income_statement_page
# from pages.cash_flow import render_cash_flow_page
# from pages.balance_sheet import render_balance_sheet_page
# from pages.earnings_estimates import render_earnings_estimates_page

st.set_page_config(
    page_title="Portfolio Company Fundamentals | Global Equities",
    layout="wide"
)

# === MAIN HEADER ===
st.title("📊 Portfolio Company Fundamentals")
st.caption("Financial statement dashboard for your portfolio holdings")

# === SIDEBAR ===
st.sidebar.markdown("### 🧾 Statements")
st.sidebar.markdown("Select a section below:")

# Custom menu with buttons
menu_options = {
    "📈 Income Statement": "income",
    "💵 Cash Flow": "cash_flow",
    "📊 Balance Sheet": "balance",
    "🔮 Earnings Forecasts": "forecast"
}

selected_page = st.session_state.get("selected_page", "income")

for label, key in menu_options.items():
    if st.sidebar.button(label, key=key):
        st.session_state.selected_page = key
        selected_page = key

# === RENDER PAGE ===
if selected_page == "income":
    render_income_statement_page()
elif selected_page == "cash_flow":
    st.info("🚧 Cash Flow Statement page coming soon.")
elif selected_page == "balance":
    st.info("🚧 Balance Sheet page coming soon.")
elif selected_page == "forecast":
    st.info("🚧 Earnings Forecasts page coming soon.")

# streamlit run fundamentals/portfolio_fundamentals_app.py