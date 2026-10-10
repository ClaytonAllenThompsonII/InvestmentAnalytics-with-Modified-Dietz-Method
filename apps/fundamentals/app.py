import streamlit as st

from views.income_statement import render_income_statement_page


st.set_page_config(
    page_title="Fundamentals",
    layout="wide",
)

st.title("Fundamentals")

page = st.sidebar.radio(
    "View",
    [
        "Income Statement",
        "Balance Sheet",
        "Cash Flow",
        "Earnings",
        "Valuation",
    ],
)

if page == "Income Statement":
    render_income_statement_page()
elif page == "Balance Sheet":
    st.info("Balance Sheet coming soon.")
elif page == "Cash Flow":
    st.info("Cash Flow coming soon.")
elif page == "Earnings":
    st.info("Earnings coming soon.")
elif page == "Valuation":
    st.info("Valuation coming soon.")