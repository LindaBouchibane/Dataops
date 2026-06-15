import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go

DB_PATH = "warehouse.duckdb"

st.set_page_config(page_title="DataOps Dashboard", page_icon="📊", layout="wide")

st.markdown("""
    <style>
    .metric-container { background-color: #1e1e2e; padding: 1rem; border-radius: 10px; }
    .stTabs [data-baseweb="tab"] { font-size: 16px; }
    </style>
""", unsafe_allow_html=True)

con = duckdb.connect(DB_PATH, read_only=True)
fct_orders    = con.execute("SELECT * FROM main.fct_orders").fetchdf()
dim_customers = con.execute("SELECT * FROM main.dim_customers").fetchdf()
dim_products  = con.execute("SELECT * FROM main.dim_products").fetchdf()
con.close()

fct_orders["order_date"] = fct_orders["order_date"].astype(str)

# ----------------------------------
# SIDEBAR FILTERS
# ----------------------------------

st.sidebar.title("Filters")

countries = ["All"] + sorted(fct_orders["country"].unique().tolist())
selected_country = st.sidebar.selectbox("Country", countries)

segments = ["All"] + sorted(fct_orders["segment"].unique().tolist())
selected_segment = st.sidebar.selectbox("Segment", segments)

statuses = ["All"] + sorted(fct_orders["status"].unique().tolist())
selected_status = st.sidebar.selectbox("Order Status", statuses)

df = fct_orders.copy()
if selected_country != "All":
    df = df[df["country"] == selected_country]
if selected_segment != "All":
    df = df[df["segment"] == selected_segment]
if selected_status != "All":
    df = df[df["status"] == selected_status]

# ----------------------------------
# HEADER
# ----------------------------------

st.title("📊 DataOps Dashboard")
st.caption(f"Showing {len(df)} orders · Filtered from {len(fct_orders)} total")
st.divider()

# ----------------------------------
# KPIs
# ----------------------------------

total_revenue   = df["total_amount"].sum()
total_orders    = len(df)
avg_order_value = df["total_amount"].mean() if total_orders > 0 else 0
anomalies       = (df["total_amount"] < 0).sum()

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 Total Revenue",    f"${total_revenue:,.2f}")
k2.metric("🛒 Total Orders",     total_orders)
k3.metric("📦 Avg Order Value",  f"${avg_order_value:,.2f}")
k4.metric("⚠️ Anomalies",        int(anomalies), delta=f"{anomalies} negative prices", delta_color="inverse")

st.divider()

# ----------------------------------
# TABS
# ----------------------------------

tab1, tab2, tab3 = st.tabs(["🛒 Orders", "👥 Customers", "📦 Products"])

# --- ORDERS TAB ---
with tab1:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Orders by Status")
        status_counts = df["status"].value_counts().reset_index()
        status_counts.columns = ["status", "count"]
        fig = px.pie(status_counts, values="count", names="status",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Revenue by Country")
        rev_country = df.groupby("country")["total_amount"].sum().reset_index()
        fig = px.bar(rev_country, x="country", y="total_amount",
                     color="country", color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Revenue Over Time")
    rev_time = df.groupby("order_date")["total_amount"].sum().reset_index()
    fig = px.line(rev_time, x="order_date", y="total_amount",
                  markers=True, color_discrete_sequence=["#636EFA"])
    fig.update_layout(xaxis_title="Date", yaxis_title="Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

    if anomalies > 0:
        st.warning(f"⚠️ {anomalies} order(s) with negative total amount detected!")
        st.dataframe(df[df["total_amount"] < 0], use_container_width=True)

# --- CUSTOMERS TAB ---
with tab2:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Customers by Segment")
        seg_counts = dim_customers["segment"].value_counts().reset_index()
        seg_counts.columns = ["segment", "count"]
        fig = px.pie(seg_counts, values="count", names="segment",
                     color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Customers by Country")
        country_counts = dim_customers["country"].value_counts().reset_index()
        country_counts.columns = ["country", "count"]
        fig = px.bar(country_counts, x="country", y="count",
                     color="country", color_discrete_sequence=px.colors.qualitative.Pastel)
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Revenue by Segment")
    rev_segment = df.groupby("segment")["total_amount"].sum().reset_index()
    fig = px.bar(rev_segment, x="segment", y="total_amount",
                 color="segment", color_discrete_sequence=px.colors.qualitative.Set2,
                 text_auto=".2s")
    fig.update_layout(showlegend=False, yaxis_title="Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

# --- PRODUCTS TAB ---
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top 10 Products by Revenue")
        top10 = dim_products.sort_values("total_revenue", ascending=False).head(10)
        fig = px.bar(top10, x="total_revenue", y="name", orientation="h",
                     color="category", color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(yaxis=dict(autorange="reversed"), xaxis_title="Revenue ($)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Revenue by Category")
        rev_cat = dim_products.groupby("category")["total_revenue"].sum().reset_index()
        fig = px.pie(rev_cat, values="total_revenue", names="category",
                     color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Cost vs Revenue by Product")
    fig = px.scatter(dim_products, x="cost", y="total_revenue",
                     color="category", size="total_quantity_sold",
                     hover_name="name", color_discrete_sequence=px.colors.qualitative.Set1)
    fig.update_layout(xaxis_title="Unit Cost ($)", yaxis_title="Total Revenue ($)")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------------------
# RAW DATA
# ----------------------------------

st.divider()
with st.expander("🔍 View Raw Data"):
    t1, t2, t3 = st.tabs(["fct_orders", "dim_customers", "dim_products"])
    with t1:
        st.dataframe(df, use_container_width=True)
    with t2:
        st.dataframe(dim_customers, use_container_width=True)
    with t3:
        st.dataframe(dim_products, use_container_width=True)
