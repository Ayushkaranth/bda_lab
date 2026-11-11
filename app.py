"""
Frontend - E-Commerce Big Data Analytics dashboard (Streamlit).

Run:  streamlit run app.py
"""
import json
import os
import subprocess
import sys
import time

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import LOG, ROOT, SILVER, WAREHOUSE

st.set_page_config(page_title="E-Commerce Big Data Analytics", page_icon="📊", layout="wide")
px.defaults.template = "plotly_white"
COLORS = px.colors.qualitative.Set2

if not WAREHOUSE.exists():
    st.error("No warehouse found. Run `python -m src.generate_data` and `python -m src.pipeline` first.")
    st.stop()


@st.cache_resource
def connection():
    return duckdb.connect(str(WAREHOUSE), read_only=True)


@st.cache_data(show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return connection().cursor().execute(sql, list(params)).df()


def inr(x) -> str:
    if x is None or pd.isna(x):
        return "-"
    if abs(x) >= 1e7:
        return f"₹{x / 1e7:,.2f} Cr"
    if abs(x) >= 1e5:
        return f"₹{x / 1e5:,.2f} L"
    return f"₹{x:,.0f}"


def show(fig, height=380):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=40, b=10), legend_title_text="")
    st.plotly_chart(fig, width="stretch")


log = json.loads(LOG.read_text()) if LOG.exists() else {}

# ------------------------------------------------------------------ sidebar
st.sidebar.title("📊 E-Commerce BDA")
st.sidebar.caption("Big Data Analytics mini-project · 100% local & free")
PAGES = ["🏠 Overview", "📈 Sales Trends", "📦 Products", "👥 Customer Segments", "🛒 Market Basket",
         "🔻 Clickstream Funnel", "⚙️ Data Pipeline", "🧮 MapReduce Demo", "💻 SQL Console"]
page = st.sidebar.radio("Navigate", PAGES)

opts = q("SELECT list(DISTINCT year ORDER BY year) y, list(DISTINCT category ORDER BY category) c, "
         "list(DISTINCT region ORDER BY region) r, list(DISTINCT channel ORDER BY channel) ch FROM gold.fact_sales").iloc[0]
FILTERED = page in PAGES[:3]
if FILTERED:
    st.sidebar.subheader("Filters")
    f_year = st.sidebar.multiselect("Year", list(opts.y), default=list(opts.y))
    f_cat = st.sidebar.multiselect("Category", list(opts.c), default=list(opts.c))
    f_reg = st.sidebar.multiselect("Region", list(opts.r), default=list(opts.r))
    f_ch = st.sidebar.multiselect("Channel", list(opts.ch), default=list(opts.ch))
    if not (f_year and f_cat and f_reg and f_ch):
        st.warning("Pick at least one value in every filter.")
        st.stop()

    def where():
        parts, params = [], []
        for col, vals in [("year", f_year), ("category", f_cat), ("region", f_reg), ("channel", f_ch)]:
            parts.append(f"{col} IN ({', '.join('?' * len(vals))})")
            params += [v.item() if hasattr(v, "item") else v for v in vals]   # numpy -> plain Python
        return " AND ".join(parts), tuple(params)
    W, PRM = where()

st.sidebar.divider()
if log:
    st.sidebar.caption(f"Pipeline last run: {log['run_at'].replace('T', ' ')}  \n"
                       f"Fact rows: {next(s['rows'] for s in log['steps'] if s['step'].startswith('fact_sales')):,}")

# ------------------------------------------------------------------ pages
if page == "🏠 Overview":
    st.title("Business Overview")
    st.caption("Revenue counts delivered orders only. All numbers respond to the sidebar filters.")
    k = q(f"""SELECT count(DISTINCT order_id) AS orders,
                     sum(line_amount) FILTER (WHERE is_delivered) AS revenue,
                     count(DISTINCT order_id) FILTER (WHERE is_delivered) AS delivered,
                     count(DISTINCT customer_id) AS customers, sum(quantity) AS units,
                     count(DISTINCT order_id) FILTER (WHERE status = 'Cancelled') AS cancelled,
                     count(DISTINCT order_id) FILTER (WHERE status = 'Returned') AS returned
              FROM gold.fact_sales WHERE {W}""", PRM).iloc[0]
    c = st.columns(6)
    c[0].metric("Revenue", inr(k.revenue))
    c[1].metric("Orders", f"{int(k.orders):,}")
    c[2].metric("Customers", f"{int(k.customers):,}")
    c[3].metric("Avg order value", inr(k.revenue / max(k.delivered, 1)))
    c[4].metric("Cancel rate", f"{k.cancelled / max(k.orders, 1):.1%}")
    c[5].metric("Return rate", f"{k.returned / max(k.orders, 1):.1%}")

    m = q(f"""SELECT date_trunc('month', order_date) AS month, sum(line_amount) FILTER (WHERE is_delivered) AS revenue,
                     count(DISTINCT order_id) AS orders
              FROM gold.fact_sales WHERE {W} GROUP BY 1 ORDER BY 1""", PRM)
    fig = px.area(m, x="month", y="revenue", title="Monthly revenue (₹) — note the Oct–Nov festive peaks")
    fig.update_traces(line_color="#2a9d8f")
    show(fig)

    a, b = st.columns(2)
    with a:
        cat = q(f"""SELECT category, sum(line_amount) FILTER (WHERE is_delivered) AS revenue
                    FROM gold.fact_sales WHERE {W} GROUP BY 1 ORDER BY 2""", PRM)
        show(px.bar(cat, x="revenue", y="category", orientation="h", title="Revenue by category",
                    color_discrete_sequence=COLORS))
    with b:
        reg = q(f"""SELECT region, sum(line_amount) FILTER (WHERE is_delivered) AS revenue
                    FROM gold.fact_sales WHERE {W} GROUP BY 1""", PRM)
        show(px.pie(reg, names="region", values="revenue", hole=0.5, title="Revenue share by region",
                    color_discrete_sequence=COLORS))

elif page == "📈 Sales Trends":
    st.title("Sales Trends")
    d = q(f"""SELECT order_date, sum(line_amount) FILTER (WHERE is_delivered) AS revenue
              FROM gold.fact_sales WHERE {W} GROUP BY 1 ORDER BY 1""", PRM)
    d["7-day moving average"] = d.revenue.rolling(7).mean()
    fig = go.Figure([go.Scatter(x=d.order_date, y=d.revenue, name="Daily revenue", line=dict(color="#cfd8dc")),
                     go.Scatter(x=d.order_date, y=d["7-day moving average"], name="7-day moving average",
                                line=dict(color="#e76f51", width=2.5))])
    fig.update_layout(title="Daily revenue with 7-day moving average (time-series smoothing)")
    show(fig)

    a, b = st.columns(2)
    with a:
        h = q(f"""SELECT weekday, hour, count(DISTINCT order_id) AS orders
                  FROM gold.fact_sales WHERE {W} GROUP BY 1, 2""", PRM)
        piv = h.pivot(index="weekday", columns="hour", values="orders").sort_index()
        piv.index = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][: len(piv)]
        show(px.imshow(piv, aspect="auto", color_continuous_scale="YlOrRd",
                       title="Orders by weekday × hour (peak = evenings & weekends)",
                       labels=dict(x="Hour of day", y="", color="Orders")))
    with b:
        p = q(f"""SELECT payment_method, count(DISTINCT order_id) AS orders,
                         count(DISTINCT order_id) FILTER (WHERE status = 'Cancelled') / count(DISTINCT order_id) AS cancel_rate
                  FROM gold.fact_sales WHERE {W} GROUP BY 1 ORDER BY 2 DESC""", PRM)
        fig = px.bar(p, x="payment_method", y="orders", color="cancel_rate", color_continuous_scale="Reds",
                     title="Orders by payment method (colour = cancel rate)")
        show(fig)

    st.subheader("City leaderboard")
    city = q(f"""SELECT city, region, count(DISTINCT order_id) AS orders,
                        sum(line_amount) FILTER (WHERE is_delivered) AS revenue,
                        sum(line_amount) FILTER (WHERE is_delivered) / count(DISTINCT order_id) FILTER (WHERE is_delivered) AS aov
                 FROM gold.fact_sales WHERE {W} GROUP BY 1, 2 ORDER BY revenue DESC""", PRM)
    show(px.bar(city, x="city", y="revenue", color="region", title="Revenue by city",
                color_discrete_sequence=COLORS), 360)
    st.dataframe(city.style.format({"revenue": "₹{:,.0f}", "aov": "₹{:,.0f}", "orders": "{:,}"}),
                 width="stretch", hide_index=True)

elif page == "📦 Products":
    st.title("Product Analytics")
    n = st.slider("Top N products", 5, 30, 15)
    top = q(f"""SELECT p.product_name, f.category, sum(f.line_amount) FILTER (WHERE f.is_delivered) AS revenue,
                       sum(f.quantity) AS units, any_value(p.rating) AS rating
                FROM gold.fact_sales f JOIN gold.product_performance p USING (product_id)
                WHERE {' AND '.join('f.' + x for x in W.split(' AND '))}
                GROUP BY 1, 2 ORDER BY revenue DESC LIMIT {n}""", PRM)
    show(px.bar(top.iloc[::-1], x="revenue", y="product_name", color="category", orientation="h",
                title=f"Top {n} products by revenue", color_discrete_sequence=COLORS), 30 * n + 120)

    a, b = st.columns(2)
    with a:
        tm = q(f"""SELECT category, subcategory, sum(line_amount) FILTER (WHERE is_delivered) AS revenue
                   FROM gold.fact_sales WHERE {W} GROUP BY 1, 2""", PRM)
        show(px.treemap(tm, path=["category", "subcategory"], values="revenue",
                        title="Revenue hierarchy: category → sub-category", color_discrete_sequence=COLORS), 460)
    with b:
        hm = q(f"""SELECT category, month, sum(line_amount) FILTER (WHERE is_delivered) AS revenue
                   FROM gold.fact_sales WHERE {W} GROUP BY 1, 2""", PRM)
        piv = hm.pivot(index="category", columns="month", values="revenue")
        show(px.imshow(piv, aspect="auto", color_continuous_scale="Blues",
                       title="Seasonality: revenue by category × month", labels=dict(x="Month", y="")), 460)

    pp = q("SELECT product_name, category, rating, revenue, units FROM gold.product_performance WHERE revenue > 0")
    show(px.scatter(pp, x="rating", y="revenue", color="category", size="units", log_y=True, opacity=0.6,
                    hover_name="product_name", title="Does customer rating drive revenue? (all products)",
                    color_discrete_sequence=COLORS), 420)

elif page == "👥 Customer Segments":
    st.title("Customer Segmentation — RFM + K-Means")
    km = log.get("kmeans", {})
    st.caption(f"Each delivered-order customer is described by **Recency** (days since last order), **Frequency** "
               f"(orders) and **Monetary** value (₹ spent). K-Means (k = {km.get('k', 4)}) groups "
               f"{km.get('customers', 0):,} customers; silhouette score = {km.get('silhouette', '-')}.")
    s = q("SELECT * FROM gold.segment_summary")
    cols = st.columns(len(s))
    for col, (_, r) in zip(cols, s.iterrows()):
        col.metric(r.segment, f"{r.customers:,} customers", f"avg spend {inr(r.avg_spend)}", delta_color="off")

    a, b = st.columns(2)
    with a:
        show(px.pie(s, names="segment", values="total_spend", hole=0.5, title="Share of total spend by segment",
                    color_discrete_sequence=COLORS))
    with b:
        e = q("SELECT * FROM gold.kmeans_elbow ORDER BY k")
        fig = px.line(e, x="k", y="inertia", markers=True, title="Elbow method — choosing k")
        fig.add_vline(x=4, line_dash="dash", line_color="#e76f51")
        show(fig)

    sc = q("SELECT recency_days, frequency, monetary, segment FROM gold.customer_segments USING SAMPLE 6000 ROWS")
    show(px.scatter(sc, x="recency_days", y="monetary", color="segment", size="frequency", log_y=True, opacity=0.6,
                    title="Customers by recency and spend (sample of 6,000)", color_discrete_sequence=COLORS,
                    labels={"recency_days": "Days since last order", "monetary": "Total spend (₹)"}), 460)

    a, b = st.columns(2)
    with a:
        ag = q("""SELECT segment, CASE WHEN age < 25 THEN '18-24' WHEN age < 35 THEN '25-34'
                         WHEN age < 45 THEN '35-44' WHEN age IS NULL THEN 'Unknown' ELSE '45+' END AS age_group,
                         count(*) AS customers
                  FROM gold.customer_segments GROUP BY 1, 2 ORDER BY 2""")
        show(px.bar(ag, x="age_group", y="customers", color="segment", barmode="group",
                    title="Age profile of each segment", color_discrete_sequence=COLORS))
    with b:
        st.subheader("Segment summary")
        st.dataframe(s, width="stretch", hide_index=True)
        st.markdown("**Suggested actions**  \n"
                    "- **Champions** — loyalty rewards, early access to sales  \n"
                    "- **Promising (Recent)** — onboarding offers to build the habit  \n"
                    "- **At Risk (Big Spenders)** — personalised win-back coupons, fast  \n"
                    "- **Hibernating** — low-cost reactivation emails")

elif page == "🛒 Market Basket":
    st.title("Market Basket Analysis — Association Rules")
    st.caption("Which sub-categories are bought in the same order? Computed over every non-cancelled order with "
               "SQL self-joins (the Apriori idea for item pairs).")
    with st.expander("How to read support, confidence and lift"):
        st.markdown("""
- **Support(A→B)** = orders containing both A and B ÷ all orders
- **Confidence(A→B)** = orders with A and B ÷ orders with A — *if a customer buys A, how likely B?*
- **Lift(A→B)** = confidence ÷ support(B) — **> 1** means A and B are bought together more than by chance
""")
    a, b = st.columns(2)
    min_lift = a.slider("Minimum lift", 1.0, 20.0, 2.0, 0.5)
    min_conf = b.slider("Minimum confidence", 0.0, 1.0, 0.1, 0.05)
    r = q("SELECT * FROM gold.association_rules WHERE lift >= ? AND confidence >= ? ORDER BY lift DESC",
          (min_lift, min_conf))
    st.write(f"**{len(r)} rules** match.")
    if len(r):
        top = r.head(15).copy()
        top["rule"] = top.antecedent + " → " + top.consequent
        show(px.bar(top.iloc[::-1], x="lift", y="rule", orientation="h", color="confidence",
                    color_continuous_scale="Teal", title="Strongest rules by lift"), 520)
        st.dataframe(r.style.format({"support": "{:.4f}", "confidence": "{:.1%}", "lift": "{:.2f}",
                                     "orders_together": "{:,}"}), width="stretch", hide_index=True)
    st.subheader("🎯 Recommendation engine")
    subs = q("SELECT DISTINCT antecedent FROM gold.association_rules ORDER BY 1").antecedent.tolist()
    pick = st.selectbox("Customer added to cart:", subs, index=subs.index("Smartphones") if "Smartphones" in subs else 0)
    rec = q("""SELECT consequent AS recommend, confidence, lift FROM gold.association_rules
               WHERE antecedent = ? AND lift > 1 ORDER BY lift DESC LIMIT 5""", (pick,))
    if len(rec):
        st.success("Frequently bought together: " + ", ".join(
            f"**{x.recommend}** ({x.confidence:.0%} of the time, lift {x.lift:.1f})" for x in rec.itertuples()))
    else:
        st.info("No strong rule for this item.")

elif page == "🔻 Clickstream Funnel":
    st.title("Clickstream Funnel (semi-structured JSON events)")
    f = q("SELECT * FROM gold.funnel")
    allr = f[f.device == "All devices"].iloc[0]
    c = st.columns(4)
    c[0].metric("Sessions", f"{allr.sessions:,}")
    c[1].metric("Added to cart", f"{allr.added_to_cart:,}", f"{allr.added_to_cart / allr.sessions:.1%} of sessions",
                delta_color="off")
    c[2].metric("Purchased", f"{allr.purchased:,}", f"{allr.purchased / allr.sessions:.1%} conversion",
                delta_color="off")
    c[3].metric("Cart abandonment", f"{1 - allr.purchased / allr.added_to_cart:.1%}")
    a, b = st.columns(2)
    with a:
        fig = go.Figure(go.Funnel(y=["Viewed products", "Added to cart", "Purchased"],
                                  x=[allr.sessions, allr.added_to_cart, allr.purchased],
                                  textinfo="value+percent initial", marker_color=["#264653", "#2a9d8f", "#e9c46a"]))
        fig.update_layout(title="Session funnel")
        show(fig)
    with b:
        dev = f[f.device != "All devices"].copy()
        dev["conversion"] = dev.purchased / dev.sessions
        show(px.bar(dev, x="device", y="conversion", text_auto=".1%", title="Conversion rate by device",
                    color="device", color_discrete_sequence=COLORS))
    ev = q("""SELECT hour(event_ts) AS hour, event_type, count(*) AS events
              FROM silver.events GROUP BY 1, 2 ORDER BY 1""")
    show(px.line(ev, x="hour", y="events", color="event_type", markers=True,
                 title="Events per hour of day (read live from the Silver Parquet lake)",
                 color_discrete_sequence=COLORS))
    st.caption("Raw events arrive as nested JSON: `{\"context\": {\"device\", \"os\", \"app_version\"}, "
               "\"geo\": {\"city\"}}`. The Silver layer flattens them into columns.")

elif page == "⚙️ Data Pipeline":
    st.title("Data Pipeline — Medallion Architecture")
    st.graphviz_chart("""
digraph {
  rankdir=LR; node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, fillcolor="#f1f5f9", color="#94a3b8"];
  raw [label="RAW landing\\nCSV + nested JSON\\n(messy source data)"];
  bronze [label="BRONZE\\nParquet copy\\n+ ingestion metadata", fillcolor="#f3e1cf"];
  silver [label="SILVER\\ncleaned · typed · deduped\\npartitioned year/month", fillcolor="#e5e7eb"];
  gold [label="GOLD (DuckDB)\\nfacts · aggregates", fillcolor="#fde68a"];
  ml [label="ML\\nRFM + K-Means"]; rules [label="Association\\nrules"]; mr [label="MapReduce\\n(parallel workers)"];
  ui [label="Streamlit\\ndashboard", fillcolor="#bae6fd"];
  raw -> bronze -> silver -> gold -> ml -> ui; gold -> rules -> ui; silver -> mr -> ui; gold -> ui;
}""")
    if log:
        c = st.columns(4)
        c[0].metric("Pipeline run time", f"{log['total_seconds']} s")
        c[1].metric("CPU cores used", log["cpu_cores"])
        sm = log["storage_mb"]
        c[2].metric("Raw size", f"{sm['Raw (CSV + JSON)']:,} MB")
        c[3].metric("Parquet compression", f"{sm['Raw (CSV + JSON)'] / max(sm['Bronze (Parquet)'], 0.1):.1f}× smaller")
        a, b = st.columns([3, 2])
        with a:
            stp = pd.DataFrame(log["steps"])
            st.subheader("Step log")
            st.dataframe(stp, width="stretch", hide_index=True, height=520)
        with b:
            sto = pd.DataFrame(list(sm.items()), columns=["layer", "MB"])
            show(px.bar(sto, x="layer", y="MB", text_auto=True, title="Storage by layer (columnar Parquet vs raw text)",
                        color="layer", color_discrete_sequence=COLORS), 340)
            st.dataframe(pd.DataFrame(log["raw_files"]), width="stretch", hide_index=True)
    st.subheader("Data quality report (Bronze → Silver)")
    st.dataframe(q("SELECT * FROM gold.data_quality"), width="stretch", hide_index=True)
    st.subheader("Partitioned data lake (Silver / orders)")
    parts = sorted({p.parent.relative_to(SILVER).as_posix() for p in (SILVER / "orders").rglob("*.parquet")})
    st.code("\n".join(parts[:6] + ["..."] + parts[-3:]), language="text")
    st.caption(f"{len(parts)} partitions. A query filtered on year/month reads only the matching folders "
               "(partition pruning).")
    t0 = time.time()
    one = q(f"SELECT count(*) AS n FROM read_parquet('{(SILVER / 'orders/**/*.parquet').as_posix()}', "
            f"hive_partitioning = true) WHERE year = 2024 AND month = 11").n[0]
    st.write(f"Example: orders in Nov 2024 = **{one:,}** (scanned 1 of {len(parts)} partitions, "
             f"{(time.time() - t0) * 1000:.0f} ms)")

elif page == "🧮 MapReduce Demo":
    st.title("MapReduce — revenue per category")
    st.markdown("""
The classic Hadoop programming model, implemented from scratch with Python processes, run directly on the
**raw 125 MB `order_items.csv` text file**:

1. **Input splits** — the file is cut into byte ranges, like HDFS blocks; a split that starts mid-line skips to the next line
2. **Map** — each worker parses its lines, drops bad records and emits `(category, line_amount)` pairs
3. **Combine** — each worker pre-sums its own pairs, so it ships one number per key instead of millions
4. **Shuffle** — the driver groups partial sums by key
5. **Reduce** — one final sum per category
""")
    if "mapreduce" in log:
        mr = log["mapreduce"]
        cores = [k for k in mr if k.isdigit() and k != "1"][0]
        c = st.columns(3)
        c[0].metric("1 worker", f"{mr['1']} s")
        c[1].metric(f"{cores} workers", f"{mr[cores]} s")
        c[2].metric("Speed-up", f"{mr['1'] / max(mr[cores], 0.001):.1f}×")
        st.caption(f"From the last pipeline run: {mr['rows']:,} records in {mr['splits']} splits of a "
                   f"{mr.get('input_mb', '')} MB file.")
    workers = st.slider("Number of workers (processes)", 1, os.cpu_count() or 4, min(8, os.cpu_count() or 4))
    splits = st.slider("Number of input splits", 4, 64, 32)
    if st.button("▶ Run MapReduce job", type="primary"):
        with st.spinner("Mapping, shuffling, reducing ..."):
            out = subprocess.run([sys.executable, "-m", "src.mapreduce", "--workers", str(workers),
                                  "--splits", str(splits)], cwd=ROOT, capture_output=True, text=True)
        if out.returncode != 0:
            st.error(out.stderr[-2000:])
        else:
            r = json.loads(out.stdout)
            s = r["seconds"]
            c = st.columns(6)
            c[0].metric("Records read", f"{r['records']:,}")
            c[1].metric("Bad records skipped", f"{r['bad_records']:,}")
            c[2].metric("Input splits", r["input_splits"])
            c[3].metric("Worker processes", r["worker_processes"])
            c[4].metric("Map phase", f"{s['map']} s")
            c[5].metric("Total", f"{s['total']} s")
            a, b = st.columns(2)
            with a:
                st.subheader("1 · Map outputs (one per split)")
                st.dataframe(pd.DataFrame(r["map_outputs"]), width="stretch", hide_index=True, height=300)
            with b:
                st.subheader("2 · Shuffle (values per key)")
                st.dataframe(pd.DataFrame(list(r["shuffle"].items()), columns=["key (category)", "values received"]),
                             width="stretch", hide_index=True)
            st.subheader("3 · Reduce output")
            res = pd.DataFrame(list(r["result"].items()), columns=["category", "mapreduce_total"])
            sql = q("""SELECT p.category, round(sum(round(q * pr * (1 - d / 100.0), 2)), 2) AS sql_total
                       FROM (SELECT product_id, TRY_CAST(quantity AS INT) AS q, TRY_CAST(discount_pct AS INT) AS d,
                                    TRY_CAST(regexp_replace(unit_price, '[^0-9.]', '', 'g') AS DOUBLE) AS pr
                             FROM bronze.order_items) i
                       JOIN silver.products p USING (product_id)
                       WHERE q > 0 AND pr > 0 GROUP BY 1""")
            chk = res.merge(sql, on="category")
            chk["match"] = (chk.mapreduce_total - chk.sql_total).abs() < 1
            st.caption("Check: the same calculation in one DuckDB SQL query over the same raw records.")
            show(px.bar(chk, x="category", y="mapreduce_total", title="Gross line revenue per category (MapReduce)",
                        color_discrete_sequence=COLORS), 340)
            st.dataframe(chk, width="stretch", hide_index=True)
            if chk.match.all():
                st.success("✅ MapReduce result matches the DuckDB SQL result for every category.")

elif page == "💻 SQL Console":
    st.title("SQL Console")
    st.caption("Query the warehouse directly with DuckDB SQL (read-only).")
    tables = q("""SELECT table_schema AS schema, table_name AS name, table_type AS type
                  FROM information_schema.tables ORDER BY 1, 2""")
    with st.expander(f"Available tables and views ({len(tables)})"):
        st.dataframe(tables, width="stretch", hide_index=True)
    examples = {
        "Top 10 cities by revenue": "SELECT city, revenue, aov, cancel_rate FROM gold.city_summary ORDER BY revenue DESC LIMIT 10",
        "Festive season vs rest (2024)": """SELECT CASE WHEN order_date BETWEEN '2024-10-10' AND '2024-11-15' THEN 'Festive' ELSE 'Normal' END AS period,
       count(*) / count(DISTINCT order_date) AS orders_per_day
FROM gold.fact_orders WHERE year = 2024 GROUP BY 1""",
        "Year-over-year growth by category": """SELECT category,
       sum(line_amount) FILTER (WHERE year = 2023) AS rev_2023,
       sum(line_amount) FILTER (WHERE year = 2024) AS rev_2024,
       round(100 * (rev_2024 / rev_2023 - 1), 1) AS growth_pct
FROM gold.fact_sales WHERE is_delivered GROUP BY 1 ORDER BY growth_pct DESC""",
        "Window function: running revenue": """SELECT order_date, revenue,
       sum(revenue) OVER (ORDER BY order_date) AS running_total
FROM gold.daily_sales ORDER BY order_date LIMIT 50""",
        "Read the raw lake directly": "SELECT * FROM bronze.orders LIMIT 20",
    }
    ex = st.selectbox("Example queries", list(examples))
    sql = st.text_area("SQL", examples[ex], height=160)
    if st.button("Run query", type="primary"):
        if not sql.strip().lower().startswith(("select", "with")):
            st.error("Only SELECT / WITH queries are allowed.")
        else:
            t0 = time.time()
            try:
                df = connection().cursor().execute(sql).df()
                st.success(f"{len(df):,} rows in {(time.time() - t0) * 1000:.0f} ms")
                st.dataframe(df, width="stretch", hide_index=True)
            except Exception as e:
                st.error(str(e))
