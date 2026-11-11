"""
The batch pipeline:  RAW files -> BRONZE -> SILVER -> GOLD -> ML -> association rules -> MapReduce

  BRONZE  raw CSV/JSON copied as-is into compressed Parquet + ingestion metadata (nothing cleaned)
  SILVER  cleaned, de-duplicated, typed, standardised; orders & order lines PARTITIONED by year/month
  GOLD    star-schema facts + business aggregates in the DuckDB warehouse (what the dashboard reads)
  ML      RFM features + K-Means customer segmentation (scikit-learn)
  RULES   market basket analysis: support / confidence / lift for every pair of sub-categories

Usage:  python -m src.pipeline
"""
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

from config import BRONZE, LAKE, LOG, RAW, ROOT, SILVER, WAREHOUSE
from src.catalog import CITIES, CITY_VARIANTS


STEPS = []


def P(path) -> str:
    return path.as_posix()


def step(layer, name, con, sql=None, count_sql=None, fn=None):
    t0 = time.time()
    if sql:
        con.execute(sql)
    if fn:
        fn()
    rows = con.execute(count_sql).fetchall()[0][0] if count_sql else None
    secs = round(time.time() - t0, 2)
    STEPS.append({"layer": layer, "step": name, "rows": rows, "seconds": secs})
    print(f"  [{layer:<6}] {name:<28} {'' if rows is None else f'{rows:>12,} rows'}  {secs:>6.2f}s")


def dir_mb(path) -> float:
    if path.is_file():
        return round(path.stat().st_size / 1e6, 1)
    return round(sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / 1e6, 1)


# ---------------------------------------------------------------- BRONZE
def bronze(con):
    for t in ["customers", "products", "orders", "order_items"]:
        step("Bronze", t, con, f"""
            COPY (SELECT *, now() AS _ingested_at, '{t}.csv' AS _source_file
                  FROM read_csv('{P(RAW / f"{t}.csv")}', all_varchar = true, header = true))
            TO '{P(BRONZE / f"{t}.parquet")}' (FORMAT parquet, COMPRESSION zstd)""",
             f"SELECT count(*) FROM '{P(BRONZE / f'{t}.parquet')}'")
    step("Bronze", "events (nested JSON)", con, f"""
        COPY (SELECT *, now() AS _ingested_at, 'events.json' AS _source_file
              FROM read_json('{P(RAW / "events.json")}', format = 'newline_delimited'))
        TO '{P(BRONZE / "events.parquet")}' (FORMAT parquet, COMPRESSION zstd)""",
         f"SELECT count(*) FROM '{P(BRONZE / 'events.parquet')}'")


# ---------------------------------------------------------------- SILVER
def silver(con):
    B = {t: f"read_parquet('{P(BRONZE / f'{t}.parquet')}')" for t in
         ["customers", "products", "orders", "order_items", "events"]}

    con.execute("CREATE TABLE silver.dim_city (city VARCHAR, state VARCHAR, region VARCHAR)")
    con.executemany("INSERT INTO silver.dim_city VALUES (?, ?, ?)", [c[:3] for c in CITIES])
    aliases = {c[0].lower(): c[0] for c in CITIES}
    aliases |= {v.strip().lower(): city for city, vs in CITY_VARIANTS.items() for v in vs}
    con.execute("CREATE TABLE silver.city_alias (alias VARCHAR, city VARCHAR)")
    con.executemany("INSERT INTO silver.city_alias VALUES (?, ?)", list(aliases.items()))
    city_fix = """LEFT JOIN silver.city_alias a ON lower(trim({x}.city)) = a.alias
                  LEFT JOIN silver.dim_city d ON d.city = a.city"""

    step("Silver", "customers", con, f"""
        COPY (SELECT DISTINCT ON (c.customer_id)
                     c.customer_id, c.gender, TRY_CAST(c.age AS INTEGER) AS age,
                     lower(trim(c.email)) AS email, a.city, d.state, d.region,
                     TRY_CAST(c.signup_date AS DATE) AS signup_date
              FROM {B['customers']} c {city_fix.format(x='c')}
              WHERE c.customer_id IS NOT NULL ORDER BY c.customer_id)
        TO '{P(SILVER / "customers.parquet")}' (FORMAT parquet, COMPRESSION zstd)""",
         f"SELECT count(*) FROM '{P(SILVER / 'customers.parquet')}'")

    step("Silver", "products", con, f"""
        COPY (SELECT DISTINCT ON (product_id) product_id, product_name, category, subcategory, brand,
                     TRY_CAST(list_price AS DOUBLE) AS list_price, TRY_CAST(rating AS DOUBLE) AS rating
              FROM {B['products']} WHERE product_id IS NOT NULL ORDER BY product_id)
        TO '{P(SILVER / "products.parquet")}' (FORMAT parquet, COMPRESSION zstd)""",
         f"SELECT count(*) FROM '{P(SILVER / 'products.parquet')}'")

    step("Silver", "orders (partitioned)", con, f"""
        COPY (SELECT *, year(order_ts) AS year, month(order_ts) AS month FROM (
                SELECT DISTINCT ON (o.order_id) o.order_id, o.customer_id,
                       COALESCE(TRY_STRPTIME(o.order_ts, '%Y-%m-%d %H:%M:%S'),
                                TRY_STRPTIME(o.order_ts, '%d/%m/%Y %H:%M')) AS order_ts,
                       a.city, d.state, d.region, o.payment_method, o.status, o.channel
                FROM {B['orders']} o {city_fix.format(x='o')}
                WHERE o.customer_id IS NOT NULL) WHERE order_ts IS NOT NULL)
        TO '{P(SILVER / "orders")}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (year, month))""",
         f"SELECT count(*) FROM read_parquet('{P(SILVER / 'orders/**/*.parquet')}')")

    step("Silver", "order_items (partitioned)", con, f"""
        COPY (SELECT *, round(quantity * unit_price * (1 - discount_pct / 100.0), 2) AS line_amount FROM (
                SELECT TRY_CAST(i.line_id AS BIGINT) AS line_id, i.order_id, i.product_id,
                       TRY_CAST(i.quantity AS INTEGER) AS quantity,
                       TRY_CAST(regexp_replace(i.unit_price, '[^0-9.]', '', 'g') AS DOUBLE) AS unit_price,
                       TRY_CAST(i.discount_pct AS INTEGER) AS discount_pct, o.year, o.month
                FROM {B['order_items']} i
                JOIN read_parquet('{P(SILVER / 'orders/**/*.parquet')}', hive_partitioning = true) o
                  ON o.order_id = i.order_id
                WHERE i.product_id IS NOT NULL)
              WHERE quantity > 0 AND unit_price > 0)
        TO '{P(SILVER / "order_items")}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (year, month))""",
         f"SELECT count(*) FROM read_parquet('{P(SILVER / 'order_items/**/*.parquet')}')")

    step("Silver", "events (flattened)", con, f"""
        COPY (SELECT e.event_id, e.session_id, e.customer_id, e.event_type,
                     CAST(e.ts AS TIMESTAMP) AS event_ts, e.product_id,
                     e.context.device AS device, e.context.os AS os, e.context.app_version AS app_version,
                     a.city
              FROM {B['events']} e LEFT JOIN silver.city_alias a ON lower(trim(e.geo.city)) = a.alias)
        TO '{P(SILVER / "events.parquet")}' (FORMAT parquet, COMPRESSION zstd)""",
         f"SELECT count(*) FROM '{P(SILVER / 'events.parquet')}'")

    # warehouse views over the lake, so SQL users see one catalogue
    for name, src in {"customers": "customers.parquet", "products": "products.parquet",
                      "orders": "orders/**/*.parquet", "order_items": "order_items/**/*.parquet",
                      "events": "events.parquet"}.items():
        con.execute(f"CREATE VIEW silver.{name} AS SELECT * FROM read_parquet('{P(SILVER / src)}', "
                    f"hive_partitioning = {'true' if '**' in src else 'false'})")
    for t in ["customers", "products", "orders", "order_items", "events"]:
        con.execute(f"CREATE VIEW bronze.{t} AS SELECT * FROM {B[t]}")

    # data quality report: what was wrong and what the Silver layer did about it
    canon = ", ".join(f"'{c[0]}'" for c in CITIES)
    checks = [
        ("orders", "Duplicate order rows", "Removed (DISTINCT ON order_id)",
         "SELECT count(*) - count(DISTINCT order_id) FROM bronze.orders"),
        ("orders", "Missing customer_id", "Row dropped",
         "SELECT count(*) FROM bronze.orders WHERE customer_id IS NULL"),
        ("orders", "Timestamp in dd/mm/yyyy format", "Parsed with second format",
         "SELECT count(*) FROM bronze.orders WHERE TRY_STRPTIME(order_ts, '%Y-%m-%d %H:%M:%S') IS NULL "
         "AND TRY_STRPTIME(order_ts, '%d/%m/%Y %H:%M') IS NOT NULL"),
        ("orders", "Non-standard city spelling", "Mapped to standard name",
         f"SELECT count(*) FROM bronze.orders WHERE city NOT IN ({canon})"),
        ("customers", "Duplicate customer rows", "Removed (DISTINCT ON customer_id)",
         "SELECT count(*) - count(DISTINCT customer_id) FROM bronze.customers"),
        ("customers", "Non-standard city spelling", "Mapped to standard name",
         f"SELECT count(*) FROM bronze.customers WHERE city NOT IN ({canon})"),
        ("customers", "Upper-case email", "Lower-cased",
         "SELECT count(*) FROM bronze.customers WHERE email <> lower(email)"),
        ("customers", "Missing age", "Kept as NULL",
         "SELECT count(*) FROM bronze.customers WHERE age IS NULL"),
        ("order_items", "Price stored as text (Rs 1,299)", "Parsed to number",
         "SELECT count(*) FROM bronze.order_items WHERE TRY_CAST(unit_price AS DOUBLE) IS NULL"),
        ("order_items", "Zero or negative quantity", "Row dropped",
         "SELECT count(*) FROM bronze.order_items WHERE TRY_CAST(quantity AS INTEGER) <= 0"),
        ("order_items", "Missing product_id", "Row dropped",
         "SELECT count(*) FROM bronze.order_items WHERE product_id IS NULL"),
    ]
    rows = [(t, issue, action, con.execute(q).fetchall()[0][0]) for t, issue, action, q in checks]
    con.execute("CREATE TABLE gold.data_quality (table_name VARCHAR, issue VARCHAR, action VARCHAR, rows_affected BIGINT)")
    con.executemany("INSERT INTO gold.data_quality VALUES (?, ?, ?, ?)", rows)
    STEPS.append({"layer": "Silver", "step": "data quality report", "rows": len(rows), "seconds": 0})


# ---------------------------------------------------------------- GOLD
def gold(con):
    step("Gold", "fact_sales (line level)", con, """
        CREATE TABLE gold.fact_sales AS
        SELECT i.line_id, i.order_id, o.order_ts, CAST(o.order_ts AS DATE) AS order_date, o.year, o.month,
               hour(o.order_ts) AS hour, isodow(o.order_ts) AS weekday,
               o.customer_id, o.city, o.state, o.region, o.channel, o.payment_method, o.status,
               (o.status = 'Delivered') AS is_delivered,
               i.product_id, p.category, p.subcategory, p.brand,
               i.quantity, i.unit_price, i.discount_pct, i.line_amount
        FROM silver.order_items i
        JOIN silver.orders o ON o.order_id = i.order_id
        JOIN silver.products p ON p.product_id = i.product_id""",
         "SELECT count(*) FROM gold.fact_sales")

    step("Gold", "fact_orders (order level)", con, """
        CREATE TABLE gold.fact_orders AS
        SELECT order_id, any_value(order_ts) AS order_ts, any_value(order_date) AS order_date,
               any_value(year) AS year, any_value(month) AS month, any_value(hour) AS hour,
               any_value(weekday) AS weekday, any_value(customer_id) AS customer_id,
               any_value(city) AS city, any_value(region) AS region, any_value(channel) AS channel,
               any_value(payment_method) AS payment_method, any_value(status) AS status,
               count(*) AS lines, sum(quantity) AS units, round(sum(line_amount), 2) AS order_value
        FROM gold.fact_sales GROUP BY order_id""",
         "SELECT count(*) FROM gold.fact_orders")

    step("Gold", "daily_sales", con, """
        CREATE TABLE gold.daily_sales AS
        SELECT order_date, count(*) AS orders,
               round(sum(order_value) FILTER (WHERE status = 'Delivered'), 2) AS revenue,
               sum(units) AS units
        FROM gold.fact_orders GROUP BY order_date ORDER BY order_date""",
         "SELECT count(*) FROM gold.daily_sales")

    step("Gold", "city_summary", con, """
        CREATE TABLE gold.city_summary AS
        SELECT city, region, count(*) AS orders, count(DISTINCT customer_id) AS customers,
               round(sum(order_value) FILTER (WHERE status = 'Delivered'), 2) AS revenue,
               round(avg(order_value) FILTER (WHERE status = 'Delivered'), 2) AS aov,
               round(avg((status = 'Cancelled')::INT), 4) AS cancel_rate,
               round(avg((status = 'Returned')::INT), 4) AS return_rate
        FROM gold.fact_orders GROUP BY city, region ORDER BY revenue DESC""",
         "SELECT count(*) FROM gold.city_summary")

    step("Gold", "product_performance", con, """
        CREATE TABLE gold.product_performance AS
        SELECT f.product_id, p.product_name, f.category, f.subcategory, f.brand, p.rating, p.list_price,
               count(DISTINCT f.order_id) AS orders, sum(f.quantity) AS units,
               round(sum(f.line_amount) FILTER (WHERE f.is_delivered), 2) AS revenue,
               round(avg((f.status = 'Returned')::INT), 4) AS return_rate
        FROM gold.fact_sales f JOIN silver.products p USING (product_id)
        GROUP BY ALL ORDER BY revenue DESC""",
         "SELECT count(*) FROM gold.product_performance")

    step("Gold", "funnel (clickstream)", con, """
        CREATE TABLE gold.funnel AS
        SELECT COALESCE(device, 'All devices') AS device,
               count(DISTINCT session_id) AS sessions,
               count(DISTINCT session_id) FILTER (WHERE event_type = 'add_to_cart') AS added_to_cart,
               count(DISTINCT session_id) FILTER (WHERE event_type = 'purchase') AS purchased
        FROM silver.events GROUP BY GROUPING SETS ((device), ()) ORDER BY sessions DESC""",
         "SELECT count(*) FROM gold.funnel")


# ---------------------------------------------------------------- ML: RFM + K-Means
def segmentation(con, summary):
    def run():
        rfm = con.execute("""
            SELECT customer_id,
                   date_diff('day', max(order_date), (SELECT max(order_date) FROM gold.fact_orders) + 1) AS recency_days,
                   count(*) AS frequency, round(sum(order_value), 2) AS monetary
            FROM gold.fact_orders WHERE status = 'Delivered' GROUP BY customer_id""").df()
        X = StandardScaler().fit_transform(np.column_stack([
            np.log1p(rfm.recency_days), np.log1p(rfm.frequency), np.log1p(rfm.monetary)]))
        km = KMeans(n_clusters=4, n_init=10, random_state=42).fit(X)
        # name clusters from their centroids (scaled log space: col 0 = recency, 1 = frequency, 2 = monetary)
        c = km.cluster_centers_
        best = int(np.argmax(-c[:, 0] + c[:, 1] + c[:, 2]))           # recent + frequent + high spend
        rest = [i for i in range(4) if i != best]
        lost = max(rest, key=lambda i: c[i, 0] - c[i, 2])              # long gone, low spend
        a, b = [i for i in rest if i != lost]
        recent, at_risk = (a, b) if c[a, 0] < c[b, 0] else (b, a)
        label = {best: "Champions", recent: "Promising (Recent)", at_risk: "At Risk (Big Spenders)",
                 lost: "Hibernating"}
        rfm["segment"] = [label[int(x)] for x in km.labels_]
        for col, asc in [("recency_days", False), ("frequency", True), ("monetary", True)]:
            r = rfm[col].rank(method="first", ascending=asc)
            rfm[col[0] + "_score"] = pd.qcut(r, 5, labels=[1, 2, 3, 4, 5]).astype(int)
        rfm["rfm_score"] = rfm.r_score * 100 + rfm.f_score * 10 + rfm.m_score

        rng = np.random.default_rng(0)
        sample = rng.choice(len(X), min(40_000, len(X)), replace=False)
        elbow = [(k, float(KMeans(n_clusters=k, n_init=5, random_state=42).fit(X[sample]).inertia_))
                 for k in range(2, 9)]
        sil_idx = sample[:10_000]
        summary["kmeans"] = {"k": 4, "customers": int(len(rfm)),
                             "silhouette": round(float(silhouette_score(X[sil_idx], km.labels_[sil_idx])), 3)}

        con.register("rfm_df", rfm)
        con.execute("""CREATE TABLE gold.customer_segments AS
                       SELECT r.*, c.age, c.gender, c.city, c.region
                       FROM rfm_df r LEFT JOIN silver.customers c USING (customer_id)""")
        con.register("elbow_df", pd.DataFrame(elbow, columns=["k", "inertia"]))
        con.execute("CREATE TABLE gold.kmeans_elbow AS SELECT * FROM elbow_df")
        con.execute("""CREATE TABLE gold.segment_summary AS
                       SELECT segment, count(*) AS customers, round(avg(recency_days), 1) AS avg_recency_days,
                              round(avg(frequency), 2) AS avg_orders, round(avg(monetary), 0) AS avg_spend,
                              round(sum(monetary), 0) AS total_spend
                       FROM gold.customer_segments GROUP BY segment ORDER BY avg_spend DESC""")
    step("ML", "RFM + K-Means segmentation", con, fn=run, count_sql="SELECT count(*) FROM gold.customer_segments")


# ---------------------------------------------------------------- Market basket analysis
def association_rules(con):
    step("Rules", "market basket (pairs)", con, """
        CREATE TEMP TABLE basket AS
            SELECT DISTINCT order_id, subcategory FROM gold.fact_sales WHERE status <> 'Cancelled';
        CREATE TABLE gold.association_rules AS
        WITH n AS (SELECT count(DISTINCT order_id) AS total FROM basket),
             item AS (SELECT subcategory, count(*) AS cnt FROM basket GROUP BY 1),
             pair AS (SELECT a.subcategory AS antecedent, b.subcategory AS consequent, count(*) AS together
                      FROM basket a JOIN basket b ON a.order_id = b.order_id AND a.subcategory <> b.subcategory
                      GROUP BY 1, 2)
        SELECT p.antecedent, p.consequent, p.together AS orders_together,
               round(p.together / n.total, 5) AS support,
               round(p.together / ia.cnt, 4) AS confidence,
               round((p.together / ia.cnt) / (ib.cnt / n.total), 2) AS lift
        FROM pair p CROSS JOIN n
        JOIN item ia ON ia.subcategory = p.antecedent
        JOIN item ib ON ib.subcategory = p.consequent
        WHERE p.together >= 100
        ORDER BY lift DESC;
        DROP TABLE basket;""",
         "SELECT count(*) FROM gold.association_rules")


def main():
    t0 = time.time()
    print("Running pipeline ...")
    shutil.rmtree(LAKE, ignore_errors=True)
    BRONZE.mkdir(parents=True)
    SILVER.mkdir(parents=True)
    if WAREHOUSE.exists():
        WAREHOUSE.unlink()
    con = duckdb.connect(str(WAREHOUSE))
    con.execute("CREATE SCHEMA bronze; CREATE SCHEMA silver; CREATE SCHEMA gold;")
    summary = {}
    bronze(con)
    silver(con)
    gold(con)
    segmentation(con, summary)
    association_rules(con)

    # MapReduce runs in its own process so worker processes start light (Windows 'spawn')
    n = min(8, os.cpu_count() or 1)
    runs = {}
    for w in (1, n):
        out = subprocess.run([sys.executable, "-m", "src.mapreduce", "--workers", str(w)],
                             cwd=ROOT, capture_output=True, text=True, check=True)
        runs[w] = json.loads(out.stdout)
        STEPS.append({"layer": "MapReduce", "step": f"category revenue, {w} worker(s)",
                      "rows": runs[w]["records"], "seconds": runs[w]["seconds"]["total"]})
        print(f"  [MapReduce] {w} worker(s): {runs[w]['seconds']['total']}s")
    summary["mapreduce"] = {"1": runs[1]["seconds"]["total"], str(n): runs[n]["seconds"]["total"],
                            "splits": runs[n]["input_splits"], "rows": runs[n]["records"],
                            "input_mb": runs[n]["input_mb"]}
    con.register("mr_df", pd.DataFrame(list(runs[n]["result"].items()), columns=["category", "revenue"]))
    con.execute("CREATE TABLE gold.mapreduce_category_revenue AS SELECT * FROM mr_df")
    con.close()

    raw_files = [{"file": f.name, "mb": dir_mb(f)} for f in sorted(RAW.iterdir())]
    log = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "total_seconds": round(time.time() - t0, 1),
        "cpu_cores": os.cpu_count(),
        "steps": STEPS,
        "storage_mb": {"Raw (CSV + JSON)": round(sum(r["mb"] for r in raw_files), 1),
                       "Bronze (Parquet)": dir_mb(BRONZE), "Silver (Parquet)": dir_mb(SILVER),
                       "Gold (DuckDB)": dir_mb(WAREHOUSE)},
        "raw_files": raw_files,
        **summary,
    }
    LOG.write_text(json.dumps(log, indent=2))
    print(f"Done in {log['total_seconds']}s. Storage (MB): {log['storage_mb']}")


if __name__ == "__main__":
    main()
