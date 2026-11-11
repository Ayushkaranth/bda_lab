"""
Step 0 - Source systems.

Generates a large, realistic and *deliberately messy* Indian e-commerce dataset:
  customers.csv, products.csv, orders.csv, order_items.csv  (structured, CSV)
  events.json                                                (semi-structured clickstream, nested JSON)

Planted patterns (the analytics must rediscover them):
  * festive-season and sale-day spikes, weekend uplift, evening peak hours, growth over time
  * products that are bought together (phone -> phone case, shampoo -> conditioner, ...)
  * a few heavy buyers and many occasional ones (Pareto)

Planted data-quality problems (the Silver layer must fix them):
  * duplicate rows, missing customer ids, two different timestamp formats
  * inconsistent city spellings (Bangalore / BLR / Bengaluru)
  * zero / negative quantities, prices written as "Rs 1,299" text, missing product ids

Usage:  python -m src.generate_data [--orders 2000000]
"""
import argparse
import time

import duckdb
import numpy as np
import pandas as pd

from config import RAW
from src.catalog import BRANDS, CATALOG, CITIES, CITY_VARIANTS, COMPLEMENTS

rng = np.random.default_rng(42)
START, END = "2023-01-01", "2024-12-31"
HOUR_WEIGHTS = np.array([1, .6, .4, .3, .3, .5, 1, 2, 3, 4, 4.5, 5, 6, 6.5, 5.5, 5, 5, 5.5, 6.5, 7.5, 8.5, 9, 7, 3])


def write(df: pd.DataFrame, name: str, fmt: str = "csv", select: str = "*") -> None:
    path = (RAW / name).as_posix()
    con = duckdb.connect()
    con.register("df", df)
    opts = "(HEADER, DELIMITER ',')" if fmt == "csv" else "(FORMAT JSON)"
    con.execute(f"COPY (SELECT {select} FROM df) TO '{path}' {opts}")
    con.close()
    print(f"  wrote {name:<16} {len(df):>12,} rows  {(RAW / name).stat().st_size / 1e6:8.1f} MB")


def calendar():
    days = pd.date_range(START, END, freq="D")
    t = np.arange(len(days)) / len(days)
    w = 1 + 0.4 * t                                        # business grows over two years
    w *= np.where(days.day_of_week >= 5, 1.15, 1.0)          # weekend uplift
    festive = np.zeros(len(days), bool)
    for y in (2023, 2024):
        for start, end, factor in [(f"{y}-10-10", f"{y}-11-15", 1.7),   # Dussehra-Diwali festive season
                                   (f"{y}-01-20", f"{y}-01-26", 1.4),   # Republic Day sale
                                   (f"{y}-08-08", f"{y}-08-15", 1.4),   # Independence Day sale
                                   (f"{y}-12-20", f"{y}-12-31", 1.25)]: # year-end sale
            m = (days >= start) & (days <= end)
            w[m] *= factor
            festive |= m
    return days, w / w.sum(), festive


def make_products(n):
    subs = [(c, s, lo, hi) for c, d in CATALOG.items() for s, (lo, hi) in d.items()]
    pick = rng.integers(0, len(subs), n)
    cat = np.array([s[0] for s in subs])[pick]
    sub = np.array([s[1] for s in subs])[pick]
    lo = np.array([s[2] for s in subs])[pick]
    hi = np.array([s[3] for s in subs])[pick]
    price = (np.floor(np.exp(rng.uniform(np.log(lo), np.log(hi))) / 10) * 10 + 9).astype(int)
    brand = np.array([BRANDS[c][rng.integers(0, len(BRANDS[c]))] for c in cat])
    model = rng.integers(100, 999, n)
    df = pd.DataFrame({
        "product_id": [f"P{i:05d}" for i in range(1, n + 1)],
        "product_name": [f"{b} {s} {m}" for b, s, m in zip(brand, sub, model)],
        "category": cat, "subcategory": sub, "brand": brand, "list_price": price,
        "rating": np.clip(rng.normal(4.0, 0.45, n), 1, 5).round(1),
    })
    popularity = rng.pareto(1.5, n) + 0.1
    return df, popularity / popularity.sum()


def make_customers(n):
    names = np.array([c[0] for c in CITIES])
    w = np.array([c[3] for c in CITIES], float)
    city = rng.choice(names, n, p=w / w.sum()).astype(object)
    clean_city = city.copy()
    for i in np.where(rng.random(n) < 0.05)[0]:              # 5% messy spellings
        if city[i] in CITY_VARIANTS:
            v = CITY_VARIANTS[city[i]]
            city[i] = v[rng.integers(0, len(v))]
    age = np.clip(rng.normal(31, 9, n), 18, 70).astype(int).astype(object)
    age[rng.random(n) < 0.02] = None                         # 2% missing age
    email = np.array([f"user{i}@mail.com" for i in range(1, n + 1)], dtype=object)
    up = rng.random(n) < 0.05
    email[up] = [e.upper() for e in email[up]]
    signup = pd.Timestamp("2020-01-01") + pd.to_timedelta(rng.integers(0, 1095, n), unit="D")
    df = pd.DataFrame({
        "customer_id": [f"C{i:07d}" for i in range(1, n + 1)],
        "gender": rng.choice(["M", "F", "Other"], n, p=[.52, .47, .01]),
        "age": age, "email": email, "city": city, "signup_date": signup.strftime("%Y-%m-%d"),
    })
    dup = df.sample(frac=0.003, random_state=1)              # 0.3% duplicate rows
    return pd.concat([df, dup], ignore_index=True), df["customer_id"].to_numpy(), city[:n], clean_city


def make_orders(n, cust_ids, cust_city, days, dw):
    cw = rng.lognormal(0, 0.8, len(cust_ids))               # some heavy buyers, many occasional ones
    ci = rng.choice(len(cust_ids), n, p=cw / cw.sum())
    di = rng.choice(len(days), n, p=dw)
    secs = rng.choice(24, n, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum()) * 3600 + rng.integers(0, 3600, n)
    ts = pd.Series(days.values[di] + secs.astype("timedelta64[s]"))
    ts_str = ts.dt.strftime("%Y-%m-%d %H:%M:%S").to_numpy(dtype=object)
    alt = rng.random(n) < 0.01                               # 1% in a different date format
    ts_str[alt] = ts[alt].dt.strftime("%d/%m/%Y %H:%M").to_numpy(dtype=object)
    pay = rng.choice(["UPI", "Credit/Debit Card", "Cash on Delivery", "Net Banking", "Wallet"], n,
                     p=[.45, .20, .18, .07, .10])
    r = rng.random(n)
    cancel_p = np.where(pay == "Cash on Delivery", 0.13, 0.06)
    status = np.where(r < cancel_p, "Cancelled", np.where(r < cancel_p + 0.07, "Returned", "Delivered"))
    cid = cust_ids[ci].astype(object)
    cid[rng.random(n) < 0.002] = None                        # 0.2% missing customer id
    df = pd.DataFrame({
        "order_id": np.char.add("O", np.char.zfill(np.arange(1, n + 1).astype(str), 8)),
        "customer_id": cid, "order_ts": ts_str, "city": cust_city[ci],
        "payment_method": pay, "status": status,
        "channel": rng.choice(["Mobile App", "Website"], n, p=[.68, .32]),
    })
    dup = df.sample(frac=0.005, random_state=2)              # 0.5% duplicate rows
    return pd.concat([df, dup], ignore_index=True), di


def make_items(order_ids, order_day, festive, products, pop):
    n_orders = len(order_ids)
    k = 1 + np.minimum(rng.poisson(0.55, n_orders), 4)
    o = np.repeat(np.arange(n_orders), k)
    p = rng.choice(len(products), len(o), p=pop)
    sub = products["subcategory"].to_numpy()
    by_sub = {s: np.where(sub == s)[0] for s in np.unique(sub)}
    psub = sub[p]
    add_o, add_p = [o], [p]
    for s, comps in COMPLEMENTS.items():                     # plant "bought together" patterns
        idx = np.where(psub == s)[0]
        take = idx[rng.random(len(idx)) < 0.45]
        choice = rng.integers(0, len(comps), len(take))
        for j, c in enumerate(comps):
            t = take[choice == j]
            add_o.append(o[t])
            add_p.append(rng.choice(by_sub[c], len(t)))
    o, p = np.concatenate(add_o), np.concatenate(add_p)
    order = np.argsort(o, kind="stable")
    o, p = o[order], p[order]
    n = len(o)
    qty = rng.choice([1, 2, 3], n, p=[.82, .14, .04]).astype(object)
    price = np.round(products["list_price"].to_numpy()[p] * rng.uniform(0.97, 1.03, n)).astype(int)
    disc = rng.choice([0, 5, 10, 15, 20, 30, 40], n, p=[.35, .2, .15, .1, .1, .06, .04])
    disc = np.where(festive[order_day[o]], np.minimum(disc + 10, 60), disc)   # bigger festive discounts
    price_txt = price.astype(str).astype(object)
    rs = rng.random(n) < 0.004                               # 0.4% prices stored as text "Rs 1,299"
    price_txt[rs] = [f"Rs {v:,}" for v in price[rs]]
    bad = rng.random(n) < 0.001                              # 0.1% zero / negative quantities
    qty[bad] = rng.choice([0, -1], bad.sum())
    pid = products["product_id"].to_numpy()[p].astype(object)
    pid[rng.random(n) < 0.0005] = None                       # 0.05% missing product id
    return pd.DataFrame({
        "line_id": np.arange(1, n + 1), "order_id": order_ids[o], "product_id": pid,
        "quantity": qty, "unit_price": price_txt, "discount_pct": disc,
    })


def make_events(n_sessions, cust_ids, products, pop, days, dw):
    S = n_sessions
    dev = rng.choice(["mobile", "desktop", "tablet"], S, p=[.62, .30, .08])
    os_ = np.where(dev == "mobile", rng.choice(["Android", "iOS"], S, p=[.78, .22]),
                   np.where(dev == "desktop", rng.choice(["Windows", "macOS", "Linux"], S, p=[.8, .15, .05]),
                            rng.choice(["Android", "iPadOS"], S, p=[.6, .4])))
    cust = cust_ids[rng.integers(0, len(cust_ids), S)].astype(object)
    cust[rng.random(S) < 0.2] = None                         # 20% anonymous visitors
    names = np.array([c[0] for c in CITIES])
    w = np.array([c[3] for c in CITIES], float)
    city = rng.choice(names, S, p=w / w.sum())
    start = days.values[rng.choice(len(days), S, p=dw)] + rng.integers(0, 86400, S).astype("timedelta64[s]")
    n_view = 1 + rng.poisson(2.2, S)
    cart = rng.random(S) < 0.30
    conv = np.select([dev == "desktop", dev == "mobile"], [0.46, 0.38], 0.32)
    buy = cart & (rng.random(S) < conv)
    n_ev = n_view + cart + buy
    s = np.repeat(np.arange(S), n_ev)
    pos = np.arange(len(s)) - np.repeat(np.cumsum(n_ev) - n_ev, n_ev)
    nv = n_view[s]
    etype = np.where(pos < nv, "view", np.where(pos == nv, "add_to_cart", "purchase"))
    ts = start[s] + (pos * rng.integers(15, 180, len(s))).astype("timedelta64[s]")
    return pd.DataFrame({
        "event_id": np.arange(1, len(s) + 1),
        "session_id": np.char.add("S", np.char.zfill(s.astype(str), 7)),
        "customer_id": cust[s], "event_type": etype, "ts": ts,
        "product_id": products["product_id"].to_numpy()[rng.choice(len(products), len(s), p=pop)],
        "device": dev[s], "os": os_[s], "app_version": rng.choice(["5.1.0", "5.2.1", "5.3.0"], S)[s],
        "city": city[s],
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=2_000_000)
    ap.add_argument("--customers", type=int, default=300_000)
    ap.add_argument("--products", type=int, default=5_000)
    ap.add_argument("--sessions", type=int, default=600_000)
    a = ap.parse_args()
    RAW.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    print("Generating source data ...")
    days, dw, festive = calendar()
    products, pop = make_products(a.products)
    write(products, "products.csv")
    customers, cust_ids, cust_city, _ = make_customers(a.customers)
    write(customers, "customers.csv")
    orders, order_day = make_orders(a.orders, cust_ids, cust_city, days, dw)
    write(orders, "orders.csv")
    base_ids = orders["order_id"].to_numpy()[: a.orders]
    items = make_items(base_ids, order_day, festive, products, pop)
    write(items, "order_items.csv")
    del orders, items
    events = make_events(a.sessions, cust_ids, products, pop, days, dw)
    write(events, "events.json", fmt="json", select=(
        "event_id, session_id, customer_id, event_type, strftime(ts, '%Y-%m-%dT%H:%M:%S') AS ts, product_id, "
        "struct_pack(device := device, os := os, app_version := app_version) AS context, "
        "struct_pack(city := city) AS geo"))
    print(f"Done in {time.time() - t0:.1f}s -> {RAW}")


if __name__ == "__main__":
    main()
