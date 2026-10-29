"""Builds the charts in docs/images from the gold and ML tables.

Queries run on a Databricks SQL warehouse through the Statement Execution API,
authenticated with the CLI profile (`databricks auth token`).

    python tools/analysis/make_charts.py --schema taco_bueno --warehouse <id>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import pandas as pd  # noqa: E402
import requests  # noqa: E402

# Validated categorical palette (adjacent-pair CVD safe) and chart chrome
S1, S2, S3 = "#2a78d6", "#eb6834", "#1baf7a"
POS, NEG = "#2a78d6", "#e34948"            # diverging poles (gain / loss)
BAND = "#cde2fb"
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"


def style():
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "font.family": ["Segoe UI", "DejaVu Sans", "sans-serif"], "font.size": 10, "text.color": INK,
        "axes.labelcolor": INK2, "axes.titlesize": 11.5, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "axes.titlepad": 10, "axes.edgecolor": AXIS, "axes.linewidth": 0.8, "axes.spines.top": False,
        "axes.spines.right": False, "axes.grid": True, "axes.axisbelow": True, "grid.color": GRID,
        "grid.linewidth": 0.6, "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK2,
        "ytick.labelcolor": INK2, "xtick.major.size": 0, "ytick.major.size": 0, "legend.frameon": False,
        "legend.fontsize": 9, "lines.linewidth": 2, "lines.solid_capstyle": "round",
    })


class Sql:
    def __init__(self, host: str, warehouse: str, catalog: str, schema: str, profile: str):
        tok = json.loads(subprocess.run(["databricks", "auth", "token", "-p", profile], capture_output=True,
                                        text=True, check=True).stdout)["access_token"]
        self.h = {"Authorization": f"Bearer {tok}"}
        self.host, self.warehouse, self.catalog, self.schema = host, warehouse, catalog, schema

    def df(self, sql: str) -> pd.DataFrame:
        body = {"warehouse_id": self.warehouse, "statement": sql, "wait_timeout": "50s", "on_wait_timeout": "CONTINUE",
                "format": "JSON_ARRAY", "disposition": "INLINE", "catalog": self.catalog, "schema": self.schema}
        r = requests.post(f"{self.host}/api/2.0/sql/statements", headers=self.h, json=body, timeout=120).json()
        while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            time.sleep(3)
            r = requests.get(f"{self.host}/api/2.0/sql/statements/{r['statement_id']}", headers=self.h, timeout=120).json()
        if r["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(r["status"].get("error", {}).get("message", "")[:500])
        cols = r["manifest"]["schema"]["columns"]
        rows = r.get("result", {}).get("data_array", []) or []
        link = r.get("result", {}).get("next_chunk_internal_link")
        while link:
            nxt = requests.get(f"{self.host}{link}", headers=self.h, timeout=120).json()
            rows += nxt.get("data_array", []) or []
            link = nxt.get("next_chunk_internal_link")
        df = pd.DataFrame(rows, columns=[c["name"] for c in cols])
        for c in cols:
            if c["type_name"] in ("DOUBLE", "FLOAT", "DECIMAL", "LONG", "INT", "SHORT", "BYTE"):
                df[c["name"]] = pd.to_numeric(df[c["name"]])
            elif c["type_name"] in ("DATE", "TIMESTAMP"):
                df[c["name"]] = pd.to_datetime(df[c["name"]])
        return df


def money(x, _=None):
    return f"${x / 1e3:,.0f}K" if abs(x) >= 1e4 else f"${x:,.0f}"


def pct(x, _=None):
    return f"{x * 100:+.0f}%" if x else "0%"


def save(fig, out: Path, name: str):
    fig.tight_layout()
    fig.savefig(out / name, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  ", name)


def chart_kpi_yoy(sql: Sql, out: Path):
    k = sql.df("SELECT month, net_sales_per_store_day, tickets_per_store_day, avg_ticket FROM mv_executive_kpis ORDER BY month")
    k["year"], k["m"] = k.month.dt.year, k.month.dt.month
    fig, axs = plt.subplots(1, 3, figsize=(13, 3.8))
    specs = [("net_sales_per_store_day", "Net sales per store-day", lambda v, _: f"${v:,.0f}"),
             ("tickets_per_store_day", "Tickets per store-day", lambda v, _: f"{v:,.0f}"),
             ("avg_ticket", "Average ticket", lambda v, _: f"${v:,.2f}")]
    for ax, (col, title, fmt) in zip(axs, specs):
        for year, color in [(2025, S2), (2026, S1)]:
            d = k[k.year == year]
            ax.plot(d.m, d[col], color=color, marker="o", ms=4, label=str(year))
            ax.annotate(str(year), (d.m.iloc[-1], d[col].iloc[-1]), xytext=(5, 0), textcoords="offset points",
                        color=INK2, fontsize=8.5, va="center")
        ax.set_title(title)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt))
        ax.set_xticks(range(1, 13), list("JFMAMJJASOND"))
        ax.grid(axis="x", visible=False)
    axs[0].legend(loc="lower left", ncol=2)
    save(fig, out, "kpi_yoy.png")


def chart_weekday_daypart(sql: Sql, out: Path):
    w = sql.df("SELECT day_of_week, day_name, avg(net_sales) v FROM mv_daily_store_sales GROUP BY ALL")
    w["order"] = (w.day_of_week + 5) % 7
    w = w.sort_values("order")
    d = sql.df("""SELECT 'Breakfast' k, sum(net_sales_breakfast) / sum(net_sales) v FROM mv_daily_store_sales
                  UNION ALL SELECT 'Lunch', sum(net_sales_lunch) / sum(net_sales) FROM mv_daily_store_sales
                  UNION ALL SELECT 'Afternoon', sum(net_sales_afternoon) / sum(net_sales) FROM mv_daily_store_sales
                  UNION ALL SELECT 'Dinner', sum(net_sales_dinner) / sum(net_sales) FROM mv_daily_store_sales
                  UNION ALL SELECT 'Late night', sum(net_sales_late_night) / sum(net_sales) FROM mv_daily_store_sales""")
    fig, axs = plt.subplots(1, 2, figsize=(12, 3.6), gridspec_kw={"width_ratios": [1.3, 1]})
    axs[0].bar(w.day_name.str[:3], w.v, color=S1, width=0.62)
    axs[0].set_title("Net sales per store by weekday")
    axs[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    axs[0].grid(axis="x", visible=False)
    axs[1].bar(d.k, d.v, color=S1, width=0.62)
    axs[1].set_title("Share of net sales by daypart")
    axs[1].yaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
    axs[1].grid(axis="x", visible=False)
    for i, v in enumerate(d.v):
        axs[1].annotate(f"{v:.0%}", (i, v), xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8.5, color=INK2)
    save(fig, out, "weekday_daypart.png")


def chart_market_channels(sql: Sql, out: Path):
    m = sql.df("""SELECT market, count(DISTINCT store_id) stores, avg(net_sales) per_store_day,
                         sum(net_sales_drive_thru) / sum(net_sales) dt, sum(net_sales_in_store) / sum(net_sales) ins,
                         sum(net_sales_delivery) / sum(net_sales) dl
                  FROM mv_daily_store_sales WHERE pos_system = 'New' GROUP BY market ORDER BY per_store_day""")
    fig, ax = plt.subplots(figsize=(10, 3.9))
    left = pd.Series(0.0, index=m.index)
    for col, label, color in [("dt", "Drive-thru", S1), ("ins", "In-store", S2), ("dl", "Delivery & digital", S3)]:
        ax.barh(m.market, m[col], left=left, color=color, height=0.62, edgecolor=SURFACE, linewidth=2, label=label)
        left += m[col]
    for i, r in m.reset_index(drop=True).iterrows():
        ax.annotate(f"{r.stores:.0f} stores · ${r.per_store_day:,.0f}/day", (1.01, i), va="center", fontsize=8.5,
                    color=INK2, annotation_clip=False)
    ax.set_xlim(0, 1)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(1.0, decimals=0))
    ax.set_title("Channel mix of net sales by market (new POS period)")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.32), ncol=3)
    save(fig, out, "market_channel_mix.png")


def diverging_bars(ax, labels, values, fmt=pct):
    colors = [POS if v >= 0 else NEG for v in values]
    ax.barh(labels, values, color=colors, height=0.6)
    ax.axvline(0, color=AXIS, lw=1)
    for i, v in enumerate(values):
        ax.annotate(f"{v * 100:+.1f}%", (v, i), xytext=(4 if v >= 0 else -4, 0), textcoords="offset points",
                    ha="left" if v >= 0 else "right", va="center", fontsize=8.5, color=INK2)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(fmt))
    ax.grid(axis="y", visible=False)


def chart_holiday_lift(sql: Sql, out: Path):
    h = sql.df("""SELECT holiday_name, sum(net_sales) / sum(baseline_net_sales_4w) - 1 lift, count(DISTINCT business_date) dates
                  FROM mv_daily_store_sales WHERE holiday_name IS NOT NULL AND baseline_net_sales_4w IS NOT NULL
                  GROUP BY 1 ORDER BY lift""")
    fig, ax = plt.subplots(figsize=(10, 6))
    diverging_bars(ax, h.holiday_name, h.lift)
    ax.set_title("Net sales on holidays vs the same weekday of the 4 previous weeks")
    lo, hi = h.lift.min(), h.lift.max()
    ax.set_xlim(lo - 0.08, hi + 0.08)
    save(fig, out, "holiday_lift.png")


def chart_event_lift(sql: Sql, out: Path):
    e = sql.df("""SELECT exposure_type, store_days, incremental_lift_vs_no_game lift FROM mv_event_impact
                  WHERE market = 'All markets' AND day_type = 'All days' AND exposure_type <> 'No relevant game'
                  ORDER BY lift""")
    fig, ax = plt.subplots(figsize=(10, 4.4))
    diverging_bars(ax, [f"{t}  (n={n:,.0f})" for t, n in zip(e.exposure_type, e.store_days)], e.lift)
    ax.set_title("Sales lift on game days, net of the lift on no-game days")
    lo, hi = e.lift.min(), e.lift.max()
    ax.set_xlim(min(lo, 0) - 0.02, max(hi, 0) + 0.02)
    save(fig, out, "event_lift.png")


def chart_price_waves(sql: Sql, out: Path):
    p = sql.df("""SELECT month, count_if(price_change_pct > 0) increases,
                         percentile_approx(CASE WHEN price_change_pct > 0 THEN price_change_pct END, 0.5) median_inc
                  FROM mv_product_price_history WHERE prev_modal_price IS NOT NULL GROUP BY month ORDER BY month""")
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.bar(p.month, p.increases, width=20, color=S1)
    waves = p.nlargest(3, "increases").sort_values("month").reset_index(drop=True)
    for i, r in waves.iterrows():
        # adjacent wave months: push the earlier label left so the two do not overlap
        adjacent = i + 1 < len(waves) and (waves.month[i + 1] - r.month).days < 40
        ax.annotate(f"{r.increases:,.0f} increases\nmedian +{r.median_inc:.1f}%", (r.month, r.increases),
                    xytext=(-8 if adjacent else 0, 4), textcoords="offset points", ha="right" if adjacent else "center",
                    fontsize=8.5, color=INK2)
    ax.set_title("Store-product menu price increases per month")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    ax.set_ylim(0, p.increases.max() * 1.25)
    ax.grid(axis="x", visible=False)
    save(fig, out, "price_waves.png")


def chart_backtest(sql: Sql, out: Path):
    b = sql.df("""SELECT b.date, sum(b.actual * s.stores) actual, sum(b.forecast * s.stores) forecast,
                         sum(b.baseline * s.stores) baseline
                  FROM ml_forecast_backtest b
                  JOIN (SELECT business_date, market, count(*) stores FROM mv_daily_store_sales GROUP BY ALL) s
                    ON s.business_date = b.date AND s.market = b.series_key
                  WHERE b.series_type = 'tickets_per_store' GROUP BY b.date ORDER BY b.date""")
    wape = lambda a, f: (a - f).abs().sum() / a.sum()
    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(b.date, b.actual, color=INK, lw=1.6, label="Actual")
    ax.plot(b.date, b.forecast, color=S1, label=f"Model (WAPE {wape(b.actual, b.forecast):.1%})")
    ax.plot(b.date, b.baseline, color=S2, lw=1.4, label=f"Seasonal naive (WAPE {wape(b.actual, b.baseline):.1%})")
    ax.set_title("Backtest: chain daily tickets, Jul-Sep 2026 (trained on data up to Jun 30)")
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    ax.grid(axis="x", visible=False)
    save(fig, out, "backtest_tickets.png")


def chart_strong_dates(sql: Sql, out: Path):
    s = sql.df("""SELECT date, source, tickets, classification, drivers FROM ml_strong_dates
                  WHERE scope = 'Chain' AND date >= DATE'2026-06-01' ORDER BY date""")
    m = sql.df("""SELECT forecast_date AS date, sum(tickets_lower) lo, sum(tickets_upper) hi
                  FROM ml_market_forecast GROUP BY 1 ORDER BY 1""")
    act, fc = s[s.source == "actual"], s[s.source == "forecast"]
    fig, ax = plt.subplots(figsize=(12, 4.4))
    ax.fill_between(m.date, m.lo, m.hi, color=BAND, lw=0, label="Forecast band (P10-P90)")
    ax.plot(act.date, act.tickets, color=INK2, lw=1.2, label="Actual")
    ax.plot(fc.date, fc.tickets, color=S1, lw=1.8, label="Forecast")
    for cls, color, marker in [("Strong", S3, "^"), ("Weak", NEG, "v")]:
        pts = fc[fc.classification == cls]
        ax.scatter(pts.date, pts.tickets, color=color, marker=marker, s=46, zorder=3, edgecolor=SURFACE, lw=1.2,
                   label=f"{cls} date")
    ax.set_title("Chain daily tickets: actual and forecast with strong and weak dates")
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.14), ncol=5, fontsize=8.5)
    ax.grid(axis="x", visible=False)
    save(fig, out, "strong_dates_forecast.png")


def chart_segments(sql: Sql, out: Path):
    g = sql.df("SELECT segment_name, stores, avg_daily_net_sales, avg_ticket, delivery_share FROM ml_store_segment_profiles ORDER BY avg_daily_net_sales")
    fig, ax = plt.subplots(figsize=(10, 0.65 * len(g) + 1.3))
    ax.barh(g.segment_name, g.avg_daily_net_sales, color=S1, height=0.6)
    for i, r in g.reset_index(drop=True).iterrows():
        ax.annotate(f"{r.stores:.0f} stores · ticket ${r.avg_ticket:.2f} · delivery {r.delivery_share:.0%}",
                    (r.avg_daily_net_sales, i), xytext=(5, 0), textcoords="offset points", va="center", fontsize=8.5, color=INK2)
    ax.set_xlim(0, g.avg_daily_net_sales.max() * 1.55)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title("Store segments: average daily net sales")
    ax.grid(axis="y", visible=False)
    save(fig, out, "store_segments.png")


def chart_product_forecasts(sql: Sql, out: Path):
    top = sql.df("""SELECT product_key, any_value(product_name) name, any_value(product_role) role, sum(units_forecast) u
                    FROM ml_demand_forecast GROUP BY 1 ORDER BY u DESC""")
    picks = pd.concat([top[top.role == "Menu item"].head(3), top[top.role == "Bundle"].head(3)])
    keys = ", ".join(f"'{k}'" for k in picks.product_key)
    hist = sql.df(f"""SELECT business_date AS date, product_key, sum(priced_units) units FROM mv_daily_product_demand
                     WHERE product_key IN ({keys}) AND business_date >= DATE'2026-07-01' GROUP BY ALL ORDER BY 1""")
    fc = sql.df(f"""SELECT forecast_date AS date, product_key, units_forecast, units_lower, units_upper
                   FROM ml_demand_forecast WHERE product_key IN ({keys}) ORDER BY 1""")
    fig, axs = plt.subplots(2, 3, figsize=(13, 6.2), sharex=True)
    for ax, (_, p) in zip(axs.flat, picks.iterrows()):
        h, f = hist[hist.product_key == p.product_key], fc[fc.product_key == p.product_key]
        ax.fill_between(f.date, f.units_lower, f.units_upper, color=BAND, lw=0)
        ax.plot(h.date, h.units, color=INK2, lw=1)
        ax.plot(f.date, f.units_forecast, color=S1, lw=1.6)
        ax.set_title(f"{p['name']}  ({p.role.lower()})", fontsize=10)
        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.grid(axis="x", visible=False)
    fig.suptitle("Daily units sold at menu price: history (gray) and forecast with P10-P90 band (blue)", x=0.01, ha="left",
                 fontsize=11.5, fontweight="bold")
    save(fig, out, "product_forecasts.png")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="https://dbc-16b88d56-aa06.cloud.databricks.com")
    ap.add_argument("--warehouse", default="75e173ab9c89a73f")
    ap.add_argument("--catalog", default="workspace")
    ap.add_argument("--schema", default="taco_bueno")
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parents[2] / "docs" / "images"))
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    style()
    sql = Sql(args.host, args.warehouse, args.catalog, args.schema, args.profile)
    for fn in [chart_kpi_yoy, chart_weekday_daypart, chart_market_channels, chart_holiday_lift, chart_event_lift,
               chart_price_waves, chart_backtest, chart_strong_dates, chart_segments, chart_product_forecasts]:
        fn(sql, out)


if __name__ == "__main__":
    main()
