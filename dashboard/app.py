"""
Streamlit dashboard for the transaction monitoring pipeline.
Open http://localhost:8501 - the data refreshes itself every few seconds.

Pages (in the sidebar), one question each:
  Overview        - is the pipeline healthy and how much is flowing?
  Transactions    - volumes, amounts, status and currency mix
  Pipeline        - throughput, Kafka lag, latency, schema versions, restarts
  Data quality    - duplicates, invalid records, late events
  Risk indicators - SIMULATED rule-based indicators (not fraud detection)
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import queries as q
import theme as t

REFRESH_SECONDS = 5
RANGES = {"15 min": 15, "1 hour": 60, "6 hours": 360, "24 hours": 1440}
PAGES = ["Overview", "Transactions", "Pipeline", "Data quality", "Risk indicators"]

st.set_page_config(page_title="Transaction Monitoring", layout="wide", initial_sidebar_state="expanded")
t.inject()


def safe(fn, *args):
    """Run a query; return None if a table doesn't exist yet (e.g. before the first dbt run)."""
    try:
        conn = q.connect()
        try:
            return fn(conn, *args)
        finally:
            conn.close()
    except Exception:
        return None


def has(df):
    """True if the query returned a non-empty table."""
    return df is not None and not df.empty


def empty(msg="Waiting for data..."):
    """Show a small grey message instead of a chart."""
    st.markdown(f'<div class="card-sub">{msg}</div>', unsafe_allow_html=True)


def dbt_wait():
    """Message shown before dbt has built its tables."""
    empty("Waiting for dbt (it refreshes every 30 s)...")


def pipeline_state(k):
    """Overall health: stalled if no batch for 60 s, catching up if Kafka lag is large."""
    age = k.get("last_batch_age_s")
    lag = int(k.get("kafka_lag") or 0)
    if age is None or float(age) > 60:
        return "bad", "Stalled: no batch for 60 s"
    if lag > 5000:
        return "warn", f"Catching up: {lag:,} messages behind"
    return "ok", "Healthy: keeping up with Kafka"


def unit(value_html, unit_text):
    """Add a small unit (like EUR or %) after a big number."""
    return f"{value_html}<small>{unit_text}</small>"


def status_bars(vol, key, height=300):
    """Stacked bars per minute: approved / declined / pending (thin paper gap between segments)."""
    pending = vol["txn_count"] - vol["approved_count"] - vol["declined_count"]
    fig = go.Figure()
    for name, y, color in [("Approved", vol["approved_count"], t.GREEN),
                           ("Declined", vol["declined_count"], t.TERRA),
                           ("Pending", pending, t.VIOLET)]:
        fig.add_trace(go.Bar(x=vol["window_start"], y=y, name=name, marker_color=color,
                             marker_line=dict(color=t.PAPER, width=1)))
    fig.update_layout(barmode="stack")
    t.show(t.style(fig, height), key)


# ================================================================ demo mode (online version)
snapshot = safe(q.snapshot_info) if q.DEMO_MODE else None
snapshot_text = pd.Timestamp(snapshot["snapshot_at"]).strftime("%B %d, %Y, %H:%M UTC") if snapshot else None

# ================================================================ sidebar
with st.sidebar:
    t.sidebar_brand(f"Snapshot of {snapshot_text}" if snapshot_text else None)
    t.side_label("Navigate")
    page = st.radio("Page", PAGES, key="page", label_visibility="collapsed")
    t.side_label("Time range")
    range_label = st.radio("Time range", list(RANGES), index=1, horizontal=True, key="range",
                           label_visibility="collapsed")

    @st.fragment(run_every=REFRESH_SECONDS)
    def side_health():
        """Health badge in the sidebar (refreshed every 5 s)."""
        k = safe(q.kpis)
        t.side_label("Pipeline")
        state, text = pipeline_state(k) if k else ("bad", "No data yet")
        st.markdown(t.pill(state, text), unsafe_allow_html=True)

    side_health()
    st.markdown('<div class="side-foot">Synthetic data · fixed demo FX rates · all amounts in EUR.<br>'
                'Risk indicators are simulated rules, not fraud detection.</div>', unsafe_allow_html=True)

minutes = RANGES[range_label]

if q.DEMO_MODE:
    st.markdown(f'<div class="notice">Online demo: a snapshot of a real pipeline run'
                f'{" recorded on " + t.esc(snapshot_text) if snapshot_text else ""}. '
                f'Times are counted back from that moment. Run it live with Docker (see the README).</div>',
                unsafe_allow_html=True)


# ================================================================ pages
@st.fragment(run_every=REFRESH_SECONDS)
def overview_live():
    """The live part of the Overview page (refreshed every 5 s)."""
    k = safe(q.kpis) or {}
    r = safe(q.kpis_in_range, minutes) or {}
    n = int(r.get("txn_count") or 0)
    approval = 100 * int(r.get("approved") or 0) / n if n else 0

    t.section("i.", "Activity", f"last {range_label}")
    t.kpi_grid([
        ("Transactions", t.num(n), f"from {int(r.get('accounts') or 0):,} accounts"),
        ("Volume", unit(t.num(r.get("amount_eur")), "EUR"), f"avg {float(r.get('avg_amount_eur') or 0):,.2f} EUR each"),
        ("Approval rate", unit(t.num(approval, 1), "%"), f"{int(r.get('declined') or 0):,} declined"),
        ("Throughput", unit(t.num(k.get("throughput_eps"), 1), "ev/s"), "processed by Spark, last 5 min"),
    ], "ov-activity")

    t.section("ii.", "Pipeline at a glance", "all time")
    t.kpi_grid([
        ("Latency Kafka → database", unit(t.num(k.get("latency_5min_s"), 1), "s"), "average, last 5 min"),
        ("Kafka consumer lag", t.num(k.get("kafka_lag")), "messages not read yet"),
        ("Invalid records", t.num(k.get("invalid_events")), "set aside in bad_events", True),
        ("Duplicates skipped", t.num(k.get("duplicates_skipped")), "never counted twice"),
    ], "ov-pipeline")

    t.section("iii.", "Volume per minute", "by event time")
    vol = safe(q.volume_per_minute, minutes)
    with st.container(border=True):
        t.card_title("Transactions per minute", "Stacked by status")
        if has(vol):
            status_bars(vol, "ov_status")
        else:
            empty()


def overview():
    """Overview page: title, statement and live numbers."""
    t.hero()
    t.statement("Every payment is an event. We read each one exactly once, place it in the minute it truly "
                "happened, set aside what is broken, and never count the same money twice.")
    overview_live()


@st.fragment(run_every=REFRESH_SECONDS)
def transactions():
    """Transactions page: volumes, amounts, status and currencies."""
    vol = safe(q.volume_per_minute, minutes)
    t.section("i.", "Volume over time", f"last {range_label} · event time")
    left, right = st.columns(2)
    with left, st.container(border=True):
        t.card_title("Transactions per minute", "Stacked by status")
        if has(vol):
            status_bars(vol, "tx_status", 280)
        else:
            empty()
    with right, st.container(border=True):
        t.card_title("Amount per minute", "In EUR, converted with fixed demo rates")
        if has(vol):
            t.show(t.line(vol["window_start"], [("EUR", vol["total_amount_eur"].astype(float), t.GREEN)],
                          height=280, area=True), "tx_amount")
        else:
            empty()

    t.section("ii.", "The mix", f"last {range_label}")
    left, right = st.columns(2)
    status = safe(q.status_distribution, minutes)
    with left, st.container(border=True):
        t.card_title("By status", "Share of all transactions")
        if has(status):
            total = status["txn_count"].sum()
            fig = go.Figure(go.Bar(
                y=status["status"], x=status["txn_count"].astype(int), orientation="h",
                marker=dict(color=[t.STATUS_COLORS.get(s, t.MUTED) for s in status["status"]]),
                text=[f"{s.title()} · {100 * c / total:.1f}%" for s, c in zip(status["status"], status["txn_count"])],
                textposition="outside", cliponaxis=False, textfont=dict(family=t.BODY, color=t.INK, size=13),
                hovertemplate="%{y}: %{x:,} transactions<extra></extra>"))
            fig = t.style(fig, 210, legend=False, hover="closest")
            fig.update_yaxes(showgrid=False, autorange="reversed")
            fig.update_xaxes(showgrid=True, showticklabels=False, ticks="", showline=False,
                             range=[0, float(status["txn_count"].max()) * 1.45])
            t.show(fig, "tx_status_mix")
        else:
            dbt_wait()
    cur = safe(q.volume_by_currency)
    with right, st.container(border=True):
        t.card_title("By currency", "All time · local amounts are never added across currencies")
        if has(cur):
            t.show(t.hbar(cur["currency"], cur["total_amount_eur"].astype(float),
                          text=[f"€ {float(v):,.0f}" for v in cur["total_amount_eur"]], height=230), "tx_currency")
        else:
            dbt_wait()

    if has(cur):
        t.section("iii.", "Currency ledger", "fixed demo rates")
        t.table(cur, [("currency", "Currency", "text"), ("rate_to_eur", "Rate to EUR", "num"),
                      ("txn_count", "Transactions", "int"), ("total_amount_local", "Local amount", "int"),
                      ("total_amount_eur", "In EUR", "eur")], "tx-ledger")


@st.fragment(run_every=REFRESH_SECONDS)
def pipeline():
    """Pipeline page: throughput, Kafka lag, delays and the logbook."""
    k = safe(q.kpis) or {}
    prog = safe(q.stream_progress, minutes)
    health = safe(q.pipeline_health, minutes)

    t.section("i.", "Streaming performance", f"last {range_label}")
    t.kpi_grid([
        ("Throughput", unit(t.num(k.get("throughput_eps"), 1), "ev/s"), "average, last 5 min"),
        ("Kafka lag", t.num(k.get("kafka_lag")), "messages behind, latest batch"),
        ("Last batch", unit(t.num(k.get("last_batch_age_s")), "s ago"), "progress saved every 10 s"),
        ("Restarts", t.num(k.get("restarts")), "each one resumed from its checkpoint"),
    ], "pl-kpis")
    st.write("")
    left, right = st.columns(2)
    with left, st.container(border=True):
        t.card_title("Throughput per micro-batch", "Events per second: processed vs arriving")
        if has(prog):
            t.show(t.line(prog["batch_timestamp"], [("Processed", prog["processed_rows_per_second"], t.GREEN),
                                                    ("Arriving", prog["input_rows_per_second"], t.TERRA)]), "pl_tp")
        else:
            empty()
    with right, st.container(border=True):
        t.card_title("Kafka consumer lag", "Messages waiting in Kafka")
        if has(prog):
            t.show(t.line(prog["batch_timestamp"], [("Lag", prog["kafka_lag"], t.GREEN)], area=True), "pl_lag")
        else:
            empty()

    t.section("ii.", "Delays & schema", "by processing time")
    left, right = st.columns(2)
    with left, st.container(border=True):
        t.card_title("Latency Kafka → PostgreSQL", "Seconds, per minute")
        if has(health):
            t.show(t.line(health["minute"], [("Average", health["avg_processing_latency_s"].astype(float), t.GREEN),
                                             ("Max", health["max_processing_latency_s"].astype(float), t.TERRA)]),
                   "pl_latency")
        else:
            dbt_wait()
    with right, st.container(border=True):
        t.card_title("Schema versions", "v1 and v2 events side by side (schema evolution)")
        if has(health):
            fig = go.Figure([
                go.Bar(x=health["minute"], y=health["schema_v1_events"], name="v1", marker_color=t.GREEN,
                       marker_line=dict(color=t.PAPER, width=1)),
                go.Bar(x=health["minute"], y=health["schema_v2_events"], name="v2", marker_color=t.TERRA,
                       marker_line=dict(color=t.PAPER, width=1))])
            fig.update_layout(barmode="stack")
            t.show(t.style(fig), "pl_schema")
        else:
            dbt_wait()

    t.section("iii.", "Logbook", "starts · restarts · failures · replays")
    events = safe(q.pipeline_events, 12)
    if has(events):
        events["logged_at"] = pd.to_datetime(events["logged_at"], utc=True).dt.strftime("%b %d, %H:%M:%S")
        t.table(events.fillna(""), [("logged_at", "When", "text"), ("event_type", "Event", "text"),
                                    ("query_name", "Query", "text"), ("details", "Details", "text")], "pl-log")
    else:
        empty()


@st.fragment(run_every=REFRESH_SECONDS)
def quality():
    """Data quality page: invalid records, duplicates and late events."""
    k = safe(q.kpis) or {}
    total = int(k.get("total_events") or 0)
    invalid = int(k.get("invalid_events") or 0)
    share = f"{100 * invalid / (total + invalid):.2f}% of all input" if total + invalid else "-"

    t.section("i.", "The ledger is clean", "all time")
    t.kpi_grid([
        ("Valid transactions", t.num(total), "stored once in raw_transactions"),
        ("Invalid records", t.num(invalid), share, True),
        ("Duplicates skipped", t.num(k.get("duplicates_skipped")), "same event_id sent again"),
        ("Late events", t.num(k.get("late_events")), "arrived over 60 s late"),
        ("Beyond watermark", t.num(k.get("beyond_watermark")), "over 10 min late, kept raw only"),
    ], "dq-kpis", cols=5)
    st.write("")

    left, right = st.columns([3, 2])
    bad = safe(q.invalid_by_reason)
    with left, st.container(border=True):
        t.card_title("Why records were rejected", "Dead-letter table bad_events")
        if has(bad):
            t.show(t.hbar(bad["error_reason"].str.replace("_", " "), bad["events"].astype(int),
                          text=[f"{int(v):,}" for v in bad["events"]], height=70 + 34 * len(bad)), "dq_reasons")
        else:
            empty("No invalid records yet.")
    vol = safe(q.volume_per_minute, minutes)
    with right, st.container(border=True):
        t.card_title("Late events per minute", "Still counted in their own minute")
        if has(vol):
            fig = go.Figure(go.Bar(x=vol["window_start"], y=vol["late_event_count"], marker_color=t.GREEN,
                                   hovertemplate="%{y} late events<extra></extra>"))
            t.show(t.style(fig, 70 + 34 * max(len(bad) if has(bad) else 4, 4), legend=False), "dq_late")
        else:
            empty()

    t.section("ii.", "The rules", "checked in Spark, re-checked by dbt every 30 s")
    rules = [("Malformed JSON", "the message can't be parsed"),
             ("Missing field", "event_id, account_id, amount or event_time is absent"),
             ("Bad timestamp", "unparseable, or more than 5 minutes in the future"),
             ("Bad amount", "below 0.01, or above 1,000,000 EUR"),
             ("Unknown currency", "not in the fx_rates table"),
             ("Unknown status", "not APPROVED, DECLINED or PENDING")]
    t.table(pd.DataFrame(rules, columns=["rule", "meaning"]),
            [("rule", "Rule", "text"), ("meaning", "Rejected when…", "text")], "dq-rules")


@st.fragment(run_every=REFRESH_SECONDS)
def risk():
    """Risk page: the three simulated rules and the flagged activity."""
    st.markdown('<div class="notice">Simulated: simple, explainable rules on synthetic data. '
                'This is not a fraud detection system.</div>', unsafe_allow_html=True)
    sus = safe(q.suspicious, minutes)
    counts = sus["rule"].value_counts() if has(sus) else {}
    rules = [
        ("high_velocity", "High velocity", "Six or more payments from one account within five minutes."),
        ("large_amount", "Large amount", "A single payment of 3,000 EUR or more, compared in EUR."),
        ("country_hopping", "Country hopping", "Payments from three or more countries by one account within an hour."),
    ]

    t.section("i.", "Three rules", f"last {range_label}")
    cols = st.columns(3)
    for col, (key, name, definition) in zip(cols, rules):
        n = int(counts.get(key, 0)) if len(counts) else 0
        col.markdown(t.reveal(f'<div class="rule"><div class="name">{t.esc(name)}</div>'
                              f'<div class="count">{t.num(n)}</div><div class="def">{t.esc(definition)}</div></div>',
                              f"rule-{key}"), unsafe_allow_html=True)

    t.section("ii.", "Flagged activity", "thresholds live in dbt/dbt_project.yml")
    if has(sus):
        choice = st.radio("Rule", ["All"] + [name for _, name, _ in rules], horizontal=True,
                          label_visibility="collapsed", key="rule_filter")
        shown = sus if choice == "All" else sus[sus["rule"] == {n: k for k, n, _ in rules}[choice]]
        shown = shown.head(40).copy()
        shown["window_start"] = pd.to_datetime(shown["window_start"], utc=True).dt.strftime("%b %d, %H:%M")
        shown["rule"] = shown["rule"].str.replace("_", " ")
        t.table(shown, [("window_start", "When", "text"), ("rule", "Rule", "text"), ("account_id", "Account", "text"),
                        ("txn_count", "Payments", "int"), ("total_amount_eur", "Amount", "eur"),
                        ("details", "Why", "text")], f"risk-table-{choice}")
    else:
        empty("No indicators in this time range yet (dbt refreshes every 30 s).")


# ================================================================ render the chosen page
HEADS = {
    "Transactions": ("02", "Transactions", "How much money moves, in which currency, and how much of it is approved.", "€ → EUR"),
    "Pipeline": ("03", "Pipeline", "How fast Spark keeps up with Kafka, and every start, restart and replay.", "5s trigger"),
    "Data quality": ("04", "Data quality", "What we reject, what we skip, and what arrives late.", "dedup ✓"),
    "Risk indicators": ("05", "Risk indicators", "Simulated, explainable rules. A demo, not a fraud detector.", "simulated!"),
}

if page == "Overview":
    overview()
else:
    t.page_head(*HEADS[page])
    {"Transactions": transactions, "Pipeline": pipeline, "Data quality": quality, "Risk indicators": risk}[page]()
