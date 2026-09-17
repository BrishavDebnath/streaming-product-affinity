"""
Streamlit dashboard: browse products, send clicks, watch the pipeline work.

    streamlit run src/ui/dashboard.py

Three panels show the PIPELINE rather than decorate the page:

  * Events per minute      read back from what Spark wrote, not a producer counter
  * Processing delay       time between a window ending and its final numbers
                           being saved
  * Products viewed together   the co-occurrence pairs as a graph

Products come from src.common.catalog, the same module the producer uses, so
a click here lands in the same product space the pipeline aggregates.
"""

import os
import re
import sys
import time

# `streamlit run src/ui/dashboard.py` puts src/ui on sys.path, NOT the project
# root, and Streamlit rewrites sys.path for the script it runs - so PYTHONPATH
# does not reliably help. Bootstrapping the root here means the dashboard runs
# from any directory with no environment variable at all.
sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd
import requests
import streamlit as st
from kafka import KafkaProducer

from src.common import catalog, config
from src.common.kafka_io import JsonValueSerializer, StringKeySerializer

st.set_page_config(page_title="Streaming Product Affinity", layout="wide")

CATEGORY_COLOUR = {
    "laptop": "#2563eb", "laptop-acc": "#60a5fa",
    "phone": "#059669", "phone-acc": "#34d399",
    "audio": "#d97706", "footwear": "#dc2626",
    "unknown": "#9ca3af",
}

CATEGORY_LABEL = {
    "laptop": "Laptops", "laptop-acc": "Laptop accessories",
    "phone": "Phones", "phone-acc": "Phone accessories",
    "audio": "Audio", "footwear": "Footwear",
}

COLLECTION_LABEL = {
    config.COLL_TRENDING: "Trending rows",
    config.COLL_PAIRS: "Product pairs",
    config.COLL_DLQ: "Rejected events",
}


def minutes_in(duration: str) -> float:
    """'2 minutes' -> 2.0, '30 seconds' -> 0.5, '1 hour' -> 60.0."""
    match = re.match(r"\s*([\d.]+)\s*(second|minute|hour)", duration or "")
    if not match:
        return 0.0
    value, unit = float(match.group(1)), match.group(2)
    return value * {"second": 1 / 60, "minute": 1, "hour": 60}[unit]


# How long after an event its pair can first be saved: the window has to end,
# then the join holds output back by the co-view gap, then the watermark.
PAIR_DELAY_MINUTES = round(minutes_in(config.COOCCURRENCE_WINDOW)
                           + minutes_in(config.CO_VIEW_GAP)
                           + minutes_in(config.WATERMARK))


@st.cache_resource(show_spinner=False)
def get_producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=JsonValueSerializer(),
        key_serializer=StringKeySerializer(),
    )


def emit(user_id, product_id, event_type):
    get_producer().send(config.TOPIC_EVENTS, key=user_id, value={
        "event_id": f"{user_id}-{product_id}-{time.time()}",
        "user_id": user_id, "product_id": product_id,
        "event_type": event_type, "timestamp": time.time(),
    })


def api(path, quiet=False):
    try:
        r = requests.get(f"{config.API_BASE_URL}{path}", timeout=5)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        if not quiet:
            st.warning(f"Could not reach the API: {exc}")
        return None


def build_dot(graph, related=None):
    """
    Graphviz DOT for the co-occurrence graph.

    st.graphviz_chart renders a DOT string directly, so this needs no extra
    Python dependency - no networkx, no pyvis, no plotly.

    `related(category_a, category_b)` decides the line style: solid for
    categories the demo generator links directly, dashed for the rest. It is
    display only - the pipeline itself never sees categories.
    """
    lines = ["graph G {",
             '  layout=neato; overlap=false; splines=true;',
             '  bgcolor="transparent";',
             '  node [shape=box style="rounded,filled" fontname="Helvetica" '
             'fontsize=10 fontcolor="white" penwidth=0];']
    category_of = {node["id"]: node["category"] for node in graph["nodes"]}
    for node in graph["nodes"]:
        colour = CATEGORY_COLOUR.get(node["category"], CATEGORY_COLOUR["unknown"])
        label = node["name"].replace('"', "'")
        lines.append(f'  n{node["id"]} [label="{label}" fillcolor="{colour}"];')

    affinities = [e["affinity"] for e in graph["edges"]] or [1.0]
    strongest = max(affinities) or 1.0
    for edge in graph["edges"]:
        width = 1.0 + 5.0 * (edge["affinity"] / strongest)
        direct = related is None or related(category_of.get(edge["source"]),
                                            category_of.get(edge["target"]))
        style = "solid" if direct else "dashed"
        lines.append(
            f'  n{edge["source"]} -- n{edge["target"]} '
            f'[penwidth={width:.2f} color="#94a3b8" style={style} '
            f'label="{edge["pair_count"]}" fontsize=8 fontcolor="#64748b"];')
    lines.append("}")
    return "\n".join(lines)


st.title("Streaming Product Affinity Pipeline")
st.caption("Kafka -> Spark Structured Streaming -> MongoDB -> FastAPI")

# ------------------------------------------------------------------ sidebar
with st.sidebar:
    st.header("Settings")
    user_id = st.selectbox("Shopper ID for your clicks", catalog.USERS, index=0,
                           help="Clicks on the View and Add to cart buttons "
                                "are sent as this shopper.")
    auto = st.checkbox("Refresh every 5 seconds", value=False)
    if st.button("Refresh now"):
        st.rerun()

    st.divider()
    st.caption(f"Kafka: `{config.KAFKA_BOOTSTRAP}`")
    st.caption(f"API: `{config.API_BASE_URL}`")

    health = api("/health", quiet=True)
    if health and health["status"] == "ok":
        st.success("API connected")
    elif health:
        st.error("API is running but cannot reach MongoDB")
    else:
        st.error("API is not reachable. Check it with `docker compose ps` "
                 "and `docker compose logs api`.")

    stats = api("/stats", quiet=True)
    if stats:
        st.subheader("Saved in MongoDB")
        for name, count in stats["collections"].items():
            st.metric(COLLECTION_LABEL.get(name, name), f"{count:,}")

# ------------------------------------------------------- pipeline telemetry
st.subheader("Pipeline health")
pipe = api("/pipeline", quiet=True)
tput = api("/throughput?windows=30", quiet=True)

m1, m2, m3, m4 = st.columns(4)
if pipe and pipe.get("lag_seconds") is not None:
    m1.metric("Processing delay", f"{pipe['lag_seconds']:.1f} s",
              help="How long after a one-minute window ends its final numbers "
                   "are saved. A few seconds up to the trigger interval is "
                   "healthy; a number that keeps growing means Spark is "
                   "falling behind.")
else:
    m1.metric("Processing delay", "-",
              help="Shown once the first one-minute window has ended.")
if pipe and pipe.get("staleness_seconds") is not None:
    m2.metric("Last update", f"{pipe['staleness_seconds']:.0f} s ago",
              help="Time since Spark last saved any results.")
else:
    m2.metric("Last update", "-")
    if pipe:
        st.info(pipe.get("message", "Waiting for the first results."))

def _naive_utc(value):
    stamp = pd.Timestamp(value)
    return stamp.tz_convert(None) if stamp.tzinfo else stamp


# The newest window is still filling up, so its count covers only part of a
# minute (it showed half the real rate). Use the last window that has ended.
now_utc = pd.Timestamp.now(tz="UTC").tz_convert(None)
finished = [s for s in (tput or {}).get("series", [])
            if s.get("window_end") and _naive_utc(s["window_end"]) <= now_utc]
if finished:
    recent = finished[-1]
    m3.metric("Events in the last full minute", f"{recent['events']:,}")
    m4.metric("Events per second", recent["events_per_second"] or "-")
else:
    m3.metric("Events in the last full minute", "-")
    m4.metric("Events per second", "-")

if pipe and pipe.get("trigger_interval"):
    st.caption(f"Spark processes new events every {pipe['trigger_interval']}. "
               f"Events arriving more than {pipe['watermark']} late are ignored.")

# ------------------------------------------------------------- throughput
st.subheader("Events per minute")
if tput and tput["series"]:
    df = pd.DataFrame(tput["series"])
    df["Minute"] = pd.to_datetime(df["window_start"]).dt.strftime("%H:%M")
    st.bar_chart(df.set_index("Minute")[["events"]].rename(
        columns={"events": "Events"}), height=220)
    st.caption(f"Last {len(df)} minutes, counted from what Spark saved - not "
               "from the producer's own counter. Times are UTC. The last bar "
               "is the minute still in progress.")
else:
    st.info("No data yet. Start the producer; the first numbers appear within "
            "about a minute.")

# ------------------------------------------------------------------ products
st.subheader("Products")
st.caption("Click to send your own events into the pipeline.")
cols = st.columns(4)
for i, item in enumerate(catalog.PRODUCTS):
    with cols[i % 4]:
        st.markdown(f"**{item['name']}**")
        st.caption(f"Rs {item['price']:,} - "
                   f"{CATEGORY_LABEL.get(item['category'], item['category'])}")
        c1, c2 = st.columns(2)
        if c1.button("View", key=f"v{item['id']}"):
            emit(user_id, item["id"], "view")
            st.toast(f"Viewed {item['name']}")
        if c2.button("Add to cart", key=f"c{item['id']}"):
            emit(user_id, item["id"], "add_to_cart")
            st.toast(f"Added {item['name']} to cart")

# ------------------------------------------------------ trending + recs
left, right = st.columns(2)

with left:
    st.subheader("Trending now")
    data = api("/trending?limit=8", quiet=True)
    if data and data["trending"]:
        st.caption(f"Most popular products in the last {data['minutes']} minutes")
        rows = [{"Product": r["name"] or r["product_id"],
                 "Popularity score": r["score"],
                 "Events": r["event_count"],
                 "Most shoppers in one minute": r.get("peak_users_per_window")}
                for r in data["trending"]]
        st.dataframe(pd.DataFrame(rows), hide_index=True,
                     width="stretch")
    elif data:
        st.info(data.get("message", "No data yet."))

with right:
    st.subheader("Related products")
    target = st.selectbox("Shoppers who viewed", catalog.product_ids(),
                          format_func=catalog.name_of)
    data = api(f"/related-products/{target}?limit=8", quiet=True)
    if data:
        if data["source"] == "trending_fallback":
            st.info("Not enough data for this product yet, so these are "
                    "trending products instead.")
            rows = [{"Product": r["name"] or r["product_id"],
                     "Popularity score": r.get("score")}
                    for r in data["related_products"]]
        else:
            st.success("...also viewed these products")
            rows = [{"Product": r["name"] or r["product_id"],
                     "Match score": r.get("affinity"),
                     "Times seen together": r.get("pair_count")}
                    for r in data["related_products"]]
        if rows:
            st.dataframe(pd.DataFrame(rows), hide_index=True,
                         width="stretch")

# ------------------------------------------------------------ affinity graph
st.subheader("Products viewed together")
st.caption("Each line joins two products viewed in the same shopping session. "
           "Thicker lines mean a stronger link; the number is how many times "
           "the pair was seen. Solid lines join categories that go together "
           "(a laptop and a laptop sleeve); dashed lines join categories that "
           "do not (shoes and a phone) - shoppers wandering, which the demo "
           "data does in about 15% of views. The weakest links are hidden "
           "until you show more lines.")
links = st.slider("Lines shown (strongest first)", min_value=10, max_value=70,
                  value=30, step=5,
                  help="12 products make at most 66 pairs. Move this to the "
                       "right to see the weak cross-category links.")
graph = api(f"/graph?limit={links}", quiet=True)
if graph and graph["edges"]:
    gcol, lcol = st.columns([3, 1])
    with gcol:
        st.graphviz_chart(build_dot(graph, catalog.categories_related),
                          width="stretch")
    with lcol:
        st.metric("Products shown", graph["node_count"])
        st.metric("Lines drawn", graph["edge_count"])
        st.markdown("**Categories**")
        for category, label in CATEGORY_LABEL.items():
            st.markdown(
                f'<span style="color:{CATEGORY_COLOUR[category]};'
                f'font-size:20px">&#9632;</span> '
                f'<span style="font-size:13px">{label}</span>',
                unsafe_allow_html=True)
        category_of = {n["id"]: n["category"] for n in graph["nodes"]}
        cross = [e for e in graph["edges"]
                 if not catalog.categories_related(category_of[e["source"]],
                                                   category_of[e["target"]])]
        st.metric("Cross-category lines", len(cross))
        st.markdown("**Strongest links**")
        for edge in sorted(graph["edges"],
                           key=lambda e: e["affinity"], reverse=True)[:5]:
            st.caption(f"{catalog.name_of(edge['source'])} + "
                       f"{catalog.name_of(edge['target'])} - "
                       f"score {edge['affinity']}")
else:
    st.info("No product pairs yet. The first ones appear about "
            f"{PAIR_DELAY_MINUTES} minutes after events start arriving - keep "
            "the producer running.")

if auto:
    time.sleep(5)
    st.rerun()
