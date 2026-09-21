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

import json
import os
import re
import shutil
import subprocess
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

# A real dataset's categories are numeric ids nobody hand-picked a colour for.
# Colouring them all "unknown" grey threw away the only grouping the graph
# has.
PALETTE = ["#2563eb", "#059669", "#d97706", "#dc2626", "#7c3aed", "#0891b2",
           "#ca8a04", "#be185d", "#15803d", "#4338ca", "#b45309", "#0f766e"]


# Every node gets this when there are too many categories to tell apart.
NEUTRAL_COLOUR = "#475569"


def colour_map(categories) -> dict:
    """One colour per category ON THIS GRAPH - or none at all.

    Assigned by position in the sorted list rather than by hashing the name:
    a hash collides, and two different categories sharing a colour makes the
    picture say something the data does not. For the same reason there is no
    wrapping round the palette: a real dataset put 31 categories on one graph
    and 12 colours made unrelated products look like one group. Past the
    palette, every node is NEUTRAL_COLOUR and `labels_categories` tells the
    graph to print each category inside its box instead. Demo categories keep
    their hand-picked colours so the generated data still reads the same.
    """
    ordered = sorted(set(categories), key=lambda c: (c == "uncategorised", str(c)))
    own = [c for c in ordered
           if c not in CATEGORY_COLOUR and c and c != "uncategorised"]
    if len(own) > len(PALETTE):
        return dict.fromkeys(ordered, NEUTRAL_COLOUR)
    spare = ([c for c in PALETTE if c not in CATEGORY_COLOUR.values()]
             + [c for c in PALETTE if c in CATEGORY_COLOUR.values()])
    colours, taken = {}, 0
    for category in ordered:
        if category in CATEGORY_COLOUR:
            colours[category] = CATEGORY_COLOUR[category]
        elif not category or category == "uncategorised":
            colours[category] = CATEGORY_COLOUR["unknown"]
        else:
            colours[category] = spare[taken]
            taken += 1
    return colours


def labels_categories(colours: dict) -> bool:
    """True when colour cannot carry the category, so the label must."""
    return len(colours) > 1 and set(colours.values()) == {NEUTRAL_COLOUR}


# How many products get click-to-send buttons. The demo catalogue has twelve;
# a catalogue built from a real dataset has tens of thousands, and Streamlit
# would render two buttons for every one of them.
CLICKABLE_PRODUCTS = 12

# How many catalogue products the "Related products" chooser offers besides
# the ones currently trending. Same reason: a 50,000-entry select is a slow
# page and mostly items with no data behind them.
CHOOSABLE_PRODUCTS = 50

# Legend entries. Never fewer than the colours in use, so every colour on the
# graph is named; a graph with more categories is not coloured at all.
LEGEND_CATEGORIES = 12   # = len(PALETTE): past that, colours are not used

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


def api(path, quiet=False, timeout=5):
    try:
        r = requests.get(f"{config.API_BASE_URL}{path}", timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        if not quiet:
            st.warning(f"Could not reach the API: {exc}")
        return None


def ago(seconds: float) -> str:
    """'42 s ago', '6 min ago', '1 h 1 min ago'. Seconds alone stop being
    readable after a minute: a finished replay showed '3649 s ago'."""
    seconds = max(0, int(round(seconds)))
    if seconds < 60:
        return f"{seconds} s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min ago"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours} h {minutes} min ago" if minutes else f"{hours} h ago"
    return f"{hours // 24} days ago"


def build_dot(graph, related=None, colours=None, counts_on_lines=True):
    """
    Graphviz DOT for the co-occurrence graph.

    Rendered by the `dot` binary into an SVG with hover boxes (graph_page), or
    by st.graphviz_chart when that binary is missing - no Python dependency
    either way.

    `counts_on_lines` prints each pair count on its line. The hover page turns
    it off: with thirty lines the numbers overlap each other and the boxes,
    so there each count appears in a box when the pointer is on its line.
    Every edge carries id="e<i>" so the page can find it again in the SVG.

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
    colours = colours or colour_map(category_of.values())
    for node in graph["nodes"]:
        colour = colours.get(node["category"], CATEGORY_COLOUR["unknown"])
        label = node["name"].replace('"', "'")
        if labels_categories(colours):
            label += "\\n" + str(node["category"]).replace('"', "'")
        lines.append(f'  n{node["id"]} [label="{label}" fillcolor="{colour}"];')

    affinities = [e["affinity"] for e in graph["edges"]] or [1.0]
    strongest = max(affinities) or 1.0
    for index, edge in enumerate(graph["edges"]):
        width = 1.0 + 5.0 * (edge["affinity"] / strongest)
        direct = related is None or related(category_of.get(edge["source"]),
                                            category_of.get(edge["target"]))
        style = "solid" if direct else "dashed"
        label = (f' label="{edge["pair_count"]}" fontsize=8 fontcolor="#64748b"'
                 if counts_on_lines else "")
        lines.append(
            f'  n{edge["source"]} -- n{edge["target"]} '
            f'[id="e{index}" penwidth={width:.2f} color="#94a3b8" '
            f'style={style}{label}];')
    lines.append("}")
    return "\n".join(lines)


# Height of the hover graph. Fixed, because an embedded page cannot size its
# own frame; the SVG scales to fit inside it, keeping its proportions.
GRAPH_HEIGHT_PX = 560


def render_svg(dot: str) -> str | None:
    """The DOT drawn by Graphviz, or None when the binary is not installed."""
    if not shutil.which("dot"):
        return None
    try:
        done = subprocess.run(["dot", "-Tsvg"], input=dot.encode("utf-8"),  # noqa: S603, S607
                              capture_output=True, timeout=20, check=True)
    except (subprocess.SubprocessError, OSError):
        return None
    svg = done.stdout.decode("utf-8")
    start = svg.find("<svg")
    if start < 0:
        return None
    # Let CSS size it: Graphviz writes a fixed width and height in points.
    head, rest = svg[start:].split(">", 1)
    head = re.sub(r'\s(width|height)="[^"]*"', "", head)
    return head + ">" + rest


def graph_page(svg: str, graph: dict, height: int = GRAPH_HEIGHT_PX) -> str:
    """The SVG plus a small translucent box that names a line on hover.

    Graphviz puts each edge in <g id="e<i>">; the script looks each one up in
    a table built from the same edge list, so the box always describes the
    line under the pointer. A wide invisible copy of every line makes thin
    dashed ones easy to hit.
    """
    names = {n["id"]: n["name"] for n in graph["nodes"]}
    info = {f"e{i}": {"a": names.get(e["source"], str(e["source"])),
                      "b": names.get(e["target"], str(e["target"])),
                      "n": e["pair_count"], "s": round(e["affinity"], 1)}
            for i, e in enumerate(graph["edges"])}
    # json.dumps output is safe inside <script> once "</" cannot close it.
    table = json.dumps(info).replace("</", "<\\/")
    return f"""<style>
  body {{ margin: 0; font-family: "Source Sans Pro", sans-serif; }}
  #graph svg {{ width: 100%; height: {height}px; display: block; }}
  #graph .hit {{ stroke: transparent; stroke-width: 14px; fill: none;
                 pointer-events: stroke; cursor: default; }}
  #graph g.edge.on path:not(.hit) {{ stroke: #334155; }}
  #tip {{ position: fixed; pointer-events: none; opacity: 0;
          transition: opacity .12s; background: rgba(15, 23, 42, .78);
          color: #fff; padding: 6px 10px; border-radius: 6px;
          font-size: 13px; line-height: 1.4; white-space: nowrap;
          backdrop-filter: blur(2px); }}
  #tip b {{ font-weight: 600; }}
</style>
<div id="graph">{svg}</div>
<div id="tip"></div>
<script>
const EDGES = {table};
const tip = document.getElementById("tip");
function show(ev, d) {{
  tip.replaceChildren();
  const top = document.createElement("div");
  top.textContent = d.a + " + " + d.b;
  const count = document.createElement("b");
  count.textContent = "Seen together " + d.n.toLocaleString() + " times";
  tip.append(top, count);
  const x = ev.clientX + 14, y = ev.clientY + 14;
  const w = tip.offsetWidth, h = tip.offsetHeight;
  tip.style.left = (x + w > innerWidth ? ev.clientX - w - 10 : x) + "px";
  tip.style.top = (y + h > innerHeight ? ev.clientY - h - 10 : y) + "px";
  tip.style.opacity = 1;
}}
document.querySelectorAll("#graph g.edge").forEach(g => {{
  const d = EDGES[g.id];
  if (!d) return;
  g.querySelectorAll("title").forEach(t => t.remove());
  g.querySelectorAll("a").forEach(a => a.removeAttribute("xlink:title"));
  const line = g.querySelector("path");
  if (line) {{
    const hit = line.cloneNode();
    hit.removeAttribute("stroke-dasharray");
    hit.setAttribute("class", "hit");
    g.appendChild(hit);
  }}
  g.addEventListener("mousemove", ev => {{ g.classList.add("on"); show(ev, d); }});
  g.addEventListener("mouseleave", () => {{
    g.classList.remove("on"); tip.style.opacity = 0; }});
}});
</script>"""


def embed(page: str, height: int) -> None:
    """An HTML page in a frame. st.iframe replaced components.html in
    Streamlit 1.5x; older installs still have only the latter. The page is
    built here from the API's own rows, never from user input."""
    if hasattr(st, "iframe"):
        st.iframe(page, height=height)
    else:                                               # pragma: no cover
        import streamlit.components.v1 as components
        components.html(page, height=height)


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
    m2.metric("Last update", ago(pipe["staleness_seconds"]),
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
    window_age = (now_utc - _naive_utc(recent["window_end"])).total_seconds() / 60
    # "the last full minute" is only true while events are still arriving.
    # After a replay finishes, the newest completed window can be an hour old,
    # and labelling it as the last minute misreports a stopped pipeline as a
    # running one.
    label = ("Events in the last full minute" if window_age < 2
             else "Events in the newest full minute")
    m3.metric(label, f"{recent['events']:,}",
              help=f"Window {_naive_utc(recent['window_start']):%H:%M}-"
                   f"{_naive_utc(recent['window_end']):%H:%M} UTC"
                   + (f", {window_age:.0f} minutes ago" if window_age >= 2 else ""))
    rate = recent["events_per_second"]
    m4.metric("Events per second", f"{rate:,.1f}" if rate is not None else "-",
              help="Over that same window.")
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
    # The chart shows the newest windows Spark wrote, which are not
    # necessarily the last N minutes: after a replay they can be an hour old.
    # Saying "last 6 minutes" there would misdescribe the data on screen.
    span_end = _naive_utc(df["window_end"].iloc[-1])
    still_open = span_end > now_utc
    span_age = (now_utc - span_end).total_seconds() / 60
    st.caption(
        f"{len(df)} one-minute windows, "
        f"{_naive_utc(df['window_start'].iloc[0]):%H:%M}-{span_end:%H:%M} UTC"
        + ("" if still_open or span_age < 2
           else f", ending {span_age:.0f} minutes ago")
        + ". Counted from what Spark saved, not from the producer's own "
        + ("counter. The last bar is the minute still in progress."
           if still_open else "counter."))
else:
    st.info("No data yet. Start the producer; the first numbers appear within "
            "about a minute.")

# ------------------------------------------------------------------ products
st.subheader("Products")
# A demo catalogue has a dozen products; one built from a real dataset has
# tens of thousands, and a button pair for each would render for minutes.
clickable = catalog.PRODUCTS[:CLICKABLE_PRODUCTS]
st.caption("Click to send your own events into the pipeline."
           + (f" Showing {len(clickable)} of {len(catalog.PRODUCTS):,} "
              f"catalogue items." if len(catalog.PRODUCTS) > len(clickable) else ""))
cols = st.columns(4)
for i, item in enumerate(clickable):
    with cols[i % 4]:
        st.markdown(f"**{item['name']}**")
        # Real datasets have no prices - RetailRocket hashes its item
        # properties - so the catalogue carries None rather than a made-up
        # number, and the caption is the category alone.
        label = CATEGORY_LABEL.get(item["category"], item["category"])
        price = item.get("price")
        st.caption(f"Rs {price:,} - {label}" if price is not None else label)
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
    trending_ids = [r["product_id"] for r in (data or {}).get("trending", [])]
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
    # Products that currently HAVE data come first: on a real catalogue of
    # tens of thousands, a plain product list is both slow to render and
    # mostly items the pipeline has never seen.
    options = trending_ids + [p for p in catalog.product_ids()[:CHOOSABLE_PRODUCTS]
                              if p not in trending_ids]
    target = st.selectbox("Shoppers who viewed", options,
                          format_func=catalog.name_of)
    data = api(f"/related-products/{target}?limit=8", quiet=True)
    if data is None:
        st.info("No data for this product yet.")
    if data and data.get("window") == "latest_available":
        st.caption("Nothing recent for this product, so these come from the "
                   "most recent data it has.")
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
# The demo catalogue has hand-written category affinities to explain; a
# catalogue from a real dataset has only the category an item belongs to, so
# the caption must not claim knowledge the data does not contain.
st.caption("Each line joins two products viewed in the same shopping session. "
           "Thicker lines mean a stronger link. "
           + ("Solid lines join categories that go together (a laptop and a "
              "laptop sleeve); dashed lines join categories that do not (shoes "
              "and a phone) - shoppers wandering, which the demo data does in "
              "about 15% of views. "
              if catalog.is_demo() else
              "Solid lines join two products from the same category; dashed "
              "lines cross categories. ")
           + "The weakest links are hidden until you show more lines.")
links = st.slider("Lines shown (strongest first)", min_value=10, max_value=70,
                  value=30, step=5,
                  help=("12 products make at most 66 pairs. Move this to the "
                        "right to see the weak cross-category links."
                        if catalog.is_demo() else
                        f"The catalogue has {len(catalog.PRODUCTS):,} products, "
                        "far more pairs than a readable graph: this is the "
                        "strongest few. Move it right for weaker links."))
# The graph is the one slow read - it groups every pair row in the lookback,
# which after a replayed month is over a million of them (5-6 s, measured).
# The API caches it; this timeout is what lets the first, uncached call
# finish, instead of aborting at five seconds and reporting "no pairs yet"
# for data that is right there.
with st.spinner("Building the affinity graph..."):
    graph = api(f"/graph?limit={links}", quiet=True, timeout=30)
if graph and graph.get("window") == "latest_available":
    st.caption("No pairs in the last few minutes, so this is the most recent "
               f"{graph.get('lookback_minutes')} minutes of data that exists - "
               "the end of a finished run rather than what is happening now.")
if graph and graph["edges"]:
    # One colour assignment, shared by the picture and its legend.
    graph_colours = colour_map(n["category"] for n in graph["nodes"])
    gcol, lcol = st.columns([3, 1])
    with gcol:
        svg = render_svg(build_dot(graph, catalog.categories_related,
                                   graph_colours, counts_on_lines=False))
        if svg:
            embed(graph_page(svg, graph), height=GRAPH_HEIGHT_PX + 10)
            st.caption("Point at a line to see how many times the pair was "
                       "seen together.")
        else:
            st.graphviz_chart(
                build_dot(graph, catalog.categories_related, graph_colours),
                width="stretch")
            st.caption("The number on each line is how many times the pair "
                       "was seen together.")
    with lcol:
        st.metric("Products shown", graph["node_count"])
        st.metric("Lines drawn", graph["edge_count"])
        # The legend lists the categories ON THIS GRAPH. It used to list the
        # demo catalogue's six regardless, so against a real dataset it
        # advertised colours that appeared nowhere in the picture.
        st.markdown("**Categories**")
        shown = sorted({n["category"] for n in graph["nodes"]},
                       key=lambda c: (c == "uncategorised", c))
        if labels_categories(graph_colours):
            st.caption(f"{len(shown)} categories - too many to tell apart by "
                       "colour, so each product's category is printed under "
                       "its id.")
            shown = []
        for category in shown[:LEGEND_CATEGORIES]:
            st.markdown(
                f'<span style="color:{graph_colours[category]};'
                f'font-size:20px">&#9632;</span> '
                f'<span style="font-size:13px">'
                f'{CATEGORY_LABEL.get(category, category)}</span>',
                unsafe_allow_html=True)
        if len(shown) > LEGEND_CATEGORIES:
            st.caption(f"+ {len(shown) - LEGEND_CATEGORIES} more categories")
        category_of = {n["id"]: n["category"] for n in graph["nodes"]}
        cross = [e for e in graph["edges"]
                 if not catalog.categories_related(category_of[e["source"]],
                                                   category_of[e["target"]])]
        st.metric("Lines crossing categories",
                  f"{len(cross)} of {graph['edge_count']}")
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
