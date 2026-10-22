# Screenshots

Captured by `scripts/screenshots.py` from the running dashboard in a headless
browser (the installed Edge or Chrome, through Playwright), so they can be
taken again the same way after a change to the page.

| filename | what it shows | set |
|---|---|---|
| `dashboard.png` | the whole page | demo |
| `pipeline-health.png` | the four health tiles | demo |
| `affinity-graph.png` | the graph with its legend | demo |
| `real-data-graph.png` | the graph on real traffic, `Item …` labels and `cat-…` categories | real |
| `related-learned.png` | trending, and related products ranked by the learned model | real |
| `demo.gif` | a walk down the page, then related products switched to the learned model | real |

```bash
pip install playwright==1.48.0 pillow
```

## The demo set

```bash
docker compose down -v && docker compose up -d --build   # seed + producer start
python scripts/screenshots.py demo
```

Wait about six minutes first: trending appears within one, and the first
pairs about five after that (one-minute window + two-minute co-view gap +
two-minute watermark).

## The real set

After a replay and a trained model whose vectors fit it (README, "Is it any
good?"), with the dashboard on the real catalogue:

```bash
CATALOG_FILE=data/catalog_retailrocket.json docker compose up -d dashboard
python scripts/screenshots.py real
```

Capture within 30 minutes of the replay finishing: trending and the graph
look back 30 minutes, and after that the page correctly says it is showing
retained history rather than live activity.
