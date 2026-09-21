# Screenshots

The main README renders `pipeline-health.png`, `affinity-graph.png` and
`dashboard.png`, all cut from one full-page capture of the demo catalogue
(DevTools device toolbar at 1400 x 4200, then "Capture full size screenshot").

| filename | what to capture | with |
|---|---|---|
| `dashboard.png` | the whole page once trending and the graph have data | demo catalogue |
| `affinity-graph.png` | the graph alone, zoomed, with the legend beside it | demo catalogue |
| `pipeline-health.png` | the four-tile row: processing delay, last update, events in the last full minute, events/s | demo catalogue |
| `real-data-graph.png` | the graph on real traffic - `Item …` labels, `cat-…` categories | RetailRocket |
| `demo.gif` | 30-60 s: click a product, watch it arrive in trending | demo catalogue |

## Capturing the demo set

The dashboard's panels only have data while events are arriving, so:

```bash
docker compose down -v                 # a clean, empty stack
docker compose up -d --build           # seed + producer start automatically
```

Wait **six minutes**: trending appears within about one, and the first product
pairs about five after that (one-minute window + two-minute co-view gap +
two-minute watermark). Then capture at http://localhost:8501.

## Capturing the real-data set

```bash
docker compose run --rm replay --days 30
$env:CATALOG_FILE = "data/catalog_retailrocket.json"    # PowerShell
docker compose up -d --force-recreate api dashboard
```

Capture **within 30 minutes of the replay finishing**: `/trending` and the
graph both look back `TRENDING_LOOKBACK_MINUTES` / `PAIR_LOOKBACK_MINUTES`
(30 by default), and after that the page correctly reports that it is showing
retained history rather than live activity - honest, but not what you want in
a screenshot.

## Practicalities

- Capture at a window width of about 1400 px; the graph and its legend sit
  side by side and a narrow window stacks them.
- For the GIF: [ScreenToGif](https://www.screentogif.com/) on Windows. Keep it
  under 10 MB or GitHub will not preview it inline.
- Crop out browser chrome and anything personal in the tab bar.
