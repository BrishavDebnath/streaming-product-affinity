#!/usr/bin/env python3
"""
Capture the README's screenshots and demo GIF from the running dashboard.

    pip install playwright==1.48.0 pillow          # uses the installed Edge or Chrome
    python scripts/screenshots.py demo             # demo catalogue, producer running
    python scripts/screenshots.py real             # after a replay, real catalogue

`demo` writes dashboard.png, pipeline-health.png and affinity-graph.png. `real`
writes real-data-graph.png, related-learned.png and demo.gif, whose last
frames switch Related products to the learned model, so it needs a trained
model that fits the replay (see README, "Is it any good?").

Sections are cut out by their headings rather than by fixed pixel offsets,
so a change to the page's layout does not silently crop the wrong thing.
"""

import argparse
import io
import sys
import time
from pathlib import Path

from PIL import Image
from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"
WIDTH = 1400


def settle(page: Page, timeout: float = 90.0) -> None:
    """Wait until Streamlit has finished a run and the graph has drawn.

    The status widget is always on the page. While a run is in progress it
    shows "Running" or a "Stop" button, so those are what is waited out.
    """
    page.get_by_text("Pipeline health").first.wait_for(timeout=timeout * 1000)
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = page.locator('[data-testid="stStatusWidget"]')
        text = status.first.inner_text() if status.count() else ""
        running = "Running" in text or "Stop" in text
        spinner = page.get_by_text("Building the affinity graph").count()
        if not running and not spinner:
            break
        time.sleep(0.5)
    time.sleep(2)      # the graph iframe paints after Streamlit reports done


def fit_page(page: Page) -> None:
    """Make the viewport as tall as the content, so nothing scrolls.

    Streamlit scrolls an inner container, not the body, so a "full page"
    screenshot of the body is empty. With the whole page in the viewport,
    every heading's position is its position on the page.
    """
    for _ in range(6):             # the graph adds height after it loads
        height = page.evaluate("""() => Math.max(...[...document.querySelectorAll('*')]
            .map(e => e.scrollHeight))""")
        if page.viewport_size["height"] >= height + 40:
            return
        page.set_viewport_size({"width": WIDTH, "height": int(height) + 40})
        time.sleep(1.5)
        settle(page)


def heading_top(page: Page, text: str) -> float:
    """Viewport y of the subheader with exactly this text."""
    return page.evaluate("""(text) => {
        const h = [...document.querySelectorAll('h1,h2,h3')]
            .find(e => e.innerText.trim() === text);
        if (!h) throw new Error('no heading ' + text);
        return h.getBoundingClientRect().top;
    }""", text)


def capture(page: Page) -> Image.Image:
    """The whole (fitted) page as one image. Clipped screenshots of a
    Streamlit page hung intermittently; one full shot, cropped here, did not."""
    fit_page(page)
    return Image.open(io.BytesIO(page.screenshot(timeout=120_000))).convert("RGB")


def section(page: Page, shot: Image.Image, start: str, end: str | None,
            path: Path) -> None:
    """Crop from heading `start` down to heading `end` (or the page end)."""
    top = max(0, round(heading_top(page, start)) - 12)
    bottom = round(heading_top(page, end)) - 8 if end else content_bottom(page, shot)
    # The sidebar is a blank strip beside a section; leave it out.
    left = page.evaluate("""() => {
        const s = document.querySelector('[data-testid="stSidebar"]');
        return s ? Math.max(0, s.getBoundingClientRect().right) : 0;
    }""")
    shot.crop((round(left), top, WIDTH, bottom)).save(path, optimize=True)
    print("wrote", path.relative_to(ROOT))


def content_bottom(page: Page, shot: Image.Image) -> int:
    """Just below the last text on the page. The graph's iframe is taller than
    what it draws, which left a large blank area under the last section."""
    bottom = page.evaluate("""() => {
        const main = document.querySelector('[data-testid="stMainBlockContainer"]');
        const els = [...(main || document.body).querySelectorAll('p, h1, h2, h3, span')]
            .filter(e => e.innerText && e.innerText.trim());
        return Math.max(...els.map(e => e.getBoundingClientRect().bottom));
    }""")
    return min(shot.height, round(bottom) + 24)


def frame(page: Page, shot: Image.Image, from_heading: str | None = None,
          height: int = 900) -> Image.Image:
    """One GIF frame: a window-sized slice of the page from a heading down."""
    top = max(0, round(heading_top(page, from_heading)) - 20) if from_heading else 0
    image = shot.crop((0, top, WIDTH, min(shot.height, top + height)))
    return image.resize((900, round(900 * image.height / image.width)))


def choose_learned(page: Page) -> None:
    settle(page)
    page.locator('[data-testid="stRadio"] label').filter(has_text="Learned model").click()
    # Wait for the answer itself: the rerun starts a moment after the click,
    # so "not running" right after it says nothing.
    try:
        page.get_by_text("ordered by the learned model").first.wait_for(timeout=60_000)
    except Exception:              # noqa: BLE001 - reported by the caller
        checked = page.evaluate(
            "[...document.querySelectorAll('input[type=radio]')].map(i => i.checked)")
        print("radio state after the click:", checked, file=sys.stderr)
        return
    settle(page)
    fit_page(page)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("set", choices=["demo", "real"])
    parser.add_argument("--url", default="http://127.0.0.1:8501")
    parser.add_argument("--browser", default="msedge", help="msedge or chrome")
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(channel=args.browser, headless=True)
        page = browser.new_page(viewport={"width": WIDTH, "height": 900},
                                device_scale_factor=1)
        page.goto(args.url)
        settle(page)
        fit_page(page)

        if args.set == "demo":
            shot = capture(page)
            shot.save(args.out / "dashboard.png", optimize=True)
            print("wrote", (args.out / "dashboard.png").relative_to(ROOT))
            section(page, shot, "Pipeline health", "Events per minute",
                    args.out / "pipeline-health.png")
            section(page, shot, "Products viewed together", None,
                    args.out / "affinity-graph.png")
        else:
            shot = capture(page)
            section(page, shot, "Products viewed together", None,
                    args.out / "real-data-graph.png")
            frames = [frame(page, shot), frame(page, shot, "Events per minute"),
                      frame(page, shot, "Trending now")]
            choose_learned(page)
            if "ordered by the learned model" not in page.inner_text("body"):
                print("the learned model did not answer: is a usable model loaded?",
                      file=sys.stderr)
                return 1
            shot = capture(page)
            section(page, shot, "Trending now", "Products viewed together",
                    args.out / "related-learned.png")
            frames += [frame(page, shot, "Trending now"),
                       frame(page, shot, "Products viewed together")]
            durations = [1800, 1800, 1800, 3000, 2600]
            gif = args.out / "demo.gif"
            frames[0].save(gif, save_all=True, append_images=frames[1:],
                           duration=durations, loop=0, optimize=True)
            print("wrote", gif.relative_to(ROOT), f"({gif.stat().st_size / 1e6:.1f} MB)")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
