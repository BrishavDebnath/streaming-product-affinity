Downloaded data lives here.

- `raw/` — the RetailRocket export, ~1.4 GB, git-ignored.
  Fetch it with `python scripts/fetch_dataset.py`. It needs a Kaggle key:
  Settings -> "API Tokens" tab -> "Create Legacy API Key", which downloads
  `kaggle.json` into `~/.kaggle/` (`%USERPROFILE%\.kaggle\` on Windows).
- `catalog_retailrocket.json` — written by `scripts/replay.py`; labels the real
  items so the API and the dashboard show categories instead of bare ids.
  Point the stack at it with `CATALOG_FILE=data/catalog_retailrocket.json`.

Nothing in this folder is committed: the pipeline runs on generated traffic
without it, and the dataset is a download away for anyone who wants the
measured numbers reproduced.
