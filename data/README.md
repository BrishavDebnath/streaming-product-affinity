Downloaded data lives here.

- `raw/` holds the RetailRocket export, ~1.4 GB, git-ignored.
  Fetch it with `python scripts/fetch_dataset.py`. It needs a Kaggle key: in
  Settings, open the "API Tokens" tab and click "Create Legacy API Key", which downloads
  `kaggle.json` into `~/.kaggle/` (`%USERPROFILE%\.kaggle\` on Windows).
- `catalog_retailrocket.json` is written by `scripts/replay.py`. It labels the
  real items so the API and the dashboard show categories instead of bare ids.
  Point the stack at it with `CATALOG_FILE=data/catalog_retailrocket.json`.

Nothing in this folder is committed: the pipeline runs on generated traffic
without it, and the dataset is a download away for anyone who wants the
measured numbers reproduced.
