# Security

## What this project is

A portfolio data-engineering project, run locally with Docker Compose. It is
not deployed anywhere, holds no user data, and is not intended for production
use as it stands. The points below say what that means in practice, so nobody
has to read the source to find out.

## Known limitations, by design

- **No authentication** on the API, the dashboard, Prometheus or Grafana. Every
  service binds to localhost through Compose; publishing those ports to a
  network would expose them unauthenticated.
- **No transport encryption.** Kafka uses PLAINTEXT listeners and MongoDB runs
  without credentials, both inside the Compose network.
- **A single Kafka broker with no replication**, so there is no durability
  story beyond one machine.
- **The dashboard writes to Kafka.** The View and Add to cart buttons publish
  events. That is the demo's point, and it means anyone who can reach the
  dashboard can write to the topic.

These simplifications are on purpose, for a project meant to be read and run
in five minutes. The README's Known limitations lists them too.

## Reporting a vulnerability

If you find a real problem that is not one of the above, such as a way to
execute code from a crafted event, a dependency with a known CVE that the
pinned version has not picked up, or a secret committed by mistake, please
open a [security advisory](https://github.com/BrishavDebnath/streaming-product-affinity/security/advisories/new)
instead of a public issue, or email brishavdevnath@gmail.com.

Expect a reply within a week. This is a personal project, not a product with an
on-call rotation, and pretending otherwise would be the wrong kind of
paperwork.

## What runs automatically

- CodeQL scans the Python on every push and weekly
  (`.github/workflows/codeql.yml`).
- Dependabot opens weekly PRs for Python packages, both Docker base images
  and the GitHub Actions the workflows call (`.github/dependabot.yml`).
- Actions are pinned to commit SHAs, not to mutable tags, so a compromised
  tag cannot change what CI runs.
- Secret scanning and push protection are enabled on the repository (a
  GitHub feature for public repositories, not something in this tree).
