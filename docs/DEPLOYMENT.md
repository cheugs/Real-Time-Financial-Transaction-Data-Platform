# Putting the Dashboard Online (free)

The full pipeline (Kafka + Spark) needs about 4 GB of memory, which is more than free hosting
offers. So the online version works like this:

```
your laptop: full pipeline ──snapshot──▶ Neon (free Postgres) ◀──reads── Streamlit Community Cloud (free)
```

The online dashboard shows a **snapshot of a real pipeline run**. It runs in *demo mode*: a
banner says when the snapshot was taken, and "last hour" is counted back from that moment.

You need about 30 minutes. Everything is free, and no credit card is needed.

> **Passwords:** the Neon password only goes into your terminal (step 3) and into the Streamlit
> secrets screen (step 5). Never put it in a file in the repository.

---

## Step 1: Put the project on GitHub

The project is already on GitHub:
https://github.com/cheugs/Real-Time-Financial-Transaction-Data-Platform

`.env` (your local password) is in `.gitignore`, so it is not in the repository.
After every push, the **Actions** tab runs the CI workflow by itself.

## Step 2: Create a free database on Neon

1. Go to https://neon.tech and sign up (you can use your GitHub account).
2. Create a project. Name: `txn-monitor`. Postgres version: 16. Region: the one closest to you (for example *Europe (Frankfurt)*).
3. On the project page, click **Connect**. Keep the default database (`neondb`), and copy the **connection string**. It looks like this:

```
postgresql://neondb_owner:PASSWORD@ep-something-123456.eu-central-1.aws.neon.tech/neondb?sslmode=require
```

## Step 3: Copy a snapshot of your pipeline to Neon

Let your local stack run normally for at least a few hours first (`docker compose up -d`), so
there is enough data. Then, in PowerShell, in the project folder:

```powershell
$env:TARGET_DATABASE_URL = "PASTE-THE-NEON-CONNECTION-STRING-HERE"
```

```powershell
.venv\Scripts\python scripts\export_snapshot.py --hours 6
```

If your stack was stopped for a while (for example overnight), add `--from-latest`: the 6 hours
are then counted back from the newest stored event instead of from now.

The script creates the tables on Neon, copies the last 6 hours of every table, saves the
snapshot time, and runs dbt (models + 26 tests) on Neon. Six hours is about 110,000
transactions, which is far below Neon's free storage limit.

## Step 4 (recommended): A read-only user for the dashboard

The website only needs to **read** data. In Neon, open the **SQL Editor** and run this,
choosing your own password:

```sql
create role dashboard_reader with login password 'CHOOSE-A-STRONG-PASSWORD';
grant usage on schema public, analytics to dashboard_reader;
grant select on all tables in schema public, analytics to dashboard_reader;
-- dbt re-creates the analytics tables, so new tables must be readable too:
alter default privileges in schema analytics grant select on tables to dashboard_reader;
```

## Step 5: Deploy the dashboard on Streamlit Community Cloud

1. Go to https://share.streamlit.io and sign in with **GitHub**.
2. Click **Create app** → **Deploy a public app from GitHub**.
3. Fill in:
   - Repository: `cheugs/Real-Time-Financial-Transaction-Data-Platform`
   - Branch: `main`
   - Main file path: `dashboard/app.py`
   - App URL: choose a name, for example `txn-monitor-yourname`
4. Click **Advanced settings**:
   - Python version: **3.12**
   - **Secrets**: paste this, with the values from Neon (use `dashboard_reader` if you did step 4):

```toml
PG_HOST = "ep-something-123456.eu-central-1.aws.neon.tech"
PG_PORT = "5432"
PG_DB = "neondb"
PG_USER = "dashboard_reader"
PG_PASSWORD = "THE-PASSWORD"
PG_SSLMODE = "require"
DEMO_MODE = "1"
```

5. Click **Deploy**. After 2–3 minutes your dashboard is online at
   `https://txn-monitor-yourname.streamlit.app`.

## Step 6: Add the link to the README

At the top of `README.md`, under the title, add:

```markdown
**Live demo:** https://txn-monitor-yourname.streamlit.app
```

Then commit and push. Streamlit Cloud updates the app on every push by itself.

---

## Good to know

- **Sleeping app:** free Streamlit apps go to sleep after some days without visitors. A visitor
  then sees a "wake up" button, and the app is back after about a minute. Open the link yourself
  before you send it to a recruiter.
- **New snapshot:** run step 3 again at any time. The old snapshot is replaced.
- **How demo mode works:** `DEMO_MODE=1` makes every time filter count back from
  `demo_meta.snapshot_at` instead of `now()`, and moves the displayed times forward so the
  newest data shows as "now" (a banner says so). See `dashboard/queries.py`. All times are
  in UTC. The local Docker version does not set `DEMO_MODE`, so it stays live.
- **Something wrong?** In Streamlit Cloud, open your app → **Manage app** (bottom right) to see the logs.
