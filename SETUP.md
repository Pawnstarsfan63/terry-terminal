# Terry's Ticker Terminal — Setup & Hosting

Goal: get a **single live URL** you can share, for **free**.

Good news: **Reddit is optional.** The app runs live on Stocktwits sentiment, Yahoo
prices, fundamentals, options flow, and news without it. Reddit only powers the extra
"mentions / crowd" column, and Reddit has made its API key process slow lately — so we
deploy now and you can add Reddit later if you want it.

Cost: **$0.** GitHub, Render's free tier, and the data sources used here are all free.
The only place cost could sneak in is choosing a paid Render instance — Step 3 tells you
exactly where to pick **Free**.

---

## Phase 0 — Get the files (1 min)
Download these three from the chat into one folder:
- `terrys_ticker_terminal.py`
- `terrys-ticker-terminal.html`
- `requirements.txt`

---

## Phase 1 — Put the files on GitHub (3 min, no command line)
1. Make a free account at **github.com**.
2. Top-right **+** → **New repository**. Name it `terry-terminal`, leave **Public**, **Create repository**.
3. On the next screen click **"uploading an existing file"**.
4. Drag in all three files, then **Commit changes**.

---

## Phase 2 — Deploy on Render (5 min)
1. Free account at **render.com** — sign in **with GitHub** (simplest).
2. **New +** → **Web Service** → pick your `terry-terminal` repo.
3. Settings:
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `gunicorn terrys_ticker_terminal:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120`
   - **Instance Type:** **Free**  ← the one setting that controls cost. Pick Free.
4. Environment variables: **none are required.** (Leave it empty to launch on Stocktwits +
   Yahoo + fundamentals + options + news.) Optional ones you can add now or later:
   | Key | Value | Enables |
   |---|---|---|
   | `FINNHUB_API_KEY` | free key from finnhub.io (**recommended**) | Cloud-reliable news + finance-aware sentiment + insider trades |
   | `FMP_API_KEY` | free key from financialmodelingprep.com | Adds congressional (politician) trades to the Insider panel |
   | `STOCKTWITS_TOKEN` | free Stocktwits token | higher Stocktwits rate limit (often still IP-blocked on cloud) |
   | `REDDIT_CLIENT_ID` | (see Phase 4) | Reddit mentions column |
   | `REDDIT_CLIENT_SECRET` | (see Phase 4) | Reddit mentions column |
   | `REDDIT_USER_AGENT` | `terry-terminal by u/yourname` | Reddit mentions column |

   > **Why Finnhub is the recommended key:** it authenticates by key, so unlike Reddit and
   > Stocktwits it is **not** blocked by Render's datacenter IP. One free Finnhub key restores
   > working news + sentiment + insider data on your shared URL. FINRA off-exchange short
   > volume needs **no key** and works automatically.
5. **Create Web Service** and wait for the build to go **Live** (a couple minutes).

---

## Phase 3 — Use & share (done)
At the top of the Render page is your URL, e.g. **`https://terry-terminal.onrender.com`**.
Open it: you should see the dashboard with a green **LIVE** dot and real data.
**That URL is what you share** — anyone who opens it gets the live app, no setup, no keys.

Things to expect:
- **First load after idle is slow** (~30–60s) — the free server sleeps when unused and wakes
  on the next visit. Fast after that.
- **Without Reddit keys**, the "Reddit" column shows 0 mentions and sentiment comes from
  Stocktwits. Everything else is fully live.
- **Rate limits are shared** across everyone on your link; fine for a few people.

---

## Phase 4 — (Optional) Add Reddit later
Reddit's key process can require their approval, so it's optional. If/when you want it:
1. **https://www.reddit.com/prefs/apps** → **create another app** → type **script**,
   redirect uri `http://localhost:8080`, check the CAPTCHA, **create app**.
   - If it won't create, use their **"submit a request"** form and select
     **"I'm a Developer"** + **"I want to register to use the Reddit API."** (Human-reviewed.)
2. Once created, copy the **client id** (under the app name) and **secret**.
3. In Render → your service → **Environment**, add `REDDIT_CLIENT_ID`,
   `REDDIT_CLIENT_SECRET`, and `REDDIT_USER_AGENT`, then redeploy. The Reddit column lights up.

---

## Run locally instead (just for you)
Needs Python 3.10+. In the folder with the files:
```bash
pip install -r requirements.txt
python terrys_ticker_terminal.py
```
Open **http://localhost:8000**. (Optional Reddit/FMP keys: set them as environment
variables before running, same names as the table above.)
