"""
TERRY'S TICKER TERMINAL — free backend  (Reddit + Stocktwits + Yahoo/yfinance)
===================================================================
Routes consumed by terrys-ticker-terminal.html:
    GET /api/trending?subs=wallstreetbets,stocks,...
    GET /api/quotes?symbols=AAPL,TSLA,...
    GET /api/news?symbol=AAPL

All sources are FREE:
  - Reddit API : free "script" app (2 min setup)
  - Stocktwits : public stream API, no key (~200 req/hr unauth)
  - Yahoo      : yfinance — price, 50/200-day MA, ranges, market cap, NEWS
  - RSI(14)    : computed from a batched yfinance history download
  - Sentiment  : VADER (offline) for Reddit text & news headlines

SETUP
-----
1) pip install -r requirements.txt
2) Reddit app: https://www.reddit.com/prefs/apps -> "create another app" -> type "script"
   redirect uri http://localhost:8080 ; copy client id (under name) + secret.
3) Set env vars below, then: python ticker_pulse_server.py
4) In ticker-pulse.html set CONFIG.USE_MOCK = false  (API_BASE already = http://localhost:8000)
"""

import os, re, time, threading
from datetime import datetime, timezone

import numpy as np
import requests
import praw
import yfinance as yf
from flask import Flask, request, jsonify
from flask_cors import CORS
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

REDDIT_CLIENT_ID     = os.getenv("REDDIT_CLIENT_ID",     "PUT_CLIENT_ID_HERE")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "PUT_SECRET_HERE")
REDDIT_USER_AGENT    = os.getenv("REDDIT_USER_AGENT",    "terrys-ticker-terminal/1.0 by u/yourname")
STOCKTWITS_TOKEN     = os.getenv("STOCKTWITS_TOKEN", "")
FMP_API_KEY          = os.getenv("FMP_API_KEY", "")   # optional: congressional + insider disclosures (financialmodelingprep.com, free tier)
FINNHUB_API_KEY      = os.getenv("FINNHUB_API_KEY", "")  # optional: cloud-reliable news + insider transactions (finnhub.io, free tier)

POSTS_PER_SUB = 60
CACHE_TTL     = 300
NEWS_TTL      = 600
HISTORY_LEN   = 14
ST_LIMIT      = 20

app = Flask(__name__)
CORS(app)
vader = SentimentIntensityAnalyzer()
REDDIT_ENABLED = all(v and not v.startswith("PUT_") for v in (REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET))
reddit = None
if REDDIT_ENABLED:
    try:
        reddit = praw.Reddit(client_id=REDDIT_CLIENT_ID, client_secret=REDDIT_CLIENT_SECRET,
                             user_agent=REDDIT_USER_AGENT, check_for_async=False)
    except Exception as e:
        print("reddit init failed; running without Reddit:", e)
        reddit, REDDIT_ENABLED = None, False
else:
    print("No Reddit credentials set — running without the Reddit mentions source. "
          "Sentiment will be driven by Stocktwits. Add REDDIT_CLIENT_ID/SECRET later to enable it.")

# ---------------- ticker extraction ----------------
CASHTAG = re.compile(r"\$([A-Za-z]{1,5})\b")
BARE    = re.compile(r"\b([A-Z]{2,5})\b")
BLACKLIST = {"A","I","DD","CEO","CFO","IPO","ETF","USA","USD","EU","UK","FED","SEC","ATH",
    "YOLO","FOMO","IMO","IMHO","TLDR","EOD","AH","PM","EPS","PE","PT","WSB","OP","RH","ER",
    "FD","FUD","HODL","LOL","LMAO","WTF","OMG","TA","RSI","MACD","AI","API","GPU","CPU","EV",
    "ALL","FOR","ARE","THE","AND","NOT","YOU","BUT","CAN","GET","NEW","NOW","ONE","OUT","SEE",
    "TWO","WHY","BIG","BUY","RED","ITM","OTM","MA"}
def extract_symbols(text):
    if not text: return set()
    syms = {m.upper() for m in CASHTAG.findall(text)}
    for m in BARE.findall(text):
        if m not in BLACKLIST and m not in syms: syms.add(m)
    return syms

_lock = threading.Lock()
_cache, _news_cache, _history, _prev_mentions = {}, {}, {}, {}

def is_market_open():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5: return False
    h = now.hour + now.minute/60
    return 13.5 <= h <= 21.0

def _fi(fi, *keys):
    for k in keys:
        v = None
        try: v = fi[k] if k in fi else None
        except Exception: v = None
        if v is None:
            try: v = getattr(fi, k)
            except Exception: v = None
        if v is not None: return v
    return None

# ---------------- Yahoo quote (+ 50/200 MA, ranges, cap) ----------------
def price_for(symbols):
    out = {}
    if not symbols: return out
    try:
        tk = yf.Tickers(" ".join(symbols))
        for s in symbols:
            try:
                t = tk.tickers.get(s) or yf.Ticker(s)
                fi = t.fast_info
                last = _fi(fi,"lastPrice","last_price")
                prev = _fi(fi,"previousClose","previous_close")
                if last is None:
                    h = t.history(period="2d")
                    if not h.empty:
                        last = float(h["Close"].iloc[-1]); prev = float(h["Close"].iloc[0])
                if last is None: continue
                chg = ((last-prev)/prev*100) if prev else 0.0
                name = s
                try: name = (t.info.get("shortName") or s)[:32]
                except Exception: pass
                out[s] = {
                    "name": name, "price": round(float(last),2), "change_pct": round(float(chg),2),
                    "volume": int(_fi(fi,"lastVolume","last_volume") or 0),
                    "avg_volume": int(_fi(fi,"threeMonthAverageVolume","tenDayAverageVolume","three_month_average_volume") or 0),
                    "market_cap": int(_fi(fi,"marketCap","market_cap") or 0) or None,
                    "day_low": round(float(_fi(fi,"dayLow","day_low") or last),2),
                    "day_high": round(float(_fi(fi,"dayHigh","day_high") or last),2),
                    "w52_low": round(float(_fi(fi,"yearLow","year_low") or last),2),
                    "w52_high": round(float(_fi(fi,"yearHigh","year_high") or last),2),
                    "ma50": round(float(_fi(fi,"fiftyDayAverage","fifty_day_average") or last),2),
                    "ma200": round(float(_fi(fi,"twoHundredDayAverage","two_hundred_day_average") or last),2),
                }
            except Exception:
                continue
    except Exception as e:
        print("price error:", e)
    return out

# ---------------- RSI(14) from a batched history download ----------------
def compute_rsi(closes, period=14):
    closes = np.asarray(closes, dtype=float)
    closes = closes[~np.isnan(closes)]
    if len(closes) <= period: return None
    deltas = np.diff(closes)
    up = deltas[:period].clip(min=0).mean()
    down = -deltas[:period].clip(max=0).mean()
    def rsi_val(up, down):
        if down == 0: return 100.0 if up > 0 else 50.0
        return 100 - 100/(1 + up/down)
    rsi = rsi_val(up, down)
    for d in deltas[period:]:
        up = (up*(period-1) + max(d,0))/period
        down = (down*(period-1) + max(-d,0))/period
        rsi = rsi_val(up, down)
    return round(float(rsi),1)

def rsi_for(symbols):
    out = {}
    if not symbols: return out
    try:
        data = yf.download(symbols, period="3mo", interval="1d",
                           progress=False, group_by="ticker", threads=True)
        for s in symbols:
            try:
                closes = (data[s]["Close"] if len(symbols) > 1 else data["Close"]).dropna().values
                out[s] = compute_rsi(closes)
            except Exception:
                out[s] = None
    except Exception as e:
        print("rsi error:", e)
    return out

# ---------------- Stocktwits ----------------
def stocktwits_for(symbols):
    out = {}
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://stocktwits.com",
        "Referer": "https://stocktwits.com/",
    }
    params = {"access_token": STOCKTWITS_TOKEN} if STOCKTWITS_TOKEN else {}
    for s in symbols:
        try:
            r = requests.get(f"https://api.stocktwits.com/api/2/streams/symbol/{s}.json",
                             headers=headers, params=params, timeout=6)
            if r.status_code == 429:
                print("stocktwits throttled; stopping ST fetch this cycle"); break
            if r.status_code != 200: continue
            msgs = r.json().get("messages", [])
            bull = bear = 0
            for m in msgs:
                basic = ((m.get("entities") or {}).get("sentiment") or {}).get("basic")
                if basic == "Bullish": bull += 1
                elif basic == "Bearish": bear += 1
            tagged = bull + bear
            out[s] = {"st_messages": len(msgs), "st_bull": bull, "st_bear": bear,
                      "st_sentiment": round((bull-bear)/tagged, 2) if tagged else 0.0}
            time.sleep(0.15)
        except Exception:
            continue
    return out

# ---------------- News + finance-aware headline sentiment ----------------
# VADER reads everyday emotion; it misreads market language ("cuts guidance" = bad,
# "inks deal" = good). This overlay nudges scores using finance keywords.
FIN_BULL = {"beat","beats","surge","surges","soar","soars","soaring","rally","rallies","rallied",
    "upgrade","upgraded","raises","raised","record","jumps","gains","outperform","tops","wins",
    "deal","deals","approval","approved","breakout","bullish","optimistic","partnership","contract",
    "awarded","buyback","acquire","acquisition","expands","launches","wins"}
FIN_BEAR = {"miss","misses","cut","cuts","plunge","plunges","downgrade","downgraded","falls","slump",
    "slumps","warn","warns","warning","probe","lawsuit","sued","layoffs","recall","halts","bearish",
    "weak","loss","losses","fraud","investigation","selloff","bankruptcy","default","slashes",
    "disappoints","disappointing","plummets","tumbles","slides","sinks"}

def score_headline(text):
    """VADER baseline + a finance keyword overlay (whole-word matches)."""
    if not text: return 0.0
    base = vader.polarity_scores(text)["compound"]
    words = set(re.findall(r"[a-z']+", text.lower()))
    bump = 0.30 * len(words & FIN_BULL) - 0.30 * len(words & FIN_BEAR)
    return round(max(-1.0, min(1.0, base + bump)), 2)

def finnhub_news(symbol):
    """Company news via Finnhub (free tier, auth by key so it works from cloud). None if unavailable."""
    if not FINNHUB_API_KEY: return None
    try:
        from datetime import timedelta
        to = datetime.now(timezone.utc).date()
        frm = to - timedelta(days=7)
        r = requests.get("https://finnhub.io/api/v1/company-news",
                         params={"symbol": symbol, "from": str(frm), "to": str(to), "token": FINNHUB_API_KEY},
                         timeout=6)
        if r.status_code != 200: return None
        arts = []
        for n in (r.json() or []):
            title = n.get("headline")
            if not title: continue
            arts.append({"title": title, "publisher": n.get("source", ""), "url": n.get("url", ""),
                         "published": n.get("datetime"), "sentiment": score_headline(title)})
            if len(arts) >= 4: break
        return arts or None
    except Exception as e:
        print("finnhub news error:", e); return None

def yahoo_news(symbol):
    arts = []
    try:
        raw = yf.Ticker(symbol).news or []
        for n in raw[:6]:
            c = n.get("content") or n
            title = c.get("title") or n.get("title")
            if not title: continue
            publisher = (c.get("provider") or {}).get("displayName") or n.get("publisher") or ""
            url = ((c.get("canonicalUrl") or {}).get("url")
                   or (c.get("clickThroughUrl") or {}).get("url") or n.get("link") or "")
            published = c.get("pubDate") or n.get("providerPublishTime")
            arts.append({"title": title, "publisher": publisher, "url": url,
                         "published": published, "sentiment": score_headline(title)})
            if len(arts) >= 4: break
    except Exception as e:
        print("news error:", e)
    return arts

def news_for(symbol):
    now = time.time()
    with _lock:
        if symbol in _news_cache and now - _news_cache[symbol][0] < NEWS_TTL:
            return _news_cache[symbol][1]
    arts = finnhub_news(symbol) or yahoo_news(symbol)
    payload = {"symbol": symbol, "articles": arts}
    with _lock:
        _news_cache[symbol] = (now, payload)
    return payload

# ---------------- FINRA off-exchange short volume (free, no key) ----------------
FINRA_TTL = 3600
_finra_cache = {}
def finra_short_volume():
    """Latest FINRA daily consolidated short-volume % per symbol. Cached ~1h. {} on failure."""
    from datetime import timedelta
    now = time.time()
    with _lock:
        if "d" in _finra_cache and now - _finra_cache["d"][0] < FINRA_TTL:
            return _finra_cache["d"][1]
    data = {}
    for back in range(0, 6):                 # walk back to the most recent published trading day
        d = (datetime.now(timezone.utc).date() - timedelta(days=back)).strftime("%Y%m%d")
        try:
            r = requests.get(f"https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d}.txt", timeout=8)
            if r.status_code != 200: continue
            for line in r.text.splitlines()[1:]:
                p = line.split("|")
                if len(p) < 5: continue
                try:
                    sv, tv = float(p[2]), float(p[4])
                except Exception:
                    continue
                if tv > 0:
                    data[p[1].strip().upper()] = round(sv / tv * 100, 1)
            if data: break
        except Exception as e:
            print("finra error:", e); continue
    with _lock:
        _finra_cache["d"] = (now, data)
    return data

# ---------------- arbitrary/added ticker (full data on demand) ----------------
def reddit_counts_for(sym, subs):
    """Search each subreddit for the symbol; count true mentions + avg sentiment."""
    if not reddit:
        return 0, {}, 0.0
    by_sub, scores, total = {}, [], 0
    query = f"${sym} OR {sym}"
    for sub in subs:
        try:
            for post in reddit.subreddit(sub).search(query, sort="new", time_filter="month", limit=30):
                text = f"{post.title} {getattr(post,'selftext','') or ''}"
                if sym in extract_symbols(text):
                    by_sub[sub] = by_sub.get(sub, 0) + 1
                    total += 1
                    scores.append(vader.polarity_scores(text)["compound"])
        except Exception as e:
            print(f"search {sub} error:", e)
    return total, by_sub, (round(sum(scores)/len(scores), 2) if scores else 0.0)

def _pct(x):
    return round(x*100, 2) if isinstance(x, (int, float)) else None

def fundamentals_for(sym):
    """Valuation / quality / analyst metrics from yfinance .info (best-effort, free)."""
    f = {k: None for k in ("pe","fwd_pe","peg","ps","pb","eps","profit_margin","roe",
                            "rev_growth","div_yield","beta","target_price","analysts",
                            "recommendation","next_earnings","short_pct")}
    try:
        t = yf.Ticker(sym)
        info = t.info or {}
        f["pe"]            = info.get("trailingPE")
        f["fwd_pe"]        = info.get("forwardPE")
        f["peg"]           = info.get("trailingPegRatio") or info.get("pegRatio")
        f["ps"]            = info.get("priceToSalesTrailing12Months")
        f["pb"]            = info.get("priceToBook")
        f["eps"]           = info.get("trailingEps")
        f["profit_margin"] = _pct(info.get("profitMargins"))
        f["roe"]           = _pct(info.get("returnOnEquity"))
        f["rev_growth"]    = _pct(info.get("revenueGrowth"))
        dy = info.get("dividendYield")
        f["div_yield"]     = round(dy*100, 2) if isinstance(dy,(int,float)) and dy < 1 else (round(dy,2) if dy else None)
        f["beta"]          = info.get("beta")
        f["target_price"]  = info.get("targetMeanPrice")
        f["analysts"]      = info.get("numberOfAnalystOpinions")
        f["recommendation"]= info.get("recommendationKey")
        f["short_pct"]     = _pct(info.get("shortPercentOfFloat"))
        try:
            cal = t.calendar
            ed = None
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date")
                if isinstance(ed, list) and ed: ed = ed[0]
            elif cal is not None and "Earnings Date" in getattr(cal, "index", []):
                ed = cal.loc["Earnings Date"][0]
            if ed is not None:
                f["next_earnings"] = str(ed)[:10]
        except Exception:
            pass
    except Exception as e:
        print("fundamentals error:", e)
    return f

def options_flow_for(sym):
    """Observable options footprint from yfinance chains (free, no key)."""
    out = {"pc_ratio": None, "opt_volume": None, "iv": None, "flow_signal": 0}
    try:
        t = yf.Ticker(sym); exps = t.options or []
        if not exps: return out
        call_v = put_v = 0; ivs = []
        for e in exps[:2]:                      # nearest two expiries keeps it light
            ch = t.option_chain(e)
            call_v += int(ch.calls["volume"].fillna(0).sum())
            put_v  += int(ch.puts["volume"].fillna(0).sum())
            ivs += [float(x) for x in ch.calls["impliedVolatility"].dropna().values]
        pc = round(put_v/call_v, 2) if call_v else None
        iv = round(float(np.median(ivs))*100, 1) if ivs else None
        sig = 1 if (pc is not None and pc < 0.7) else (-1 if (pc is not None and pc > 1.3) else 0)
        out = {"pc_ratio": pc, "opt_volume": int(call_v+put_v), "iv": iv, "flow_signal": sig}
    except Exception as e:
        print("options error:", e)
    return out

def finnhub_insiders(sym):
    """Insider (Form 4) transactions via Finnhub free tier. Used when no FMP key is set."""
    if not FINNHUB_API_KEY: return []
    rows = []
    try:
        from datetime import timedelta
        to = datetime.now(timezone.utc).date()
        frm = to - timedelta(days=180)        # explicit window so recent filings reliably return
        r = requests.get("https://finnhub.io/api/v1/stock/insider-transactions",
                         params={"symbol": sym, "from": str(frm), "to": str(to), "token": FINNHUB_API_KEY},
                         timeout=6)
        if r.status_code != 200:
            print("finnhub insider status", r.status_code, "for", sym); return []
        for d in (r.json().get("data") or []):
            code = str(d.get("transactionCode", "")).upper()
            buy = code in ("P", "A")          # P = purchase, A = grant/acquire
            shares = d.get("change") or d.get("share") or 0
            try:
                amt = f"{abs(int(shares)):,} sh"
            except Exception:
                amt = str(shares)
            rows.append({"who": d.get("name") or "Insider", "role": "Insider",
                         "type": "BUY" if buy else "SELL", "amount": amt,
                         "date": str(d.get("transactionDate") or "")[:10]})
    except Exception as e:
        print("finnhub insider error:", e)
    rows.sort(key=lambda x: x.get("date") or "", reverse=True)
    return rows[:5]

def disclosures_for(sym):
    """Politician (STOCK Act) + insider (Form 4) recent trades. Prefers FMP (adds congressional);
    falls back to Finnhub insider data if only a Finnhub key is set. [] if neither key is present.
    NOTE: FMP has shifted paths across v3/v4/stable — confirm the current endpoints in their docs."""
    if not FMP_API_KEY:
        return finnhub_insiders(sym)
    rows = []
    base = "https://financialmodelingprep.com/api"
    endpoints = [
        (f"{base}/v4/senate-trading?symbol={sym}&apikey={FMP_API_KEY}", "Senator"),
        (f"{base}/v4/senate-disclosure?symbol={sym}&apikey={FMP_API_KEY}", "House Rep"),
        (f"{base}/v4/insider-trading?symbol={sym}&page=0&apikey={FMP_API_KEY}", "Insider"),
    ]
    for url, kind in endpoints:
        try:
            r = requests.get(url, timeout=6)
            if r.status_code != 200:
                continue
            for d in (r.json() or [])[:4]:
                ttype = str(d.get("type") or d.get("transactionType") or "").upper()
                buy = ("P" == ttype) or ("BUY" in ttype) or ("PURCHASE" in ttype)
                who = d.get("representative") or d.get("reportingName") or d.get("office") or kind
                amount = d.get("amount") or d.get("securitiesTransacted") or ""
                date = str(d.get("transactionDate") or d.get("date") or d.get("dateRecieved") or "")[:10]
                rows.append({"who": str(who), "role": kind, "type": "BUY" if buy else "SELL",
                             "amount": str(amount), "date": date})
        except Exception:
            continue
    rows.sort(key=lambda x: x.get("date") or "", reverse=True)
    return rows[:5]

# ---------------- Macro: indices + general market news (free) ----------------
INDEX_MAP = [("^GSPC","S&P 500"),("^IXIC","Nasdaq"),("^DJI","Dow Jones"),
             ("^RUT","Russell 2000"),("^VIX","VIX"),("^TNX","10Y Yield")]
MACRO_TTL = 120
_macro_cache = {}

def indices_for():
    out = []
    for sym, label in INDEX_MAP:
        try:
            t = yf.Ticker(sym); fi = t.fast_info
            last = _fi(fi,"lastPrice","last_price"); prev = _fi(fi,"previousClose","previous_close")
            if last is None:
                h = t.history(period="2d")
                if not h.empty:
                    last = float(h["Close"].iloc[-1]); prev = float(h["Close"].iloc[0])
            if last is None: continue
            chg = ((last-prev)/prev*100) if prev else 0.0
            out.append({"symbol": sym, "name": label, "price": round(float(last),2),
                        "change_pct": round(float(chg),2)})
        except Exception as e:
            print("index error", sym, e)
    return out

def macro_news():
    if not FINNHUB_API_KEY: return []
    try:
        r = requests.get("https://finnhub.io/api/v1/news",
                         params={"category":"general","token":FINNHUB_API_KEY}, timeout=6)
        if r.status_code != 200: return []
        arts = []
        for n in (r.json() or []):
            title = n.get("headline")
            if not title: continue
            arts.append({"title": title, "publisher": n.get("source",""), "url": n.get("url",""),
                         "published": n.get("datetime"), "sentiment": score_headline(title)})
            if len(arts) >= 6: break
        return arts
    except Exception as e:
        print("macro news error:", e); return []

def build_macro():
    now = time.time()
    with _lock:
        if "m" in _macro_cache and now - _macro_cache["m"][0] < MACRO_TTL:
            return _macro_cache["m"][1]
    payload = {"indices": indices_for(), "news": macro_news(),
               "market_open": is_market_open(), "updated": datetime.now(timezone.utc).isoformat()}
    with _lock:
        _macro_cache["m"] = (now, payload)
    return payload

def _clampf(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, x))

def blend_composite(news_sent, pc_ratio, short_vol_pct):
    """Consolidated sentiment from live sources: news 45% · options put/call 35% · short volume 20%."""
    sigs = []
    if news_sent is not None:
        sigs.append((_clampf(news_sent), 0.45))
    if pc_ratio:
        sigs.append((_clampf((1.0 - pc_ratio) / 0.6), 0.35))
    if short_vol_pct is not None:
        sigs.append((_clampf((45.0 - short_vol_pct) / 15.0), 0.20))
    if not sigs:
        return 0.0
    wsum = sum(w for _, w in sigs)
    return round(sum(v * w for v, w in sigs) / wsum, 2)

def build_one(sym, subs):
    """Full ticker object (identical shape to trending rows) for any symbol."""
    key = f"ticker:{sym}:" + ",".join(sorted(subs)); now = time.time()
    with _lock:
        if key in _cache and now - _cache[key][0] < CACHE_TTL:
            return _cache[key][1]
    q = price_for([sym])
    row = None
    if sym in q:
        mentions, by_sub, r_sent = 0, {}, 0.0          # Reddit removed: API non-functional on cloud
        stx = {"st_messages": 0, "st_bull": 0, "st_bear": 0, "st_sentiment": 0.0}  # Stocktwits removed: IP-blocked on cloud
        rsi_v = rsi_for([sym]).get(sym)
        with _lock:
            hist = _history.setdefault(sym, []); hist.append(mentions); del hist[:-HISTORY_LEN]; hist = list(hist)
            prevm = _prev_mentions.get(sym, mentions); _prev_mentions[sym] = mentions
        rw, sw = mentions, stx["st_messages"]
        composite = round((r_sent*rw + stx["st_sentiment"]*sw)/(rw+sw), 2) if (rw+sw) else r_sent
        row = {"symbol": sym, "name": q[sym]["name"], "mentions": mentions, "mentions_prev": prevm,
               "sentiment": r_sent, "by_sub": by_sub, "history": hist, "composite": composite, "rsi": rsi_v}
        row.update(stx)
        row.update({k: q[sym][k] for k in ("price","change_pct","volume","avg_volume",
                    "market_cap","day_low","day_high","w52_low","w52_high","ma50","ma200")})
        row.update(fundamentals_for(sym))
        row.update(options_flow_for(sym))
        row["short_vol_pct"] = finra_short_volume().get(sym)
        row["disclosures"] = disclosures_for(sym)
        # consolidated sentiment across all working sources
        narts = news_for(sym).get("articles", [])
        news_sent = round(sum(a.get("sentiment", 0) for a in narts) / len(narts), 2) if narts else None
        row["news_sent"] = news_sent
        row["composite"] = blend_composite(news_sent, row.get("pc_ratio"), row.get("short_vol_pct"))
    with _lock:
        _cache[key] = (now, row)
    return row

# ---------------- core ----------------
def build_trending(subs):
    agg = {}
    for sub in (subs if reddit else []):
        try:
            for post in reddit.subreddit(sub).hot(limit=POSTS_PER_SUB):
                text = f"{post.title} {getattr(post,'selftext','') or ''}"
                syms = extract_symbols(text)
                if not syms: continue
                comp = vader.polarity_scores(text)["compound"]
                for s in syms:
                    d = agg.setdefault(s, {"by_sub": {}, "scores": [], "mentions": 0})
                    d["by_sub"][sub] = d["by_sub"].get(sub,0)+1
                    d["mentions"] += 1
                    d["scores"].append(comp)
        except Exception as e:
            print(f"subreddit {sub} error:", e)

    candidates = sorted([s for s,d in agg.items() if d["mentions"] >= 2],
                        key=lambda s: agg[s]["mentions"], reverse=True)[:40]
    quotes = price_for(candidates)
    real = [s for s in candidates if s in quotes]
    st = {}
    rsi = rsi_for(real)

    tickers = []
    with _lock:
        for s in real:
            d = agg[s]
            hist = _history.setdefault(s, []); hist.append(d["mentions"]); del hist[:-HISTORY_LEN]
            r_sent = round(sum(d["scores"])/len(d["scores"]),2) if d["scores"] else 0.0
            stx = st.get(s, {"st_messages":0,"st_bull":0,"st_bear":0,"st_sentiment":0.0})
            rw, sw = d["mentions"], stx["st_messages"]
            composite = round((r_sent*rw + stx["st_sentiment"]*sw)/(rw+sw),2) if (rw+sw) else r_sent
            row = {"symbol": s, "name": quotes[s]["name"], "mentions": d["mentions"],
                   "mentions_prev": _prev_mentions.get(s, d["mentions"]), "sentiment": r_sent,
                   "by_sub": d["by_sub"], "history": list(hist), "composite": composite, "rsi": rsi.get(s)}
            row.update(stx)
            row.update({k: quotes[s][k] for k in ("price","change_pct","volume","avg_volume",
                        "market_cap","day_low","day_high","w52_low","w52_high","ma50","ma200")})
            tickers.append(row)
            _prev_mentions[s] = d["mentions"]

    tickers.sort(key=lambda t: t["mentions"], reverse=True)
    return {"updated": datetime.now(timezone.utc).isoformat(),
            "market_open": is_market_open(), "tickers": tickers}

# ---------------- routes ----------------
@app.route("/api/trending")
def trending():
    subs = [s.strip() for s in request.args.get("subs","wallstreetbets,stocks").split(",") if s.strip()]
    key = "trending:" + ",".join(sorted(subs)); now = time.time()
    with _lock:
        if key in _cache and now - _cache[key][0] < CACHE_TTL:
            return jsonify(_cache[key][1])
    payload = build_trending(subs)
    with _lock: _cache[key] = (now, payload)
    return jsonify(payload)

@app.route("/api/macro")
def macro_route():
    return jsonify(build_macro())

@app.route("/api/quotes")
def quotes():
    syms = [s.strip().upper() for s in request.args.get("symbols","").split(",") if s.strip()]
    q = price_for(syms); rsi = rsi_for(syms)
    for s in syms:
        if s in q:
            q[s]["rsi"] = rsi.get(s)
    return jsonify({"symbols": q})

@app.route("/api/news")
def news():
    sym = request.args.get("symbol","").strip().upper()
    if not sym: return jsonify({"symbol":"", "articles":[]})
    return jsonify(news_for(sym))

@app.route("/api/tickers")
def tickers_route():
    syms = [s.strip().upper() for s in request.args.get("symbols","").split(",") if s.strip()]
    subs = [s.strip() for s in request.args.get("subs","wallstreetbets,stocks").split(",") if s.strip()]
    rows = [build_one(s, subs) for s in syms]
    return jsonify({"tickers": [r for r in rows if r]})

HTML_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "terrys-ticker-terminal.html")

@app.route("/")
def home():
    """Serve the dashboard itself, flipped to live same-origin mode, so the whole app
    is reachable at ONE url. Opening the .html file directly still runs in mock mode."""
    try:
        with open(HTML_FILE, encoding="utf-8") as f:
            html = f.read()
        html = (html.replace("USE_MOCK: true", "USE_MOCK: false")
                    .replace("API_BASE: 'http://localhost:8000'", "API_BASE: ''"))
        return html, 200, {"Content-Type": "text/html; charset=utf-8"}
    except Exception:
        return jsonify({"ok": True, "note": "dashboard html not found next to server; api still live"})

@app.route("/health")
def health():
    sources = ["yfinance(price/MA/RSI/options/indices)", "finra(short-vol)"]
    sources.append("finnhub(news/insider/macro)" if FINNHUB_API_KEY else "finnhub:off")
    sources.append("fmp(congress/insider)" if FMP_API_KEY else "fmp:off")
    return jsonify({"ok": True, "service": "terrys-ticker-terminal", "sources": sources,
                    "routes": ["/api/tickers","/api/news","/api/macro","/api/quotes"]})

if __name__ == "__main__":
    print("TERRY'S TICKER TERMINAL backend -> http://localhost:8000  (set CONFIG.USE_MOCK=false in the HTML)")
    app.run(host="0.0.0.0", port=8000, debug=False)
