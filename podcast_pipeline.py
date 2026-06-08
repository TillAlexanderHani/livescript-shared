#!/usr/bin/env python3
"""
Financial podcast pipeline — Claude Code edition.

Sources macro/finance podcasts, transcribes them with Whisper, summarizes each
one with Claude (via the Claude Code CLI on a Pro/Max subscription — no paid
API), then emails a single daily DIGEST sorted by relevance to the user's
markets (EM Asia FX + LATAM).

Two run modes (set by RUN_MODE env, driven by the GitHub Actions cron):
  - collect : find new episodes, transcribe + summarize, store them as "pending".
              Sends nothing.
  - digest  : collect first (catch overnight episodes), then send ONE email with
              every pending summary and clear the pending queue.

State:
  - emailed_episodes.json : master dedup DB (episode_id -> metadata). Never lose
                            this; it remembers everything already processed.
  - pending_digest.json   : episodes summarized but not yet sent + last_digest_date.
"""
import os
import re
import json
import time
import shutil
import hashlib
import smtplib
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import feedparser
import whisper
import dateutil.parser

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Feeds
# --------------------------------------------------------------------------- #
RSS_FEEDS = {
    "Goldman Sachs The Markets": "https://feeds.megaphone.fm/GLD9322922848",
    "At Any Rate Podcast": "https://feed.podbean.com/atanyrate/feed.xml",
    "NatWest Currency Exchange": "https://feeds.buzzsprout.com/2109661.rss",
    "TMI TreasuryCast (HSBC Treasury Beyond Borders inside)": "https://treasurycast.libsyn.com/rss",
    "Under the Banyan Tree (HSBC Global Research)": "https://feeds.acast.com/public/shows/64db93c01796c400110e0ce3",
    "The Macro Brief (HSBC Global Research)": "https://feeds.acast.com/public/shows/6476e27317ed970011e62580",
    "Thoughts on the Market": "https://rss.art19.com/thoughts-on-the-market",
    "Global Data Pod": "https://feed.podbean.com/globaldatapod/feed.xml",
    "Goldman Sachs Exchanges": "https://feeds.megaphone.fm/GLD9218176758",
    "CNBC - The Exchange": "https://feeds.simplecast.com/tc4zxWgX",
    "BlackRock - The Bid": "https://rss.art19.com/the-bid",
    "Invest Like the Best": "https://feeds.libsyn.com/85372/rss",
    "The Emerging Markets Equities Podcast (Aberdeen)": "https://feeds.buzzsprout.com/1632829.rss",
    "Latin America Today (WLRN)": "https://www.wlrn.org/podcast/latin-america-report/rss.xml",
    "Horizontes de Latinoamérica (S&P Global)": "https://feeds.buzzsprout.com/2374938.rss",
    "The GlobalCapital Podcast": "https://feeds.buzzsprout.com/1811593.rss",
    "Standard Chartered Macro Bytes": "https://feeds.buzzsprout.com/1572577.rss",
    "Eurizon Podcast": "https://feeds.buzzsprout.com/2032072.rss",
    "Citi GPS: Global Perspectives & Solutions": "https://feeds.buzzsprout.com/2019492.rss",
    "Barings Streaming Income": "https://feeds.buzzsprout.com/796658.rss",
    "PGIM Fixed Income Podcast": "https://feeds.buzzsprout.com/1943383.rss",
    "Financial Times Behind the Money (Macro)": "https://rss.acast.com/behindthemoney",
    "INSEAD Emerging Markets Podcast": "https://feeds.buzzsprout.com/1833434.rss",
    "Asia Climate Finance Podcast": "https://feeds.buzzsprout.com/1951932.rss",
    "Latin America in Focus (AS/COA)": "https://feeds.buzzsprout.com/2360149.rss",
    "The LatinNews Podcast": "https://feeds.buzzsprout.com/2374942.rss",
    "Macro Trading Floor": "https://feeds.megaphone.fm/ALFINVESTMENTSTRATEGYBV2974145286",
    "Planet Money": "https://feeds.npr.org/510289/podcast.xml",
    "The Economics Show": "https://feeds.acast.com/public/shows/the-economics-show-with-soumaya-keynes",
    "Macro Voices": "https://feed.podbean.com/macrovoices/feed.xml",
    "Macro Hive Conversations": "https://macrohive.libsyn.com/rss",
    "McKeany-Flavell Hot Commodity Podcast": "https://feed.podbean.com/mckeanyflavell/feed.xml",
    "The HC Commodities Podcast": "https://feeds.simplecast.com/QXaWSc4o",
    # --- Added 2026-06: verified feeds focused on EM Asia FX + LATAM + macro ---
    "Odd Lots (Bloomberg)": "https://www.omnycontent.com/d/playlist/e73c998e-6e60-432f-8610-ae210140c5b1/8a94442e-5a74-4fa2-8b8d-ae27003a8d6b/982f5071-765c-403d-969d-ae27003a8d83/podcast.rss",
    "Bloomberg FICC Focus": "https://feeds.megaphone.fm/BLM4409767076",
    "Sinica Podcast (China)": "https://rss.art19.com/sinica",
    "Standard Chartered Money Insights": "https://rss.buzzsprout.com/1662247.rss",
    "Standard Chartered India - Money Insights": "https://rss.buzzsprout.com/1885786.rss",
    "Deutsche Bank Podzept": "https://www.dbresearch.com/podcast_en.xml",
    "Money Talks (The Economist)": "https://access.acast.com/rss/39fc4a99-8861-437d-81e2-684d13e48f92",
    "The Indicator (Planet Money)": "https://feeds.npr.org/510325/podcast.xml",
    "The Americas Quarterly Podcast": "https://rss.buzzsprout.com/2066030.rss",
    # --- Added 2026-06 (round 2): more EM Asia + global macro, RSS verified active ---
    "Nomura - The Week Ahead": "https://nomurapodcasts.libsyn.com/rss",
    "Capital Economics Weekly Briefing": "https://feeds.transistor.fm/capital-economics-weekly-briefing",
    "MERICS China Podcast": "https://rss.buzzsprout.com/1825833.rss",
    "The China-Global South Podcast": "https://rss.libsyn.com/shows/500733/destinations/4287628.xml",
    "Financial Times Unhedged": "https://feeds.acast.com/public/shows/6478a825654260001190a7cb",
    "Reuters Econ World": "https://feeds.megaphone.fm/reuterseconworld",
    "PIMCO Pod": "https://feeds.megaphone.fm/pimcopod",
    "BNP Paribas Markets 360": "https://feed.ausha.co/BNAN3TJw1POP",
    "Talking Markets with Franklin Templeton": "https://rss.libsyn.com/shows/107803/destinations/584964.xml",
    "IMF Podcasts": "https://rss.libsyn.com/shows/78322/destinations/355221.xml",
}

PROVIDER_MAPPINGS = {
    "Macro Hive Conversations": "Macro Hive",
    "Macro Trading Floor": "At Any Rate",
    "Planet Money": "NPR",
    "The Economics Show": "The Economist",
    "Macro Voices": "Macro Voices",
    "Thoughts on the Market": "Morgan Stanley",
    "Goldman Sachs Exchanges": "Goldman Sachs",
    "Goldman Sachs The Markets": "Goldman Sachs",
    "NatWest Currency Exchange": "NatWest",
    "At Any Rate Podcast": "J.P. Morgan",
    "Global Data Pod": "J.P. Morgan",
    "BlackRock - The Bid": "BlackRock",
    "Invest Like the Best": "Colossus",
    "The Emerging Markets Equities Podcast (Aberdeen)": "abrdn",
    "Latin America Today (WLRN)": "WLRN",
    "Horizontes de Latinoamérica (S&P Global)": "S&P Global",
    "The GlobalCapital Podcast": "GlobalCapital",
    "Standard Chartered Macro Bytes": "Standard Chartered",
    "Eurizon Podcast": "Eurizon",
    "Citi GPS: Global Perspectives & Solutions": "Citi",
    "Barings Streaming Income": "Barings",
    "PGIM Fixed Income Podcast": "PGIM",
    "Financial Times Behind the Money (Macro)": "Financial Times",
    "INSEAD Emerging Markets Podcast": "INSEAD",
    "Asia Climate Finance Podcast": "Asia Climate Finance",
    "Latin America in Focus (AS/COA)": "AS/COA",
    "The LatinNews Podcast": "LatinNews",
    "McKeany-Flavell Hot Commodity Podcast": "McKeany-Flavell",
    "The HC Commodities Podcast": "HC Group",
    "Odd Lots (Bloomberg)": "Bloomberg",
    "Bloomberg FICC Focus": "Bloomberg Intelligence",
    "Sinica Podcast (China)": "Kaiser Kuo",
    "Standard Chartered Money Insights": "Standard Chartered",
    "Standard Chartered India - Money Insights": "Standard Chartered",
    "Deutsche Bank Podzept": "Deutsche Bank Research",
    "Money Talks (The Economist)": "The Economist",
    "The Indicator (Planet Money)": "NPR",
    "The Americas Quarterly Podcast": "Americas Quarterly",
    "Nomura - The Week Ahead": "Nomura",
    "Capital Economics Weekly Briefing": "Capital Economics",
    "MERICS China Podcast": "MERICS",
    "The China-Global South Podcast": "China-Global South Project",
    "Financial Times Unhedged": "Financial Times",
    "Reuters Econ World": "Reuters",
    "PIMCO Pod": "PIMCO",
    "BNP Paribas Markets 360": "BNP Paribas",
    "Talking Markets with Franklin Templeton": "Franklin Templeton",
    "IMF Podcasts": "IMF",
}

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
DOWNLOAD_DIR = "downloads"
EMAILED_DB_FILE = "emailed_episodes.json"
PENDING_FILE = "pending_digest.json"

WHISPER_MODEL = os.getenv("WHISPER_MODEL", "base")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "")  # empty = Claude Code default (Sonnet)
RUN_MODE = os.getenv("RUN_MODE", "collect").strip().lower()
MAX_RECENT_DAYS = int(os.getenv("MAX_RECENT_DAYS", "5"))
EPISODES_PER_FEED = int(os.getenv("EPISODES_PER_FEED", "1"))
MAX_EPISODES_PER_RUN = int(os.getenv("MAX_EPISODES_PER_RUN", "0"))  # 0 = unlimited (quota guard)
MAX_TRANSCRIPT_CHARS = int(os.getenv("MAX_TRANSCRIPT_CHARS", "120000"))  # safety cap (~30k tok)
MIN_DIGEST_PRIORITY = int(os.getenv("MIN_DIGEST_PRIORITY", "2"))  # drop priority 1 (non-finance noise)
PARIS_TZ = timezone(timedelta(hours=2))  # Europe/Paris summer; display only

MAX_RETRIES = 3
RETRY_DELAY = 5


# --------------------------------------------------------------------------- #
# Persistence helpers
# --------------------------------------------------------------------------- #
def _atomic_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    shutil.move(tmp, path)


def load_emailed_db():
    if not os.path.exists(EMAILED_DB_FILE):
        return {"version": "4.0", "episodes": {}}
    try:
        with open(EMAILED_DB_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "episodes" not in data:
            data = {"version": "4.0", "episodes": {}}
        return data
    except Exception as e:
        logger.error(f"Could not load {EMAILED_DB_FILE}: {e}")
        return {"version": "4.0", "episodes": {}}


def load_pending():
    if not os.path.exists(PENDING_FILE):
        return {"episodes": [], "last_digest_date": None}
    try:
        with open(PENDING_FILE, encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("episodes", [])
        data.setdefault("last_digest_date", None)
        return data
    except Exception as e:
        logger.error(f"Could not load {PENDING_FILE}: {e}")
        return {"episodes": [], "last_digest_date": None}


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
class PodcastPipeline:
    def __init__(self):
        Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
        self.db = load_emailed_db()
        self.pending = load_pending()
        # processed = anything we already transcribed/summarized (url or title)
        self.processed_urls = set()
        self.processed_titles = set()
        for ep in self.db.get("episodes", {}).values():
            if isinstance(ep, dict):
                if ep.get("url"):
                    self.processed_urls.add(ep["url"])
                if ep.get("title"):
                    self.processed_titles.add(ep["title"])
        for ep in self.pending["episodes"]:
            self.processed_urls.add(ep.get("url"))
            self.processed_titles.add(ep.get("title"))
        logger.info(f"Dedup DB: {len(self.processed_urls)} URLs known | "
                    f"pending queue: {len(self.pending['episodes'])} episode(s)")

    # ----- dedup / feed parsing ------------------------------------------- #
    def is_processed(self, url, title=None):
        return url in self.processed_urls or (title and title in self.processed_titles)

    def is_recent(self, published, max_days=MAX_RECENT_DAYS):
        try:
            dt = dateutil.parser.parse(published)
            cutoff = datetime.now(dt.tzinfo) - timedelta(days=max_days)
            return dt > cutoff
        except Exception:
            return True

    def extract_provider(self, feed_data, feed_name):
        if feed_name in PROVIDER_MAPPINGS:
            return PROVIDER_MAPPINGS[feed_name]
        provider = None
        info = getattr(feed_data, "feed", None)
        if info:
            for field in ("publisher", "author", "managingEditor", "copyright"):
                if getattr(info, field, None):
                    provider = getattr(info, field)
                    break
        if provider:
            provider = re.sub(r"\s*\([^)]*\)", "", provider)
            provider = re.sub(r"@.*", "", provider).strip()
            if len(provider) > 50:
                provider = provider.split(",")[0].split("-")[0].strip()
        return provider

    @staticmethod
    def find_audio_url(entry):
        for enc in getattr(entry, "enclosures", []) or []:
            if "audio" in enc.get("type", ""):
                return enc.get("href") or enc.get("url")
        for link in getattr(entry, "links", []) or []:
            if "audio" in link.get("type", ""):
                return link.get("href")
        return None

    # ----- audio / transcription ------------------------------------------ #
    def download_audio(self, url, filename):
        headers = {"User-Agent": "Mozilla/5.0 (compatible; PodcastBot/1.0)",
                   "Accept": "audio/*,*/*;q=0.9"}
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Download attempt {attempt + 1}/{MAX_RETRIES}")
                r = requests.get(url, headers=headers, stream=True, timeout=300)
                r.raise_for_status()
                with open(filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                if os.path.getsize(filename) > 100_000:
                    logger.info(f"Downloaded {os.path.getsize(filename) // (1024 * 1024)} MB")
                    return True
            except Exception as e:
                logger.error(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        return False

    def transcribe(self, audio_file):
        for model_name in (WHISPER_MODEL, "tiny", "small"):
            try:
                logger.info(f"Transcribing with Whisper '{model_name}'...")
                model = whisper.load_model(model_name)
                text = model.transcribe(audio_file)["text"].strip()
                if len(text) > 50:
                    logger.info(f"Transcribed {len(text)} characters")
                    return text
            except Exception as e:
                logger.error(f"Transcription failed with {model_name}: {e}")
        return None

    # ----- Claude summary -------------------------------------------------- #
    def summarize(self, transcript, title, feed, provider):
        """Summarize via the Claude Code CLI (subscription auth). Returns dict."""
        transcript = transcript[:MAX_TRANSCRIPT_CHARS]
        prompt = self._build_prompt(transcript, title, feed, provider)

        for attempt in range(2):
            try:
                cmd = ["claude", "-p", prompt, "--output-format", "text"]
                if CLAUDE_MODEL:
                    cmd += ["--model", CLAUDE_MODEL]
                res = subprocess.run(cmd, capture_output=True, text=True,
                                     timeout=600, env=os.environ)
                if res.returncode != 0:
                    err = (res.stderr or "").strip() or (res.stdout or "").strip()
                    logger.error(f"claude CLI rc={res.returncode}: {err[:500]}")
                    time.sleep(RETRY_DELAY)
                    continue
                parsed = self._parse_claude_json(res.stdout)
                if parsed:
                    logger.info(f"Claude summary ok (priority {parsed.get('priority')})")
                    return parsed
                logger.warning("Could not parse Claude JSON, retrying...")
            except subprocess.TimeoutExpired:
                logger.error("claude CLI timed out")
            except Exception as e:
                logger.error(f"claude CLI error: {e}")
            time.sleep(RETRY_DELAY)
        return None

    @staticmethod
    def _build_prompt(transcript, title, feed, provider):
        prov = f" ({provider})" if provider else ""
        return f"""You are a senior markets/macro analyst writing a Bloomberg-style morning brief for an emerging-markets FX & rates trader. Score relevance generously against his world — cast a WIDE net so nothing market-relevant is missed.

His book:
- Core EM Asia FX/rates: India/INR, Indonesia/IDR, Philippines/PHP; also China/CNY/CNH, South Korea/KRW, Thailand/THB, Malaysia/MYR, Taiwan/TWD, Singapore/SGD, Vietnam/VND.
- Core LATAM FX/rates: Mexico/MXN, Brazil/BRL, Chile/CLP, Colombia/COP, Peru/PEN; also Argentina/ARS.
- Global drivers of EM: the Fed/FOMC, US dollar/DXY, US Treasury yields, ECB/EUR, BoE/GBP, BoJ/JPY, SNB/CHF, PBoC/China; oil & commodities (Brent, WTI, gold, copper, iron ore, gas, agri); global risk sentiment, inflation/CPI, growth/PMIs, trade & tariffs, capital flows, credit spreads, sovereign debt, carry, FX fixings and any central-bank policy anywhere.

Respond EXCLUSIVELY with a valid JSON object (no text before/after, no ``` fences), with EXACTLY these keys:

- "priority": integer 1-5:
  5 = directly about his core EM markets above (EM Asia or LATAM FX, rates, central banks, flows, or politics moving those currencies).
  4 = global macro with strong, direct transmission to EM FX/rates (Fed, USD/DXY, US Treasuries, China, oil/commodities, broad risk-on/off).
  3 = general markets/finance useful to any macro trader (DM FX EUR/GBP/JPY, equities/S&P 500, credit, sovereign debt, inflation prints, geopolitics moving markets).
  2 = tangential finance/economics with weak market transmission.
  1 = NOT about markets, macro, finance, economics or commodities at all (e.g. corporate culture, product launches, careers, sport, lifestyle). Use 1 ONLY for genuinely off-topic episodes.

- "tags": 4-10 short Bloomberg-style market tags that are ACTUALLY discussed. Use: currencies as ISO codes (USD, EUR, JPY, GBP, CNY, INR, IDR, PHP, KRW, THB, MYR, MXN, BRL, CLP, COP, PEN...) and pairs (USDINR, USDMXN); DXY; central banks (Fed, ECB, BoE, BoJ, PBoC, RBI, BI, BSP, BoK, Banxico, BCB, BCCh, BanRep, BCRP); rates (UST 10y, JGB, Bund); commodities (Brent, WTI, Gold, Copper, Iron Ore); indices (S&P 500); themes (Inflation, CPI, Carry, Risk sentiment, Trade, Tariffs, Credit, Fiscal, Elections). Empty list only if genuinely none.

- "summary": English markdown, dense and actionable (200-320 words), no bold outside the headers. Include ONLY sections that have real content — OMIT any section you cannot fill, and NEVER write filler such as "no implications can be inferred", "this episode does not discuss the Fed/dollar", or similar absence statements. Format:

**Summary**
[2-3 sentences: the core topic and why it matters to an EM FX/rates trader]

**Key points**
- [factual point with figures/context]
- [further points]

**FX / rates implications**
- [concrete read-through for currencies or rates, prioritising EM Asia/LATAM; if the episode is global, spell out the channel via USD/Fed/risk/commodities]
- [further implications]

**Watch list**
- [catalyst, data release, level or deadline to watch]

Keep tickers, currencies and proper nouns as-is. Never invent anything not in the transcript.

Podcast: {title} — {feed}{prov}

Transcript:
{transcript}"""

    @staticmethod
    def _parse_claude_json(text):
        text = (text or "").strip()
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
        candidates = []
        try:
            candidates.append(json.loads(text))
        except Exception:
            m = re.search(r"\{.*\}", text, re.DOTALL)
            if m:
                try:
                    candidates.append(json.loads(m.group(0)))
                except Exception:
                    pass
        for c in candidates:
            if isinstance(c, dict) and "summary" in c:
                try:
                    c["priority"] = int(c.get("priority", 3))
                except Exception:
                    c["priority"] = 3
                c["priority"] = max(1, min(5, c["priority"]))
                tags = c.get("tags")
                if not isinstance(tags, list):
                    tags = c.get("regions") if isinstance(c.get("regions"), list) else []
                c["tags"] = [str(t).strip() for t in tags if str(t).strip()]
                return c
        return None

    # ----- episode processing --------------------------------------------- #
    def process_episode(self, entry, audio_url, feed_name, provider):
        title = entry.title
        published = entry.get("published", "Unknown")
        logger.info(f"Processing: {title}")

        audio_file = os.path.join(DOWNLOAD_DIR, f"temp_{int(time.time() * 1000)}.mp3")
        try:
            if not self.download_audio(audio_url, audio_file):
                logger.error(f"Download failed: {title}")
                return False
            transcript = self.transcribe(audio_file)
            if not transcript:
                logger.error(f"Transcription failed: {title}")
                return False
            summary = self.summarize(transcript, title, feed_name, provider)
            if not summary:
                logger.error(f"Summary failed: {title}")
                return False

            episode = {
                "title": title,
                "published": published,
                "url": audio_url,
                "feed": feed_name,
                "provider": provider or "",
                "priority": summary["priority"],
                "tags": summary.get("tags", []),
                "summary": summary["summary"],
                "summarized_at": datetime.now(timezone.utc).isoformat(),
            }
            # Always record in the master DB so we never re-transcribe it...
            self._record_processed(episode)
            # ...but only queue finance/macro-relevant episodes for the digest.
            if episode["priority"] >= MIN_DIGEST_PRIORITY:
                self._queue_pending(episode)
                logger.info(f"QUEUED for digest (p{episode['priority']}): {title}")
            else:
                logger.info(f"Off-topic (p{episode['priority']}), recorded but not queued: {title}")
            return True
        except Exception as e:
            logger.error(f"Error processing {title}: {e}")
            return False
        finally:
            if os.path.exists(audio_file):
                os.remove(audio_file)

    def _record_processed(self, episode):
        """Mark in the master dedup DB so we never re-download/transcribe it."""
        episode_id = hashlib.sha256(episode["url"].encode()).hexdigest()[:16]
        self.db.setdefault("episodes", {})[episode_id] = {
            "url": episode["url"], "title": episode["title"], "feed": episode["feed"],
            "processed": episode["summarized_at"], "episode_id": episode_id,
        }
        self.db["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.db["version"] = "4.0"
        self.processed_urls.add(episode["url"])
        self.processed_titles.add(episode["title"])
        _atomic_write(EMAILED_DB_FILE, self.db)

    def _queue_pending(self, episode):
        """Add a relevant episode to the unsent digest queue."""
        self.pending["episodes"].append(episode)
        _atomic_write(PENDING_FILE, self.pending)

    def collect(self):
        feeds = list(RSS_FEEDS.items())
        total = len(feeds)
        new_count = 0
        attempts = 0
        for i, (feed_name, feed_url) in enumerate(feeds, 1):
            if MAX_EPISODES_PER_RUN and attempts >= MAX_EPISODES_PER_RUN:
                logger.info(f"Reached MAX_EPISODES_PER_RUN={MAX_EPISODES_PER_RUN} attempts, stopping collection")
                break
            logger.info(f"[{i}/{total}] Checking: {feed_name}")
            try:
                feed = feedparser.parse(feed_url)
                if not feed.entries:
                    logger.warning(f"No episodes for {feed_name}")
                    continue
                provider = self.extract_provider(feed, feed_name)
                for entry in feed.entries[:EPISODES_PER_FEED]:
                    if MAX_EPISODES_PER_RUN and attempts >= MAX_EPISODES_PER_RUN:
                        break
                    title = entry.title
                    if not self.is_recent(entry.get("published", "Unknown")):
                        logger.info(f"Skip old: {title}")
                        continue
                    audio_url = self.find_audio_url(entry)
                    if not audio_url:
                        logger.warning(f"No audio URL: {title}")
                        continue
                    if self.is_processed(audio_url, title):
                        logger.info(f"Already processed: {title}")
                        continue
                    logger.info(f"NEW: {title}")
                    attempts += 1
                    if self.process_episode(entry, audio_url, feed_name, provider):
                        new_count += 1
            except Exception as e:
                logger.error(f"Feed error {feed_name}: {e}")
            time.sleep(3)
        logger.info(f"Collect done: {new_count} new episode(s) queued")
        return new_count

    # ----- digest email --------------------------------------------------- #
    def send_digest(self):
        # Only send finance/macro-relevant episodes; drop priority-1 noise
        # (and clear any low-priority leftovers so they don't accumulate).
        episodes = [e for e in self.pending["episodes"]
                    if e.get("priority", 3) >= MIN_DIGEST_PRIORITY]
        if not episodes:
            logger.info("No pending episodes to send")
            if self.pending["episodes"]:
                self.pending["episodes"] = []
                _atomic_write(PENDING_FILE, self.pending)
            return True

        today = datetime.now(PARIS_TZ).strftime("%Y-%m-%d")
        if self.pending.get("last_digest_date") == today:
            logger.info(f"Digest already sent today ({today}) — skipping")
            return True

        episodes.sort(key=lambda e: (-e.get("priority", 0), e.get("title", "")))
        html, text = self._render_digest(episodes, today)
        subject = f"Morning Macro Brief — {datetime.now(PARIS_TZ).strftime('%a %d %b %Y')} ({len(episodes)})"

        if not self._send_email(subject, html, text):
            logger.error("Digest email failed — keeping pending queue for retry")
            return False

        self.pending["episodes"] = []
        self.pending["last_digest_date"] = today
        _atomic_write(PENDING_FILE, self.pending)
        logger.info(f"Digest sent with {len(episodes)} episode(s) and queue cleared")
        return True

    def _render_digest(self, episodes, today):
        cards_html, cards_text = [], []
        for ep in episodes:
            cards_html.append(self._episode_card_html(ep))
            cards_text.append(self._episode_card_text(ep))
        nice_date = datetime.now(PARIS_TZ).strftime("%A %d %B %Y")
        html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0"><style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;
line-height:1.6;color:#1f2937;max-width:820px;margin:0 auto;padding:16px;background:#f3f4f6;}}
.top{{background:linear-gradient(135deg,#1e3a8a 0%,#1e40af 100%);color:#fff;padding:20px;
border-radius:12px;margin-bottom:16px;text-align:center;}}
.top h1{{margin:0;font-size:18px;font-weight:700;}}
.top .sub{{margin-top:6px;font-size:13px;opacity:.9;}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:18px;margin-bottom:14px;
box-shadow:0 1px 4px rgba(0,0,0,.05);}}
.card h2{{margin:0 0 4px;font-size:15px;color:#111827;}}
.meta{{font-size:12px;color:#6b7280;margin-bottom:8px;}}
.meta .feed{{color:#1e40af;font-weight:600;}}
.tags{{margin:6px 0 10px;}}
.tag{{display:inline-block;background:#eff6ff;color:#1d4ed8;font-size:11px;font-weight:600;
padding:2px 8px;border-radius:999px;margin:0 4px 4px 0;}}
.prio{{display:inline-block;font-size:11px;font-weight:700;padding:2px 8px;border-radius:999px;
margin-bottom:8px;}}
.section-title{{color:#1e40af;font-size:13px;font-weight:700;margin:12px 0 4px;}}
.card p{{margin:0 0 8px;font-size:14px;}}
.card ul{{margin:0 0 8px;padding-left:0;list-style:none;}}
.card li{{position:relative;padding-left:16px;margin:3px 0;font-size:14px;line-height:1.5;}}
.card li:before{{content:"•";color:#2563eb;position:absolute;left:0;font-weight:700;}}
.listen a{{color:#0ea5e9;text-decoration:none;font-size:13px;font-weight:600;}}
.footer{{text-align:center;color:#9ca3af;font-size:11px;margin-top:14px;}}
</style></head><body>
<div class="top"><h1>Morning Macro Brief</h1>
<div class="sub">{nice_date} · {len(episodes)} episode(s)</div></div>
{''.join(cards_html)}
<div class="footer">Generated automatically · Summaries: Claude · Transcription: Whisper</div>
</body></html>"""
        text = (f"MORNING MACRO BRIEF — {nice_date} ({len(episodes)} episodes)\n"
                f"{'=' * 60}\n\n" + "\n\n".join(cards_text))
        return html, text

    @staticmethod
    def _prio_badge(p):
        colors = {5: ("#dc2626", "#fee2e2"), 4: ("#ea580c", "#ffedd5"),
                  3: ("#ca8a04", "#fef9c3"), 2: ("#6b7280", "#f3f4f6"),
                  1: ("#9ca3af", "#f9fafb")}
        fg, bg = colors.get(p, colors[3])
        return f'<span class="prio" style="color:{fg};background:{bg};">RELEVANCE {p}/5</span>'

    def _episode_card_html(self, ep):
        prov = f" ({ep['provider']})" if ep.get("provider") else ""
        date = self._fmt_date(ep.get("published", ""))
        tag_list = ep.get("tags") or ep.get("regions") or []
        tags = "".join(f'<span class="tag">{t}</span>' for t in tag_list[:10])
        listen = (f'<div class="listen"><a href="{ep["url"]}" target="_blank">▶ Listen to episode</a></div>'
                  if ep.get("url") else "")
        return f"""<div class="card">
{self._prio_badge(ep.get('priority', 3))}
<h2>{ep['title']}</h2>
<div class="meta"><span class="feed">{ep['feed']}{prov}</span> · {date}</div>
{f'<div class="tags">{tags}</div>' if tags else ''}
{self._summary_to_html(ep['summary'])}
{listen}
</div>"""

    def _episode_card_text(self, ep):
        prov = f" ({ep['provider']})" if ep.get("provider") else ""
        date = self._fmt_date(ep.get("published", ""))
        tags = ", ".join(ep.get("tags") or ep.get("regions") or [])
        head = (f"[{ep.get('priority', 3)}/5] {ep['title']}\n"
                f"{ep['feed']}{prov} · {date}\n")
        if tags:
            head += f"Tags: {tags}\n"
        body = self._summary_to_text(ep["summary"])
        link = f"\nListen: {ep['url']}" if ep.get("url") else ""
        return f"{head}{'-' * 50}\n{body}{link}"

    @staticmethod
    def _fmt_date(s):
        try:
            return dateutil.parser.parse(s).strftime("%a %d %b %Y")
        except Exception:
            return s or ""

    @staticmethod
    def _summary_to_html(summary):
        summary = re.sub(r"-{3,}", "", summary)
        parts = re.split(r"(\*\*[^*]+\*\*)", summary)
        out = ""
        for i, chunk in enumerate(parts):
            if i % 2 == 1:
                out += f'<div class="section-title">{chunk.strip("*").strip()}</div>'
            elif chunk.strip():
                lines = [l.strip() for l in chunk.strip().splitlines() if l.strip()]
                bullets = [l for l in lines if l.startswith(("-", "•"))]
                if bullets:
                    items = "".join(f"<li>{l.lstrip('-•').strip()}</li>" for l in bullets)
                    out += f"<ul>{items}</ul>"
                    paras = [l for l in lines if not l.startswith(("-", "•"))]
                    out += "".join(f"<p>{l}</p>" for l in paras)
                else:
                    out += "".join(f"<p>{l}</p>" for l in lines)
        return out

    @staticmethod
    def _summary_to_text(summary):
        summary = re.sub(r"-{3,}", "", summary)
        parts = re.split(r"(\*\*[^*]+\*\*)", summary)
        out = ""
        for i, chunk in enumerate(parts):
            if i % 2 == 1:
                title = chunk.strip("*").strip()
                out += f"\n{title}\n"
            elif chunk.strip():
                for l in chunk.strip().splitlines():
                    l = l.strip()
                    if l.startswith(("-", "•")):
                        out += f"  • {l.lstrip('-•').strip()}\n"
                    elif l:
                        out += f"{l}\n"
        return out.strip()

    # ----- email transport ------------------------------------------------ #
    @staticmethod
    def _send_email(subject, html, text):
        email_from = os.getenv("EMAIL_FROM")
        email_to = os.getenv("EMAIL_TO")
        password = os.getenv("EMAIL_PASSWORD")
        if not all([email_from, email_to, password]):
            logger.error("Email credentials not configured")
            return False
        try:
            msg = MIMEMultipart("alternative")
            msg["From"] = email_from
            msg["To"] = email_to
            msg["Subject"] = subject
            msg.attach(MIMEText(text, "plain", "utf-8"))
            msg.attach(MIMEText(html, "html", "utf-8"))
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(email_from, password)
                server.send_message(msg, email_from, email_to)
            logger.info(f"Email sent: {subject}")
            return True
        except Exception as e:
            logger.error(f"Email error: {repr(e)}")
            return False

    # ----- orchestration -------------------------------------------------- #
    def run(self):
        logger.info(f"Pipeline start — mode={RUN_MODE}, whisper={WHISPER_MODEL}, "
                    f"claude_model={CLAUDE_MODEL or 'default'}")
        start = datetime.now()
        # digest = send-only (fast): the overnight collect passes fill the queue,
        # so the morning email goes out immediately instead of transcribing first.
        if RUN_MODE in ("collect", "both"):
            self.collect()
        if RUN_MODE in ("digest", "both"):
            self.send_digest()
        logger.info(f"Pipeline done in {datetime.now() - start} | "
                    f"pending={len(self.pending['episodes'])}")


def main():
    try:
        PodcastPipeline().run()
    except Exception as e:
        logger.error(f"Critical error: {e}")


if __name__ == "__main__":
    main()
