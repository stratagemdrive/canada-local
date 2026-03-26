"""
fetch_news.py
Fetches Canadian news headlines from RSS feeds, categorizes each story,
and maintains a rolling 7-day window of up to 20 stories per category.
Output: docs/canada_news.json
"""

import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

import feedparser
import requests
from dateutil import parser as dateparser

# ── Configuration ─────────────────────────────────────────────────────────────

OUTPUT_PATH = Path("docs/canada_news.json")
MAX_STORIES_PER_CATEGORY = 20
MAX_AGE_DAYS = 7

FEEDS = [
    {"source": "Castanet",              "url": "https://www.castanet.net/rss/bc.cfm"},
    {"source": "Castanet",              "url": "https://www.castanet.net/rss/canada.cfm"},
    {"source": "Pembroke Observer",     "url": "https://www.pembrokeobserver.com/category/news/local-news/feed"},
    {"source": "Burns Lake District News", "url": "https://www.burnslakelakesdistrictnews.com/feed"},
    {"source": "Vancouver Sun",         "url": "https://vancouversun.com/feed/atom"},
    {"source": "Toronto Sun",           "url": "https://torontosun.com/category/news/feed"},
    {"source": "Windsor Star",          "url": "https://windsorstar.com/feed"},
    {"source": "The StarPhoenix",       "url": "https://thestarphoenix.com/feed"},
    {"source": "YGK News",              "url": "https://ygknews.ca/feed"},
]

CATEGORIES = ["Diplomacy", "Military", "Energy", "Economy", "Local Events"]

# ── Keyword maps for categorisation ───────────────────────────────────────────

CATEGORY_KEYWORDS = {
    "Diplomacy": [
        r"\bdiplomat\w*\b", r"\bambassador\b", r"\btreaty\b", r"\bagreement\b",
        r"\bforeign (affairs|minister|policy|relations)\b", r"\bsanction\w*\b",
        r"\bnato\b", r"\bun\b", r"\bunited nations\b", r"\bconsulate\b",
        r"\bembassy\b", r"\btrade (deal|negotiation|agreement|talks)\b",
        r"\binternational (relations|summit|talks)\b", r"\bbilateral\b",
        r"\bmultilateral\b", r"\bG7\b", r"\bG20\b", r"\bimf\b",
        r"\bworld (bank|trade organization)\b", r"\bwto\b",
        r"\bconsul\w*\b", r"\bpeace (deal|talks|process|treaty)\b",
        r"\bprime minister.*meeting\b", r"\btrudeau.*(meeting|visit|trip)\b",
        r"\bcanada.*(us|usa|united states|china|russia|europe|eu|india)\b",
        r"\bcarney.*(visit|summit|meeting)\b",
    ],
    "Military": [
        r"\bmilitary\b", r"\bdefence\b", r"\bdefense\b", r"\bsoldier\w*\b",
        r"\btroops?\b", r"\bnavy\b", r"\barmy\b", r"\bair force\b",
        r"\brcaf\b", r"\brcn\b", r"\bcaf\b", r"\bcanadian armed forces\b",
        r"\bnorad\b", r"\bweapon\w*\b", r"\bwarship\b", r"\bfrigate\b",
        r"\bjet\w*\b", r"\bf-35\b", r"\bwarplane\b", r"\bdrone\b",
        r"\bwar\b", r"\bconflict\b", r"\bbattle\b", r"\bcombat\b",
        r"\bveteran\w*\b", r"\bpeacekeep\w*\b", r"\bdeployment\b",
        r"\bmission abroad\b", r"\bbase (closure|opening|expansion)\b",
        r"\brcmp.*terror\b", r"\bnational security\b", r"\bterror\w*\b",
        r"\bintelligence (agency|report|service)\b", r"\bcsis\b",
        r"\bexplosion\b", r"\bmunition\w*\b",
    ],
    "Energy": [
        r"\benergy\b", r"\boil\b", r"\bnatural gas\b", r"\bpipeline\b",
        r"\btrans mountain\b", r"\bkeystone\b", r"\bcoal\b", r"\blng\b",
        r"\brenewable\b", r"\bsolar\b", r"\bwind (power|energy|farm|turbine)\b",
        r"\bhydro\b", r"\bnuclear (plant|power|energy|reactor)\b",
        r"\belectricit\w*\b", r"\bpower (grid|plant|outage)\b",
        r"\bcarbon (tax|price|credit|emission)\b", r"\bclimate\b",
        r"\bgreenhouse gas\b", r"\bnet.zero\b", r"\bemission\w*\b",
        r"\bfuel\b", r"\bgasoline\b", r"\bgas price\b",
        r"\benbridge\b", r"\btc energy\b", r"\bsuncor\b", r"\bcnrl\b",
        r"\boilsand\w*\b", r"\btar sand\w*\b", r"\bfracking\b",
        r"\bgeothermal\b", r"\btidal energy\b", r"\bsmr\b",
    ],
    "Economy": [
        r"\beconom\w*\b", r"\bbudget\b", r"\bgdp\b", r"\binflation\b",
        r"\binterest rate\w*\b", r"\bbank of canada\b", r"\brecession\b",
        r"\btrade (war|tariff|deficit|surplus|balance)\b", r"\btariff\w*\b",
        r"\bjob\w*\b", r"\bunemployment\b", r"\blabour\b", r"\blabor\b",
        r"\bwage\w*\b", r"\bsalary\b", r"\bhousing (market|price|affordability|crisis)\b",
        r"\breal estate\b", r"\bmortgage\b", r"\bstock market\b",
        r"\btsx\b", r"\binvestment\b", r"\bstartup\b", r"\bmerger\b",
        r"\bbankruptcy\b", r"\bexport\w*\b", r"\bimport\w*\b",
        r"\bcost of living\b", r"\bfood (price|cost|bank|security)\b",
        r"\bfederal (budget|spending|deficit|debt)\b", r"\btax\w*\b",
        r"\bfiscal\b", r"\bmonetary policy\b", r"\bfreeland\b",
        r"\bfinance minister\b", r"\bcpib?\b", r"\bstatistics canada\b",
    ],
    "Local Events": [
        r"\bcommunity\b", r"\btown hall\b", r"\bfestival\b", r"\bparade\b",
        r"\bfire\b", r"\bflood\b", r"\baccident\b", r"\bcrash\b",
        r"\bcrime\b", r"\barrest\b", r"\bpolice\b", r"\bcourt\b",
        r"\bmunicip\w*\b", r"\bmayor\b", r"\bcouncil\b", r"\bcity hall\b",
        r"\bschool\b", r"\buniversity\b", r"\bcollege\b", r"\bhospital\b",
        r"\bhealth (care|unit|authority|region)\b", r"\bweather\b",
        r"\bstorm\b", r"\bblizzard\b", r"\bwildfire\b", r"\bdrought\b",
        r"\bfundraiser\b", r"\bcharity\b", r"\bvolunteer\b",
        r"\bsports (team|league)\b", r"\bculture\b", r"\barts\b",
        r"\bheritage\b", r"\bcelebration\b", r"\bholiday\b",
        r"\bobiturar\w*\b", r"\bdeath\b", r"\bfuneral\b",
        r"\binfrastructure\b", r"\broad (repair|closure|construction)\b",
        r"\btransit\b", r"\bbus\b", r"\btrain\b",
    ],
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_date(entry) -> datetime | None:
    """Extract a timezone-aware datetime from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                import calendar
                ts = calendar.timegm(t)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                pass
    for attr in ("published", "updated"):
        raw = getattr(entry, attr, None)
        if raw:
            try:
                dt = dateparser.parse(raw)
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
    return None


def score_category(text: str) -> str:
    """Return the best-matching category for the given text."""
    text_lower = text.lower()
    scores = {cat: 0 for cat in CATEGORIES}
    for cat, patterns in CATEGORY_KEYWORDS.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                scores[cat] += 1
    best = max(scores, key=scores.get)
    # If nothing matched at all, default to Local Events
    return best if scores[best] > 0 else "Local Events"


def fetch_feed(source: str, url: str) -> list[dict]:
    """Fetch one RSS/Atom feed and return a list of normalised story dicts."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; StratagemdrivBot/1.0; "
            "+https://stratagemdrive.github.io/canada-local/)"
        )
    }
    stories = []
    cutoff = now_utc() - timedelta(days=MAX_AGE_DAYS)

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as exc:
        print(f"[WARN] Could not fetch {url}: {exc}")
        return stories

    for entry in feed.entries:
        pub_dt = parse_date(entry)
        if pub_dt is None:
            continue
        if pub_dt < cutoff:
            continue

        title = (entry.get("title") or "").strip()
        link  = (entry.get("link")  or "").strip()
        if not title or not link:
            continue

        summary = entry.get("summary") or entry.get("description") or ""
        category = score_category(f"{title} {summary}")

        stories.append({
            "title":          title,
            "source":         source,
            "url":            link,
            "published_date": pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "category":       category,
        })

    return stories


def load_existing() -> dict[str, list[dict]]:
    """Load existing Canada_news.json, returning a dict keyed by category."""
    if OUTPUT_PATH.exists():
        try:
            with OUTPUT_PATH.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "stories" in data:
                by_cat: dict[str, list[dict]] = {c: [] for c in CATEGORIES}
                for story in data["stories"]:
                    cat = story.get("category")
                    if cat in by_cat:
                        by_cat[cat].append(story)
                return by_cat
        except Exception as exc:
            print(f"[WARN] Could not parse existing JSON: {exc}")
    return {c: [] for c in CATEGORIES}


def merge_stories(
    existing: dict[str, list[dict]],
    fresh: list[dict],
) -> dict[str, list[dict]]:
    """
    Merge fresh stories into existing pools per category.
    - Remove stories older than MAX_AGE_DAYS.
    - Deduplicate by URL.
    - Add new stories; if pool > MAX_STORIES_PER_CATEGORY, drop oldest first.
    """
    cutoff = now_utc() - timedelta(days=MAX_AGE_DAYS)

    # Prune old entries from existing
    for cat in CATEGORIES:
        existing[cat] = [
            s for s in existing[cat]
            if dateparser.parse(s["published_date"]).replace(tzinfo=timezone.utc) >= cutoff
        ]

    # Build a set of known URLs per category
    known_urls: dict[str, set[str]] = {
        cat: {s["url"] for s in existing[cat]} for cat in CATEGORIES
    }

    # Insert fresh stories
    for story in fresh:
        cat = story["category"]
        if story["url"] in known_urls.get(cat, set()):
            continue
        existing[cat].append(story)
        known_urls[cat].add(story["url"])

    # Sort by date descending and enforce cap
    for cat in CATEGORIES:
        existing[cat].sort(
            key=lambda s: s["published_date"],
            reverse=True,
        )
        existing[cat] = existing[cat][:MAX_STORIES_PER_CATEGORY]

    return existing


def write_output(by_cat: dict[str, list[dict]]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    all_stories = [s for stories in by_cat.values() for s in stories]
    payload = {
        "generated_at": now_utc().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "country":       "Canada",
        "total_stories": len(all_stories),
        "categories":    CATEGORIES,
        "stories":       all_stories,
    }
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Wrote {len(all_stories)} stories to {OUTPUT_PATH}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"[INFO] Starting fetch at {now_utc().isoformat()}")

    # Collect fresh stories from all feeds
    fresh_stories: list[dict] = []
    for feed_cfg in FEEDS:
        print(f"[INFO] Fetching {feed_cfg['source']} → {feed_cfg['url']}")
        stories = fetch_feed(feed_cfg["source"], feed_cfg["url"])
        print(f"       Found {len(stories)} recent stories")
        fresh_stories.extend(stories)

    print(f"[INFO] Total fresh stories collected: {len(fresh_stories)}")

    existing = load_existing()
    merged   = merge_stories(existing, fresh_stories)
    write_output(merged)


if __name__ == "__main__":
    main()
