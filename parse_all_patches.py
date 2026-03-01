"""
Parse ALL patch notes from THE FINALS Wiki and extract balance changes via AI.
Outputs updated patch_notes.json with ~100 patches (not just 9 season-openers).

Usage:
  python3 parse_all_patches.py          # Full run: scrape + AI extract
  python3 parse_all_patches.py --cache   # Skip scraping, use cached pages
"""

import json
import os
import re
import sys
import time
from dotenv import load_dotenv
from openai import OpenAI
import requests
from bs4 import BeautifulSoup

load_dotenv()

# --- Config ---
API_KEY = os.environ["PPQ_API_KEY"]
BASE_URL = os.environ.get("PPQ_BASE_URL", "https://api.ppq.ai")
MODEL = "google/gemini-2.5-flash-lite"

WIKI_BASE = "https://www.thefinals.wiki"
PATCHNOTES_URL = f"{WIKI_BASE}/wiki/Patchnotes"
CACHE_FILE = "patch_pages_cache.json"
OUTPUT_FILE = "patch_notes.json"
PROGRESS_FILE = "patch_parse_progress.json"

MAX_RETRIES = 3
RETRY_DELAY = 5
DELAY_BETWEEN_CALLS = 1

# Season-opening patches we already have (preserve their new_content, bug_fixes, etc.)
SEASON_OPENERS = {
    "Season_1", "1.2.0",  # S1
    "2.0.0", "3.0.0", "4.0.0", "5.0.0",
    "6.0.0", "7.0.0", "8.0.0", "9.0.0",
}

# --- Step 1: Discover all patch URLs ---

def discover_patches():
    """Fetch the Patchnotes page and extract all Update URLs."""
    print("Fetching patch notes index...")
    resp = requests.get(PATCHNOTES_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    patches = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/wiki/Update:" not in href:
            continue
        version_id = href.split("Update:")[-1]
        if version_id in seen:
            continue
        seen.add(version_id)

        # Skip pre-release patches (Closed Beta, Open Beta)
        if any(x in version_id for x in ["CB1", "CB2", "Closed", "Open", "OB_"]):
            continue

        patches.append({
            "version_id": version_id,
            "url": WIKI_BASE + href,
            "title": a.get_text(strip=True),
        })

    print(f"  Found {len(patches)} patches (post-launch)")
    return patches


# --- Step 2: Fetch each patch page ---

def fetch_patch_pages(patches):
    """Fetch HTML for each patch and extract text content. Cache results."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"  Loaded cache with {len(cache)} pages")
    else:
        cache = {}

    for i, patch in enumerate(patches):
        vid = patch["version_id"]
        if vid in cache:
            continue

        print(f"  [{i+1}/{len(patches)}] Fetching {vid}...")
        try:
            resp = requests.get(patch["url"], timeout=30)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            # Extract main content area
            content = soup.find("div", class_="mw-parser-output")
            if not content:
                content = soup.find("main") or soup.find("article") or soup
            text = content.get_text(separator="\n", strip=True)

            # Extract date from page
            date = extract_date(soup, text)

            cache[vid] = {
                "text": text[:50000],  # cap at 50K chars
                "date": date,
                "url": patch["url"],
                "title": patch["title"],
            }
        except Exception as e:
            print(f"    ERROR: {e}")
            cache[vid] = {"text": "", "date": "", "url": patch["url"], "title": patch["title"]}

        # Save cache incrementally
        if (i + 1) % 10 == 0 or i == len(patches) - 1:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, ensure_ascii=False)

        time.sleep(0.5)  # polite crawling

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)
    print(f"  Cache saved: {len(cache)} pages")
    return cache


def extract_date(soup, text):
    """Try to extract patch date from the page."""
    # Look for date patterns in text
    date_patterns = [
        r"(\w+ \d{1,2},? \d{4})",  # "March 20, 2025"
        r"(\d{4}-\d{2}-\d{2})",     # "2025-03-20"
    ]
    for pattern in date_patterns:
        m = re.search(pattern, text[:500])
        if m:
            return parse_date(m.group(1))
    return ""


def parse_date(date_str):
    """Normalize date to YYYY-MM-DD."""
    import datetime
    for fmt in ["%Y-%m-%d", "%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"]:
        try:
            return datetime.datetime.strptime(date_str.strip().replace(",", ","), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


# --- Step 3: Extract balance changes via AI ---

SYSTEM_PROMPT = """You are a game balance analyst. Given a patch note page for THE FINALS, extract ALL weapon, gadget, and specialization balance changes.

For each change, output a JSON object with:
- "item": exact item name (e.g., "Sword", "CL-40", "Cloaking Device", "Healing Beam", "Defibrillator")
- "type": one of: weapon_buff, weapon_nerf, weapon_rework, gadget_buff, gadget_nerf, gadget_rework, specialization_buff, specialization_nerf, specialization_rework
- "details": specific changes with numbers (e.g., "Damage 110 -> 93; fire rate 250 -> 275 RPM")

Rules:
- Only include BALANCE changes (stat modifications, reworks). Skip bug fixes, UI changes, visual-only changes.
- If a change has both buffs and nerfs, classify as rework.
- Use exact weapon/gadget names from the game.
- Include specializations (Cloaking Device, Healing Beam, Grappling Hook, etc.) and gadgets (Defibrillator, C4, Dome Shield, etc.)
- If there are NO balance changes in this patch, return an empty array: []

Return ONLY a JSON array, no markdown, no explanation."""


def extract_balance_changes_ai(cache, progress=None):
    """Use AI to extract balance changes from each cached patch page."""
    client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

    if progress is None:
        progress = {}
    results = dict(progress)

    to_process = [vid for vid in cache if vid not in results and cache[vid].get("text")]
    print(f"  {len(to_process)} patches to process ({len(results)} already done)")

    for i, vid in enumerate(to_process):
        page = cache[vid]
        text = page["text"]

        # Skip very short pages (likely empty/redirect)
        if len(text) < 100:
            results[vid] = []
            continue

        print(f"  [{i+1}/{len(to_process)}] Extracting from {vid}...")

        for attempt in range(MAX_RETRIES):
            try:
                response = client.chat.completions.create(
                    model=MODEL,
                    temperature=0.1,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": f"Patch: {page.get('title', vid)}\n\n{text}"},
                    ],
                    max_tokens=2000,
                )
                raw = response.choices[0].message.content.strip()

                # Clean up response
                if raw.startswith("```"):
                    raw = re.sub(r"^```(?:json)?\s*", "", raw)
                    raw = re.sub(r"\s*```$", "", raw)

                changes = json.loads(raw)
                if not isinstance(changes, list):
                    changes = []

                results[vid] = changes
                print(f"    → {len(changes)} balance changes found")
                break

            except json.JSONDecodeError as e:
                print(f"    JSON error (attempt {attempt+1}): {e}")
                if attempt == MAX_RETRIES - 1:
                    results[vid] = []
            except Exception as e:
                print(f"    API error (attempt {attempt+1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    results[vid] = []

        # Save progress incrementally
        if (i + 1) % 5 == 0 or i == len(to_process) - 1:
            with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False)

        time.sleep(DELAY_BETWEEN_CALLS)

    print(f"  Extraction complete: {len(results)} patches processed")
    return results


# --- Step 4: Build patch_notes.json ---

def version_to_season(version_id):
    """Map version ID to season code. e.g., '4.6.0' → 'S4', 'Season_1' → 'S1'."""
    if version_id == "Season_1" or version_id == "1.2.0":
        return "S1"
    m = re.match(r"^(\d+)\.", version_id)
    if m:
        return f"S{m.group(1)}"
    return ""


def build_patch_notes(cache, ai_results):
    """Combine scraped data + AI results into final patch_notes.json format."""
    # Load existing data to preserve season-opener details
    existing = {}
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            old = json.load(f)
            for p in old.get("patches", old if isinstance(old, list) else []):
                # Key by version
                existing[p.get("version", "")] = p

    patches = []
    for vid, page in cache.items():
        if not page.get("text"):
            continue

        season = version_to_season(vid)
        if not season:
            continue

        # Use AI-extracted balance changes
        balance_changes = ai_results.get(vid, [])

        # Version display name
        if vid == "Season_1":
            version = "Season 1"
        else:
            version = vid

        # Check if this is a season opener we have existing data for
        existing_patch = existing.get(version, {})

        patch = {
            "version": version,
            "season": season,
            "date": page.get("date", ""),
            "url": page.get("url", ""),
            "balance_changes": balance_changes,
            "new_content": existing_patch.get("new_content", []),
            "bug_fixes": existing_patch.get("bug_fixes", []),
            "other_changes": existing_patch.get("other_changes", []),
        }
        patches.append(patch)

    # Sort by date, then version
    patches.sort(key=lambda p: (p["date"] or "9999", p["version"]))

    output = {"patches": patches}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Stats
    total_changes = sum(len(p["balance_changes"]) for p in patches)
    with_changes = sum(1 for p in patches if p["balance_changes"])
    print(f"\nFinal output: {len(patches)} patches, {total_changes} balance changes ({with_changes} patches with changes)")
    print(f"Saved to {OUTPUT_FILE}")

    return patches


# --- Main ---

def main():
    use_cache = "--cache" in sys.argv

    # Step 1: Discover patches
    patches = discover_patches()

    # Step 2: Fetch pages
    if use_cache and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"Using cached pages: {len(cache)}")
    else:
        cache = fetch_patch_pages(patches)

    # Step 3: AI extraction
    progress = {}
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, encoding="utf-8") as f:
            progress = json.load(f)
        print(f"Resuming from progress: {len(progress)} patches already done")

    ai_results = extract_balance_changes_ai(cache, progress)

    # Step 4: Build output
    build_patch_notes(cache, ai_results)


if __name__ == "__main__":
    main()
