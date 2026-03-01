"""
Parallel Steam Reviews Scraper for THE FINALS (appid 2073850).

Downloads ALL reviews by running parallel scrapers per language.
Saves incrementally to JSONL (one review per line) to avoid data loss.
Then merges and deduplicates into a single JSON file.

Usage:
    python3 scrape_all_reviews.py [--workers N] [--delay SECONDS] [--output FILE]
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

APP_ID = 2073850
API_URL = f"https://store.steampowered.com/appreviews/{APP_ID}"
BATCH_SIZE = 100

# All Steam language codes
LANGUAGES = [
    "english", "russian", "german", "french", "spanish",
    "brazilian", "turkish", "schinese", "polish", "koreana",
    "latam", "italian", "swedish", "tchinese", "ukrainian",
    "portuguese", "dutch", "danish", "finnish", "norwegian",
    "hungarian", "czech", "romanian", "thai", "japanese",
    "vietnamese", "arabic", "greek", "indonesian", "bulgarian",
]

print_lock = Lock()
progress = {}


def log(msg):
    with print_lock:
        print(msg, flush=True)


def fetch_batch(cursor="*", language="english", delay=0.3):
    """Fetch a single batch of reviews."""
    params = {
        "json": "1",
        "num_per_page": str(BATCH_SIZE),
        "cursor": cursor,
        "language": language,
        "filter": "recent",
        "review_type": "all",
        "purchase_type": "all",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

    time.sleep(delay)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def scrape_language(language, output_dir, delay=0.3):
    """Scrape all reviews for a single language. Save incrementally to JSONL."""
    filepath = os.path.join(output_dir, f"reviews_{language}.jsonl")
    cursor = "*"
    seen_ids = set()
    total = 0
    page = 0
    retries = 0

    # Resume: load already downloaded IDs
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    seen_ids.add(r["recommendationid"])
                except (json.JSONDecodeError, KeyError):
                    pass
        if seen_ids:
            log(f"  [{language}] Resuming: {len(seen_ids)} reviews already downloaded")
            total = len(seen_ids)

    with open(filepath, "a", encoding="utf-8") as f:
        while True:
            try:
                data = fetch_batch(cursor, language, delay)
            except Exception as e:
                retries += 1
                if retries > 5:
                    log(f"  [{language}] Too many errors, stopping. Last error: {e}")
                    break
                log(f"  [{language}] Error: {e}, retrying in 5s...")
                time.sleep(5)
                continue

            retries = 0
            reviews = data.get("reviews", [])
            if not reviews:
                break

            new_count = 0
            for r in reviews:
                rid = r["recommendationid"]
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    new_count += 1
                    total += 1

            cursor = data.get("cursor")
            page += 1

            if page % 50 == 0:
                log(f"  [{language}] Page {page}: {total} reviews collected")

            if new_count == 0:
                break

            if not cursor:
                break

    log(f"  [{language}] Done: {total} reviews in {page} pages")
    return language, total


def merge_jsonl_files(output_dir, output_file):
    """Merge all JSONL files into one deduplicated JSON file."""
    seen_ids = set()
    all_reviews = []

    for fname in sorted(os.listdir(output_dir)):
        if not fname.endswith(".jsonl"):
            continue
        filepath = os.path.join(output_dir, fname)
        count = 0
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line)
                    rid = r["recommendationid"]
                    if rid not in seen_ids:
                        seen_ids.add(rid)
                        all_reviews.append(r)
                        count += 1
                except (json.JSONDecodeError, KeyError):
                    pass
        print(f"  {fname}: {count} unique reviews")

    # Sort by timestamp
    all_reviews.sort(key=lambda r: r.get("timestamp_created", 0))

    data = {
        "app_id": APP_ID,
        "reviews_count": len(all_reviews),
        "reviews": all_reviews,
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"\nMerged: {len(all_reviews)} unique reviews -> {output_file}")
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    print(f"File size: {file_size:.1f} MB")
    return len(all_reviews)


def main():
    parser = argparse.ArgumentParser(description="Parallel Steam review scraper")
    parser.add_argument("--workers", type=int, default=10,
                        help="Number of parallel workers (default: 10)")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Delay between requests per worker in seconds (default: 0.3)")
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, "data")

    parser.add_argument("--output", default=os.path.join(data_dir, "reviews_all.json"),
                        help="Output JSON file (default: data/reviews_all.json)")

    args = parser.parse_args()

    output_dir = os.path.join(data_dir, "reviews_by_lang")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Scraping ALL reviews for THE FINALS (appid {APP_ID})")
    print(f"Workers: {args.workers}, Delay: {args.delay}s")
    print(f"Languages: {len(LANGUAGES)}")
    print(f"Output dir: {output_dir}/")
    print()

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(scrape_language, lang, output_dir, args.delay): lang
            for lang in LANGUAGES
        }

        results = {}
        for future in as_completed(futures):
            lang = futures[future]
            try:
                lang, count = future.result()
                results[lang] = count
            except Exception as e:
                log(f"  [{lang}] FAILED: {e}")
                results[lang] = 0

    elapsed = time.time() - start_time
    total = sum(results.values())

    print(f"\n{'='*60}")
    print(f"Scraping complete in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"Total reviews downloaded: {total}")
    print(f"\nTop languages:")
    for lang, count in sorted(results.items(), key=lambda x: -x[1])[:15]:
        print(f"  {lang}: {count}")

    print(f"\nMerging files...")
    merge_jsonl_files(output_dir, args.output)


if __name__ == "__main__":
    main()
