"""
Steam Reviews Scraper for THE FINALS (appid 2073850).

Uses the public Steam Store API to fetch reviews in JSON format.
Saves results to a CSV file.

Usage:
    python scrape_reviews.py [--limit N] [--language LANG] [--filter FILTER] [--output FILE]

Examples:
    python scrape_reviews.py                          # all reviews, default output
    python scrape_reviews.py --limit 500              # first 500 reviews
    python scrape_reviews.py --language russian        # only Russian reviews
    python scrape_reviews.py --filter negative         # only negative reviews
"""

import argparse
import csv
import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

APP_ID = 2073850
API_URL = f"https://store.steampowered.com/appreviews/{APP_ID}"
BATCH_SIZE = 100  # max allowed by Steam API
REQUEST_DELAY = 0.5  # seconds between requests to avoid rate limiting


def fetch_reviews_batch(cursor="*", language="all", review_filter="all",
                        review_type="all", purchase_type="all"):
    """Fetch a single batch of reviews from the Steam API."""
    params = {
        "json": "1",
        "num_per_page": str(BATCH_SIZE),
        "cursor": cursor,
        "language": language,
        "filter": review_filter,
        "review_type": review_type,
        "purchase_type": purchase_type,
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def unix_to_iso(ts):
    """Convert unix timestamp to ISO 8601 string."""
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def scrape_all_reviews(limit=None, language="all", review_filter="all",
                       review_type="all", purchase_type="all"):
    """Fetch all reviews using cursor-based pagination."""
    all_reviews = []
    cursor = "*"
    seen_ids = set()
    page = 0

    # First request to get totals
    data = fetch_reviews_batch(cursor, language, review_filter, review_type, purchase_type)
    if not data.get("success"):
        print("Error: Steam API returned unsuccessful response.", file=sys.stderr)
        sys.exit(1)

    summary = data.get("query_summary", {})
    total = summary.get("total_reviews", 0)
    print(f"Total reviews available: {total}")
    print(f"Score: {summary.get('review_score_desc', '?')} "
          f"(+{summary.get('total_positive', 0)} / -{summary.get('total_negative', 0)})")

    target = min(limit, total) if limit else total
    print(f"Fetching up to {target} reviews...\n")

    # Process first batch
    for r in data.get("reviews", []):
        rid = r["recommendationid"]
        if rid not in seen_ids:
            seen_ids.add(rid)
            all_reviews.append(r)
    cursor = data.get("cursor")
    page += 1
    print(f"  Page {page}: got {len(data.get('reviews', []))} reviews "
          f"(total collected: {len(all_reviews)})")

    # Paginate
    while cursor and (not limit or len(all_reviews) < target):
        time.sleep(REQUEST_DELAY)
        try:
            data = fetch_reviews_batch(cursor, language, review_filter,
                                       review_type, purchase_type)
        except Exception as e:
            print(f"  Request error: {e}. Retrying in 5s...", file=sys.stderr)
            time.sleep(5)
            continue

        reviews = data.get("reviews", [])
        if not reviews:
            break

        new_count = 0
        for r in reviews:
            rid = r["recommendationid"]
            if rid not in seen_ids:
                seen_ids.add(rid)
                all_reviews.append(r)
                new_count += 1

        cursor = data.get("cursor")
        page += 1
        print(f"  Page {page}: got {len(reviews)} reviews, {new_count} new "
              f"(total collected: {len(all_reviews)})")

        if new_count == 0:
            print("  No new reviews found, stopping pagination.")
            break

    if limit:
        all_reviews = all_reviews[:limit]

    return all_reviews, summary


def save_csv(reviews, output_path):
    """Save reviews to a CSV file."""
    fieldnames = [
        "recommendationid",
        "steamid",
        "personaname",
        "voted_up",
        "language",
        "review",
        "timestamp_created",
        "timestamp_updated",
        "playtime_forever_hours",
        "playtime_at_review_hours",
        "votes_up",
        "votes_funny",
        "weighted_vote_score",
        "comment_count",
        "steam_purchase",
        "received_for_free",
        "written_during_early_access",
        "primarily_steam_deck",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for r in reviews:
            author = r.get("author", {})
            row = {
                "recommendationid": r.get("recommendationid", ""),
                "steamid": author.get("steamid", ""),
                "personaname": author.get("personaname", ""),
                "voted_up": r.get("voted_up", ""),
                "language": r.get("language", ""),
                "review": r.get("review", ""),
                "timestamp_created": unix_to_iso(r.get("timestamp_created")),
                "timestamp_updated": unix_to_iso(r.get("timestamp_updated")),
                "playtime_forever_hours": round(author.get("playtime_forever", 0) / 60, 1),
                "playtime_at_review_hours": round(author.get("playtime_at_review", 0) / 60, 1),
                "votes_up": r.get("votes_up", 0),
                "votes_funny": r.get("votes_funny", 0),
                "weighted_vote_score": r.get("weighted_vote_score", ""),
                "comment_count": r.get("comment_count", 0),
                "steam_purchase": r.get("steam_purchase", ""),
                "received_for_free": r.get("received_for_free", ""),
                "written_during_early_access": r.get("written_during_early_access", ""),
                "primarily_steam_deck": r.get("primarily_steam_deck", ""),
            }
            writer.writerow(row)


def save_json(reviews, summary, output_path):
    """Save reviews and summary to a JSON file."""
    data = {
        "app_id": APP_ID,
        "scraped_at": datetime.now(tz=timezone.utc).isoformat(),
        "summary": summary,
        "reviews_count": len(reviews),
        "reviews": reviews,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Scrape Steam reviews for THE FINALS")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of reviews to fetch (default: all)")
    parser.add_argument("--language", default="all",
                        help="Language filter: all, english, russian, schinese, etc.")
    parser.add_argument("--filter", dest="review_filter", default="all",
                        choices=["recent", "updated", "all"],
                        help="Sort/filter mode (default: all)")
    parser.add_argument("--type", dest="review_type", default="all",
                        choices=["all", "positive", "negative"],
                        help="Review type filter (default: all)")
    parser.add_argument("--format", dest="output_format", default="csv",
                        choices=["csv", "json"],
                        help="Output format (default: csv)")
    parser.add_argument("--output", default=None,
                        help="Output file path (default: reviews_APPID.csv/json)")

    args = parser.parse_args()

    output = args.output
    if not output:
        output = f"reviews_{APP_ID}.{args.output_format}"

    print(f"Scraping reviews for THE FINALS (appid {APP_ID})...")
    print(f"Language: {args.language}, Filter: {args.review_filter}, "
          f"Type: {args.review_type}\n")

    reviews, summary = scrape_all_reviews(
        limit=args.limit,
        language=args.language,
        review_filter=args.review_filter,
        review_type=args.review_type,
    )

    if args.output_format == "json":
        save_json(reviews, summary, output)
    else:
        save_csv(reviews, output)

    print(f"\nDone! Saved {len(reviews)} reviews to {output}")


if __name__ == "__main__":
    main()
