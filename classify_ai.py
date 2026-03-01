"""
AI Classification of THE FINALS Steam Reviews via PPQ API (Gemini 2.5 Flash Lite).
Processes all 247k reviews in batches, saves results incrementally.

Sentiment-split approach: positive and negative reviews are classified separately
with different system prompts to prevent cross-sentiment category assignment.
"""

import json
import time
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# --- Config ---
API_KEY = os.environ["PPQ_API_KEY"]
BASE_URL = os.environ.get("PPQ_BASE_URL", "https://api.ppq.ai")
MODEL = "google/gemini-2.5-flash-lite"

BATCH_SIZE = 200          # reviews per API call
MAX_RETRIES = 3           # retries per batch on failure
RETRY_DELAY = 5           # seconds between retries
DELAY_BETWEEN_BATCHES = 1 # seconds between successful batches
TEMPERATURE = 0.1         # low temp for consistent classification
MAX_TOKENS = 16000        # enough for 200 classifications

REVIEWS_FILE = "reviews_all.json"
CATEGORIES_FILE = "categories_final.json"
OUTPUT_FILE = "reviews_ai_classified.json"
PROGRESS_FILE = "classify_progress.json"

# --- Load categories ---
with open(CATEGORIES_FILE, encoding="utf-8") as f:
    categories = json.load(f)

neg_cat_names = set(categories["negative"].keys())
pos_cat_names = set(categories["positive"].keys())

neg_cats_text = "\n".join(
    f"- **{name}**: {desc}" for name, desc in categories["negative"].items()
)
pos_cats_text = "\n".join(
    f"- **{name}**: {desc}" for name, desc in categories["positive"].items()
)

# --- Shared prompt parts ---
GAME_KNOWLEDGE = """Key game knowledge:
- 3v3v3v3 matches (four teams of three), destructible environments
- Cashout mode: carry vault to cashout station, defend while it deposits
- Three weight classes: Light (invisibility, grapple, dash), Medium (healing beam, defibrillator, zipline), Heavy (mesh shield, dome, charge slam, RPG, C4)
- Gadgets: mines, turrets, goo/gas/glitch grenades, APS turret, barricades, thermal bore
- Weapons: ARs, shotguns, swords/melee, snipers, SMGs, LMGs, revolvers, throwing knives
- Free-to-play with battle pass and cosmetic shop, crossplay PC/console
- Game modes: Cashout, Bank It, Power Shift, Terminal Attack, Quick Cash, Ranked
- Seasons S1-S9 with meta shifts and balance patches"""

PLAYTIME_CONTEXT = """## PLAYTIME CONTEXT:
Each review includes the reviewer's total playtime in hours. Use this as context:
- A veteran (500+ hours) complaining about balance carries more weight and specificity.
- A new player (< 10 hours) saying "game is bad" is more likely vague frustration.
- Long playtime + negative review often signals burnout, meta fatigue, or specific grievances.
- Short playtime + positive review is often first impressions (fun, graphics, uniqueness).
- Do NOT create a category based on playtime alone — it's context, not content."""

CLASSIFICATION_RULES = """## RULES:
1. Use EXACT category names from the list above — no paraphrasing, no inventing new ones.
2. Return empty cats [] for reviews that are:
   - Too short to classify (single words: "yes", "no", "nice", "6", "хз")
   - Gibberish or random characters ("uww m,üq", "asdf", keyboard smash)
   - Pure memes, ASCII art, or copypasta with no game-specific content
   - Single emoji or punctuation (".", "👍", "♥♥♥")
   - Reviews where censored text (♥♥♥♥♥♥) makes the meaning ambiguous or unclear
   NOTE: A short review like "fun game" IS classifiable. But "yes", "nice", "对", "#1", "GODÍN", "豪玩", "the best", "good", "amazing" alone is NOT — a review must mention at least one game aspect to be classifiable.
3. A review CAN match multiple categories, BUT only if EACH category is EXPLICITLY and CLEARLY supported by the text. When in doubt, use FEWER categories.
4. AVOID OVER-CLASSIFICATION:
   - "kill and steal" does NOT imply "3v3v3v3 Format" unless the review EXPLICITLY mentions multiple teams or third-partying.
   - "matchmaking is bad" does NOT imply "Server Performance & Connectivity" — matchmaking and servers are DIFFERENT.
   - A general complaint about teammates does NOT imply "Game Mode Complaints" — only assign if a SPECIFIC mode is named.
   - "Player Behavior & Toxicity" requires EXPLICIT mention of toxicity, AFK, leavers, griefing.
   - Do NOT add categories based on tone or sarcasm — only based on explicit content.
5. Be concise. Do NOT add any text outside the JSON array."""

# --- Build sentiment-specific system prompts ---
SYSTEM_PROMPT_NEGATIVE = f"""You are a review classifier for THE FINALS, a free-to-play team-based FPS by Embark Studios.

{GAME_KNOWLEDGE}

Your task: classify each NEGATIVE review (thumbs down) into one or more categories from the list below. A review can match ZERO or MULTIPLE categories.
ALL reviews in this batch are NEGATIVE (voted_up=false). You may ONLY use categories from the list below.

## CATEGORIES (for negative reviews only):
{neg_cats_text}

{PLAYTIME_CONTEXT}

## OUTPUT FORMAT:
Return ONLY a valid JSON array. For each review:
{{"idx": <review index>, "cats": ["Category 1", "Category 2"], "conf": "high"|"medium"|"low"}}

{CLASSIFICATION_RULES}"""

SYSTEM_PROMPT_POSITIVE = f"""You are a review classifier for THE FINALS, a free-to-play team-based FPS by Embark Studios.

{GAME_KNOWLEDGE}

Your task: classify each POSITIVE review (thumbs up) into one or more categories from the list below. A review can match ZERO or MULTIPLE categories.
ALL reviews in this batch are POSITIVE (voted_up=true). You may ONLY use categories from the list below.

## CATEGORIES (for positive reviews only):
{pos_cats_text}

{PLAYTIME_CONTEXT}

## OUTPUT FORMAT:
Return ONLY a valid JSON array. For each review:
{{"idx": <review index>, "cats": ["Category 1", "Category 2"], "conf": "high"|"medium"|"low"}}

{CLASSIFICATION_RULES}"""

# --- Load reviews ---
print("Loading reviews...")
with open(REVIEWS_FILE, encoding="utf-8") as f:
    raw = json.load(f)
reviews = raw["reviews"]
print(f"Loaded {len(reviews):,} reviews")

# --- Split by sentiment and index ---
pos_indices = [i for i, r in enumerate(reviews) if r["voted_up"]]
neg_indices = [i for i, r in enumerate(reviews) if not r["voted_up"]]
print(f"Positive: {len(pos_indices):,} | Negative: {len(neg_indices):,}")

# --- Load progress (for resume) ---
results = {}
classified_indices = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, encoding="utf-8") as f:
        progress = json.load(f)
    results = {str(k): v for k, v in progress.get("results", {}).items()}
    classified_indices = set(results.keys())
    print(f"Resuming: {len(results):,} reviews already classified")

# --- API client ---
client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

# --- Stats ---
total_tokens_used = 0
total_cost = 0.0
errors = []
batch_times = []
total_sentiment_violations = 0


def classify_batch(indices, system_prompt, allowed_cats):
    """Classify a batch of reviews. Returns list of classification dicts."""
    review_data = []
    for global_idx in indices:
        r = reviews[global_idx]
        hours = r.get("author", {}).get("playtime_forever", 0) / 60
        review_data.append({
            "idx": global_idx,
            "hours": round(hours, 1),
            "text": r["review"][:2000],
        })

    user_msg = (
        f"Classify these {len(review_data)} Steam reviews for THE FINALS.\n\n"
        f"Reviews:\n{json.dumps(review_data, ensure_ascii=False)}\n\n"
        f"Return ONLY a JSON array."
    )

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        temperature=TEMPERATURE,
        max_tokens=MAX_TOKENS,
    )

    usage = response.usage
    content = response.choices[0].message.content

    # Parse JSON (handle markdown code blocks)
    content = content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        content = content.strip()

    parsed = json.loads(content)
    return parsed, usage


def save_progress(batch_num):
    """Save progress for resume capability."""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "next_batch": batch_num,
            "results": results,
            "total_tokens": total_tokens_used,
            "errors": errors[-20:],
        }, f)


def process_stream(indices, sentiment_label, system_prompt, allowed_cats):
    """Process a stream of reviews (all pos or all neg) in batches."""
    global total_tokens_used, total_cost, total_sentiment_violations

    # Filter out already classified
    remaining = [i for i in indices if str(i) not in classified_indices]
    if not remaining:
        print(f"  All {sentiment_label} reviews already classified, skipping")
        return

    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n{'='*60}")
    print(f"Processing {len(remaining):,} {sentiment_label} reviews in {total_batches} batches")
    print(f"{'='*60}")

    for b_idx in range(total_batches):
        batch_start = b_idx * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(remaining))
        batch_indices = remaining[batch_start:batch_end]

        t0 = time.time()
        success = False

        for attempt in range(MAX_RETRIES):
            try:
                parsed, usage = classify_batch(batch_indices, system_prompt, allowed_cats)

                # Store results with programmatic validation
                violations = 0
                for item in parsed:
                    idx = str(item.get("idx", ""))
                    cats = item.get("cats", item.get("categories", []))
                    conf = item.get("conf", item.get("confidence", "low"))

                    # Filter to allowed categories only (safety net)
                    filtered = [c for c in cats if c in allowed_cats]
                    if len(filtered) != len(cats):
                        violations += 1
                    cats = filtered

                    results[idx] = {
                        "categories": cats,
                        "confidence": conf,
                    }

                if violations > 0:
                    total_sentiment_violations += violations
                    print(f"  ⚠ Fixed {violations} invalid categories in batch")

                elapsed = time.time() - t0
                batch_times.append(elapsed)
                total_tokens_used += usage.total_tokens

                cost = (usage.prompt_tokens * 0.07 + usage.completion_tokens * 0.28) / 1_000_000
                total_cost += cost

                pct = (b_idx + 1) / total_batches * 100
                avg_time = sum(batch_times[-50:]) / len(batch_times[-50:])
                eta_min = (total_batches - b_idx - 1) * avg_time / 60

                print(
                    f"  [{sentiment_label}] {b_idx+1}/{total_batches} ({pct:.1f}%) | "
                    f"{len(parsed)} classified | {elapsed:.1f}s | "
                    f"tokens: {usage.total_tokens:,} | "
                    f"cost: ${total_cost:.2f} | "
                    f"ETA: {eta_min:.0f}min"
                )

                success = True
                break

            except json.JSONDecodeError as e:
                print(f"  [{sentiment_label}] Batch {b_idx+1} attempt {attempt+1}: JSON parse error: {e}")
                errors.append({"batch": f"{sentiment_label}_{b_idx}", "error": f"JSON: {e}", "attempt": attempt+1})
                time.sleep(RETRY_DELAY)

            except Exception as e:
                print(f"  [{sentiment_label}] Batch {b_idx+1} attempt {attempt+1}: {e}")
                errors.append({"batch": f"{sentiment_label}_{b_idx}", "error": str(e), "attempt": attempt+1})
                time.sleep(RETRY_DELAY * (attempt + 1))

        if not success:
            print(f"  FAILED [{sentiment_label}] batch {b_idx+1} after {MAX_RETRIES} attempts — skipping")
            errors.append({"batch": f"{sentiment_label}_{b_idx}", "error": "FAILED after all retries"})

        # Save progress every 10 batches
        if (b_idx + 1) % 10 == 0:
            save_progress(b_idx + 1)

        time.sleep(DELAY_BETWEEN_BATCHES)


# --- Main ---
print(f"\nModel: {MODEL}")
print(f"Estimated cost: ~$2-4 for {len(reviews):,} reviews")

# Process negative reviews first (smaller set, faster)
process_stream(neg_indices, "NEG", SYSTEM_PROMPT_NEGATIVE, neg_cat_names)
save_progress(0)

# Process positive reviews
process_stream(pos_indices, "POS", SYSTEM_PROMPT_POSITIVE, pos_cat_names)
save_progress(0)

# --- Merge results with original reviews ---
print(f"\nClassification complete!")
print(f"Total tokens: {total_tokens_used:,}")
print(f"Total cost: ${total_cost:.2f}")
print(f"Errors: {len(errors)}")
print(f"Sentiment violations fixed: {total_sentiment_violations}")
print(f"Reviews classified: {len(results):,} / {len(reviews):,}")

print(f"\nMerging results into {OUTPUT_FILE}...")
output_reviews = []
for i, review in enumerate(reviews):
    r = dict(review)
    classification = results.get(str(i), {"categories": [], "confidence": "none"})
    r["ai_categories"] = classification["categories"]
    r["ai_confidence"] = classification["confidence"]
    output_reviews.append(r)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(output_reviews, f, ensure_ascii=False)

print(f"Saved {len(output_reviews):,} reviews to {OUTPUT_FILE}")

# --- Summary stats ---
cat_counts = {}
for r in output_reviews:
    for cat in r["ai_categories"]:
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

print(f"\nTop 20 categories:")
for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1])[:20]:
    print(f"  {count:>6,}  {cat}")

unclassified = sum(1 for r in output_reviews if not r["ai_categories"])
print(f"\nUnclassified: {unclassified:,} / {len(output_reviews):,} ({unclassified/len(output_reviews)*100:.1f}%)")
