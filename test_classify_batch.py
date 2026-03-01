"""Quick test: classify 50 diverse reviews with Gemini via PPQ API.
Uses sentiment-split approach: positive and negative reviews get separate prompts."""

import json
import time
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

API_KEY = os.environ["PPQ_API_KEY"]
BASE_URL = os.environ.get("PPQ_BASE_URL", "https://api.ppq.ai")
MODEL = "google/gemini-2.5-flash-lite"

# Load categories
with open("categories_final.json", encoding="utf-8") as f:
    categories = json.load(f)

neg_cat_names = set(categories["negative"].keys())
pos_cat_names = set(categories["positive"].keys())

neg_cats_text = "\n".join(f"- **{n}**: {d}" for n, d in categories["negative"].items())
pos_cats_text = "\n".join(f"- **{n}**: {d}" for n, d in categories["positive"].items())

# Shared prompt parts
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

# Load test reviews
print("Loading reviews...")
with open("reviews_all.json", encoding="utf-8") as f:
    raw = json.load(f)

reviews = raw["reviews"]

# Pick 50 diverse reviews
import hashlib, datetime
seed = int(hashlib.md5(datetime.datetime.now().isoformat().encode()).hexdigest(), 16) % len(reviews)
step = len(reviews) // 50
sample_indices = [(seed + i * step) % len(reviews) for i in range(50)]
sample = [reviews[i] for i in sample_indices]

print(f"Test sample: {len(sample)} reviews")
print(f"  Positive: {sum(1 for r in sample if r['voted_up'])}")
print(f"  Negative: {sum(1 for r in sample if not r['voted_up'])}")
langs = {}
for r in sample:
    l = r.get("language", "?")
    langs[l] = langs.get(l, 0) + 1
print(f"  Languages: {dict(sorted(langs.items(), key=lambda x: -x[1]))}")

# Split by sentiment
pos_items = [(i, r) for i, r in enumerate(sample) if r["voted_up"]]
neg_items = [(i, r) for i, r in enumerate(sample) if not r["voted_up"]]

# Build review data for each group
def build_review_data(items):
    data = []
    for idx, r in items:
        hours = r.get("author", {}).get("playtime_forever", 0) / 60
        data.append({
            "idx": idx,
            "hours": round(hours, 1),
            "text": r["review"][:2000],
        })
    return data

client = OpenAI(api_key=API_KEY, base_url=BASE_URL)
all_results = {}
total_input = 0
total_output = 0
total_time = 0

# Classify negative reviews
if neg_items:
    neg_data = build_review_data(neg_items)
    user_msg = f"Classify these {len(neg_data)} Steam reviews for THE FINALS.\n\nReviews:\n{json.dumps(neg_data, ensure_ascii=False)}\n\nReturn ONLY a JSON array."

    print(f"\nSending {len(neg_data)} NEGATIVE reviews to {MODEL}...")
    t0 = time.time()
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT_NEGATIVE},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.1,
        max_tokens=8000,
    )
    elapsed = time.time() - t0
    total_time += elapsed
    total_input += response.usage.prompt_tokens
    total_output += response.usage.completion_tokens
    print(f"  Done in {elapsed:.1f}s | tokens: {response.usage.total_tokens:,}")

    content = response.choices[0].message.content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:]).strip()

    try:
        parsed = json.loads(content)
        for item in parsed:
            all_results[item["idx"]] = item
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw: {content[:300]}")

# Classify positive reviews
if pos_items:
    pos_data = build_review_data(pos_items)
    user_msg = f"Classify these {len(pos_data)} Steam reviews for THE FINALS.\n\nReviews:\n{json.dumps(pos_data, ensure_ascii=False)}\n\nReturn ONLY a JSON array."

    print(f"\nSending {len(pos_data)} POSITIVE reviews to {MODEL}...")
    t0 = time.time()
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT_POSITIVE},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.1,
        max_tokens=8000,
    )
    elapsed = time.time() - t0
    total_time += elapsed
    total_input += response.usage.prompt_tokens
    total_output += response.usage.completion_tokens
    print(f"  Done in {elapsed:.1f}s | tokens: {response.usage.total_tokens:,}")

    content = response.choices[0].message.content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:]).strip()

    try:
        parsed = json.loads(content)
        for item in parsed:
            all_results[item["idx"]] = item
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  Raw: {content[:300]}")

print(f"\nTotal: {total_time:.1f}s | input: {total_input:,} | output: {total_output:,} | total: {total_input+total_output:,}")

# Display results
print("\n" + "=" * 80)
print("CLASSIFICATION RESULTS")
print("=" * 80)

cat_counts = {}
multi_count = 0
empty_count = 0
sentiment_violations = 0

for i in range(len(sample)):
    if i not in all_results:
        continue
    item = all_results[i]
    cats = item.get("cats", [])
    conf = item.get("conf", "?")
    r = sample[i]
    hours = r.get("author", {}).get("playtime_forever", 0) / 60
    sentiment = "+" if r["voted_up"] else "-"
    text_preview = r["review"][:120].replace("\n", " ")

    # Check sentiment lock
    is_positive = r["voted_up"]
    allowed = pos_cat_names if is_positive else neg_cat_names
    bad_cats = [c for c in cats if c not in allowed and c in (neg_cat_names | pos_cat_names)]
    if bad_cats:
        sentiment_violations += 1

    for c in cats:
        cat_counts[c] = cat_counts.get(c, 0) + 1
    if len(cats) > 1:
        multi_count += 1
    if not cats:
        empty_count += 1

    violation_mark = " ⚠ SENTIMENT VIOLATION" if bad_cats else ""
    cats_str = ", ".join(cats) if cats else "[unclassified]"
    print(f"\n[{sentiment}] #{i} | {hours:.0f}h | {r.get('language', '?')} | conf={conf}{violation_mark}")
    print(f"  Text: {text_preview}")
    print(f"  Categories: {cats_str}")

total = len(all_results)
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total classified: {total}")
print(f"Multi-category: {multi_count} ({multi_count/total*100:.0f}%)")
print(f"Sentiment violations: {sentiment_violations} ({sentiment_violations/total*100:.0f}%)")
print(f"Unclassified: {empty_count} ({empty_count/total*100:.0f}%)")
print(f"\nCategory distribution:")
for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
    print(f"  {count:>3}  {cat}")

# Save full results
import os
os.makedirs("test_results", exist_ok=True)
review_data = []
for i, r in enumerate(sample):
    hours = r.get("author", {}).get("playtime_forever", 0) / 60
    review_data.append({
        "idx": i, "hours": round(hours, 1),
        "voted_up": r["voted_up"], "text": r["review"][:2000],
    })
with open("test_results/test_batch_50.json", "w", encoding="utf-8") as f:
    json.dump({"reviews": review_data, "classifications": list(all_results.values())}, f, ensure_ascii=False, indent=2)
print(f"\nFull results saved to test_results/test_batch_50.json")
