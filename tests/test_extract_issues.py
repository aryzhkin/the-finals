"""Test Stage 2: extract issues from 20 diverse reviews (10 neg + 10 pos)."""

import json
import os
import time
import random
from openai import OpenAI

# Reuse prompts from main script
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
exec(open(os.path.join(ROOT_DIR, "scripts", "extract_issues.py")).read().split("# --- Load reviews ---")[0])

# Load reviews
with open(INPUT_FILE, encoding="utf-8") as f:
    reviews = json.load(f)

# Pick 20 diverse reviews with actual text content
random.seed(42)
neg_pool = [i for i, r in enumerate(reviews) if not r["voted_up"] and len(r["review"]) > 30]
pos_pool = [i for i, r in enumerate(reviews) if r["voted_up"] and len(r["review"]) > 30]
neg_sample = random.sample(neg_pool, 10)
pos_sample = random.sample(pos_pool, 10)

client = OpenAI(api_key=API_KEY, base_url=BASE_URL)


def run_batch(indices, system_prompt, label):
    review_data = []
    for gi in indices:
        r = reviews[gi]
        hours = r.get("author", {}).get("playtime_forever", 0) / 60
        cats = r.get("ai_categories", [])
        review_data.append({
            "idx": gi,
            "hours": round(hours, 1),
            "cats": cats,
            "text": r["review"][:2000],
        })

    user_msg = (
        f"Extract specific issues from these {len(review_data)} reviews.\n\n"
        f"Reviews:\n{json.dumps(review_data, ensure_ascii=False)}\n\n"
        f"Return ONLY a JSON array."
    )

    for attempt in range(3):
        try:
            t0 = time.time()
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                temperature=TEMPERATURE,
                max_tokens=16000,
            )
            elapsed = time.time() - t0

            content = response.choices[0].message.content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:]).strip()

            parsed = json.loads(content)
            print(f"{label}: {elapsed:.1f}s | tokens: {response.usage.total_tokens:,}")
            return parsed
        except Exception as e:
            print(f"{label} attempt {attempt+1} failed: {e}")
            time.sleep(5)
    raise RuntimeError(f"{label} failed after 3 attempts")


neg_results = run_batch(neg_sample, SYSTEM_PROMPT_NEGATIVE, "NEG")
time.sleep(10)
pos_results = run_batch(pos_sample, SYSTEM_PROMPT_POSITIVE, "POS")

# Display results
all_results = {item["idx"]: item for item in neg_results + pos_results}

for gi in neg_sample + pos_sample:
    r = reviews[gi]
    sentiment = "+" if r["voted_up"] else "-"
    hours = r.get("author", {}).get("playtime_forever", 0) / 60
    cats = r.get("ai_categories", [])
    text = r["review"][:150].replace("\n", " ")

    item = all_results.get(gi, {})
    issues = item.get("issues", [])

    print(f"\n[{sentiment}] idx={gi} | {hours:.0f}h | {r.get('language', '?')} | cats: {cats}")
    print(f"  Text: {text}")
    if issues:
        for iss in issues:
            prim = "★" if iss.get("primary") else " "
            sref = f" [{iss['season_ref']}]" if iss.get("season_ref") else ""
            ents = ", ".join(iss.get("entities", []))
            print(f"  {prim} [{iss['type']:10}] {iss['text']}{sref}  entities: [{ents}]")
    else:
        print("  (no issues extracted)")
