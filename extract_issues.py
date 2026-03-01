"""
Stage 2: Extract specific issues, complaints, suggestions, and praise from THE FINALS reviews.
Uses AI-classified reviews from Stage 1 + game entity data for normalization.

For each review, extracts structured issues with:
- Normalized text description
- Type: complaint / suggestion / praise
- Game entities referenced (canonical names)
- Whether it's the primary topic of the review
- Season reference if explicitly mentioned
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

BATCH_SIZE = 80           # smaller batches — output is more complex than Stage 1
MAX_RETRIES = 3
RETRY_DELAY = 5
DELAY_BETWEEN_BATCHES = 1
TEMPERATURE = 0.1
MAX_TOKENS = 24000        # higher limit — each review can produce 3-5 issues

INPUT_FILE = "reviews_ai_classified.json"
ENTITIES_FILE = "game_entities.json"
OUTPUT_FILE = "reviews_issues.json"
PROGRESS_FILE = "extract_issues_progress.json"

# --- Load game entities ---
with open(ENTITIES_FILE, encoding="utf-8") as f:
    entities = json.load(f)

# Build flat entity list for the prompt
all_weapons = []
for cls, weapons in entities["weapons"].items():
    all_weapons.extend(f"{w} ({cls})" for w in weapons)

all_specializations = []
for cls, specs in entities["specializations"].items():
    all_specializations.extend(f"{s} ({cls})" for s in specs)

all_gadgets = []
for cls, gadgets in entities["gadgets"].items():
    all_gadgets.extend(f"{g} ({cls})" for g in gadgets)

ENTITIES_TEXT = f"""## GAME ENTITIES (use these canonical names):

**Classes**: Light (150 HP, fast), Medium (250 HP, support), Heavy (350 HP, tank)

**Weapons**:
{', '.join(all_weapons)}

**Specializations/Abilities**:
{', '.join(all_specializations)}

**Gadgets**:
{', '.join(all_gadgets)}

**Game Modes**: {', '.join(entities['game_modes'].keys())}

**Arenas/Maps**: {', '.join(entities['arenas'])}

**Common Aliases** (normalize these):
sword/katana → Sword, sniper → SR-84/XP-54, invis/cloak → Cloaking Device,
grapple → Grappling Hook, dash → Evasive Dash, heal beam → Healing Beam,
defib → Defibrillator, mesh → Mesh Shield, dome → Dome Shield,
slam/charge → Charge N Slam, rpg → RPG-7, turret → Guardian Turret/APS Turret,
lewis → Lewis Gun, akm → AKM, fcar → FCAR"""

# --- Season data ---
SEASONS_TEXT = """## SEASONS (for season_ref field):
S0 (Closed Beta), S1 (Dec 2023), S2 (Mar 2024), S3 (Jun 2024),
S4 (Sep 2024), S5 (Dec 2024), S6 (Feb 2025), S7 (May 2025),
S8 (Aug 2025), S9 (Nov 2025)
Only set season_ref if the review EXPLICITLY mentions a season, patch, or update by name/number."""

# --- System prompts (sentiment-split as in Stage 1) ---
SHARED_INSTRUCTIONS = f"""You are an issue extractor for THE FINALS, a free-to-play team-based FPS by Embark Studios.

{ENTITIES_TEXT}

{SEASONS_TEXT}

## YOUR TASK:
For each review, extract specific issues, complaints, suggestions, or praise points.
Each review includes its Stage 1 categories for context — use them to guide extraction but look for SPECIFICS.

## OUTPUT FORMAT:
Return ONLY a valid JSON array. For each review:
{{"idx": <review index>, "issues": [
  {{
    "text": "Short normalized description in English",
    "type": "complaint"|"suggestion"|"praise",
    "entities": ["Entity1", "Entity2"],
    "primary": true|false,
    "season_ref": "S7"|null
  }}
]}}

## TEXT NORMALIZATION — CRITICAL:
Use these STANDARD PATTERNS for the "text" field so that identical issues from different reviews produce IDENTICAL strings:

**For weapons/gadgets/abilities:**
- "{{Entity}}: overpowered" / "{{Entity}}: underpowered" / "{{Entity}}: needs nerf" / "{{Entity}}: needs buff"
- "{{Entity}}: no skill required" / "{{Entity}}: no counterplay" / "{{Entity}}: unfun to play against"
- "{{Entity}}: broken / bugged"

**For classes:**
- "{{Class}}: overpowered" / "{{Class}}: underpowered" / "{{Class}}: unfun to play against"
- "{{Class}}: too fast / too slow / too tanky / too fragile"

**For infrastructure:**
- "Servers: high latency" / "Servers: disconnects" / "Servers: crashes"
- "Matchmaking: skill disparity" / "Matchmaking: solo vs premade stacks" / "Matchmaking: long queue times"
- "Anti-cheat: ineffective" / "Cheating: too many cheaters" / "Cheating: aimbots" / "Cheating: wallhacks"
- "Cheating: region-specific" (when review blames specific region, e.g. China)

**For player behavior:**
- "Teammates: leaving" / "Teammates: AFK" / "Teammates: toxic"
- "Players: toxic chat" / "Players: griefing" / "Players: cheating reports ignored"

**For game design:**
- "{{Mode}}: unbalanced" / "{{Mode}}: unfun" / "{{Mode}}: needs rework"
- "TTK: too fast" / "TTK: too slow"
- "Respawn: too easy" / "Respawn: too punishing"
- "Content: needs more maps" / "Content: needs more modes" / "Content: game feels stale"
- "Game pace: too slow" / "Game pace: too fast" (overall match tempo, NOT movement speed)
- "Movement: too slow" / "Movement: nerfed" (character movement speed specifically)

**For praise:**
- "Destruction: satisfying" / "Destruction: unique mechanic"
- "Gameplay: fun and addictive" / "Gameplay: unique among FPS"
- "Gunplay: satisfying" / "Movement: fluid and fast"
- "{{Mode}}: fun" / "{{Class}}: fun to play" / "{{Entity}}: fun to use"
- "Graphics: beautiful" / "Graphics: well polished"
- "F2P: fair model" / "F2P: good value"

**For suggestions:**
- "Add: solo ranked queue" / "Add: text chat" / "Add: VOIP improvements"
- "Add: more maps" / "Add: new weapons" / "Add: new game modes"
- "Fix: anti-cheat" / "Fix: server stability" / "Fix: matchmaking"
- "Remove: forced crossplay" / "Remove: {{feature}}"
- "Add: region lock" / "Add: localization for {{language}}"

You MAY create variations not listed above, but ALWAYS follow the "{{Topic}}: {{short description}}" pattern.
Two reviews saying the same thing MUST produce the SAME text string.

## ENTITY EXTRACTION — CRITICAL:
The "entities" field must contain ALL game elements mentioned or clearly implied in the issue:
- If a review says "cheaters" → entities: [] (cheating is not a game entity)
- If a review says "sword is broken" → entities: ["Sword", "Light"] (Sword is Light-class weapon)
- If a review says "invis is OP" → entities: ["Cloaking Device", "Light"]
- If a review says "heal beam + defib combo" → entities: ["Healing Beam", "Defibrillator", "Medium"]
- If a review says "RPG spam" → entities: ["RPG-7", "Heavy"]
- If a review says "ranked is broken" → entities: ["Ranked Cashout"]
- If a review says "Power Shift is boring" → entities: ["Power Shift"]
- If a review says "shotguns are OP" but doesn't name which → entities: [] (too vague)
- If a review says "the game runs well" → entities: [] (general, no specific entity)
- ALWAYS include the class that owns a weapon/ability/gadget when you include that weapon/ability/gadget.

## EXAMPLES (follow this style exactly):

Input: {{"idx": 1001, "hours": 45.2, "cats": ["Cheating & Anti-Cheat", "Region Lock & China"], "text": "Fun game ruined by Chinese hackers. Every other match has an aimbotter. Please add region lock."}}
Output: {{"idx": 1001, "issues": [
  {{"text": "Gameplay: fun and addictive", "type": "praise", "entities": [], "primary": false, "season_ref": null}},
  {{"text": "Cheating: too many cheaters", "type": "complaint", "entities": [], "primary": true, "season_ref": null}},
  {{"text": "Cheating: aimbots", "type": "complaint", "entities": [], "primary": true, "season_ref": null}},
  {{"text": "Cheating: region-specific", "type": "complaint", "entities": [], "primary": false, "season_ref": null}},
  {{"text": "Add: region lock", "type": "suggestion", "entities": [], "primary": false, "season_ref": null}}
]}}

Input: {{"idx": 1002, "hours": 312.0, "cats": ["Weapon Imbalance", "Light Class OP"], "text": "Since S6 the sword is completely broken. One-hit kill with no counterplay. Invis + sword combo is cancer. Meanwhile AKM got nerfed for no reason."}}
Output: {{"idx": 1002, "issues": [
  {{"text": "Sword: overpowered", "type": "complaint", "entities": ["Sword", "Light"], "primary": true, "season_ref": "S6"}},
  {{"text": "Sword: no counterplay", "type": "complaint", "entities": ["Sword", "Light"], "primary": true, "season_ref": "S6"}},
  {{"text": "Cloaking Device: overpowered", "type": "complaint", "entities": ["Cloaking Device", "Light"], "primary": false, "season_ref": null}},
  {{"text": "AKM: overnerfed", "type": "complaint", "entities": ["AKM", "Medium"], "primary": false, "season_ref": null}}
]}}

Input: {{"idx": 1003, "hours": 2.1, "cats": [], "text": "yes"}}
Output: {{"idx": 1003, "issues": []}}

## RULES:
1. Use the STANDARD PATTERNS above for text normalization. This is critical for aggregation.
2. **Normalize entity names** to the canonical list above. "katana" → "Sword", "invis" → "Cloaking Device"
3. **type field**:
   - "complaint" = something is wrong/broken/unfair
   - "suggestion" = player proposes a change or addition
   - "praise" = player likes this specific thing
4. **primary** = true if this is the MAIN topic of the review, false if mentioned in passing
5. **season_ref** = only if the review EXPLICITLY names a season ("S6", "season 7", "last update", "since the patch"). Do NOT guess from review date.
6. **Empty issues []** for reviews that are:
   - Too short to extract anything ("yes", "no", "nice", "👍")
   - Gibberish or random characters
   - Pure memes with no extractable game feedback
   - Censored text (♥♥♥♥♥♥) making meaning unclear
7. **Multiple issues per review are expected** — a review often touches 2-4 topics.
8. Keep "text" SHORT (under 60 chars).
9. Do NOT add any text outside the JSON array."""

SYSTEM_PROMPT_NEGATIVE = f"""{SHARED_INSTRUCTIONS}

ALL reviews in this batch are NEGATIVE (thumbs down). Focus on extracting complaints and suggestions.
Praise points CAN appear in negative reviews ("game is fun BUT cheaters ruin it" — extract both)."""

SYSTEM_PROMPT_POSITIVE = f"""{SHARED_INSTRUCTIONS}

ALL reviews in this batch are POSITIVE (thumbs up). Focus on extracting praise and positive observations.
Complaints CAN appear in positive reviews ("love the game BUT servers lag" — extract both)."""

# --- Load reviews ---
print("Loading classified reviews...")
with open(INPUT_FILE, encoding="utf-8") as f:
    reviews = json.load(f)
print(f"Loaded {len(reviews):,} reviews")

# --- Pre-filter obviously empty reviews ---
MIN_TEXT_LEN = 5  # reviews shorter than this with no categories → skip

def is_worth_processing(r):
    """Returns True if the review is worth sending to the API."""
    text = r.get("review", "").strip()
    cats = r.get("ai_categories", [])
    # If it has categories from Stage 1, always process it
    if cats:
        return True
    # No categories — only process if text is long enough to contain feedback
    return len(text) >= MIN_TEXT_LEN

skipped_indices = set()
for i, r in enumerate(reviews):
    if not is_worth_processing(r):
        skipped_indices.add(i)

# --- Split by sentiment (excluding pre-filtered) ---
pos_indices = [i for i, r in enumerate(reviews) if r["voted_up"] and i not in skipped_indices]
neg_indices = [i for i, r in enumerate(reviews) if not r["voted_up"] and i not in skipped_indices]
print(f"Positive: {len(pos_indices):,} | Negative: {len(neg_indices):,} | Skipped (too short): {len(skipped_indices):,}")

# --- Load progress ---
results = {}
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, encoding="utf-8") as f:
        progress = json.load(f)
    results = {str(k): v for k, v in progress.get("results", {}).items()}
    print(f"Resuming: {len(results):,} reviews already processed")

# --- API client ---
client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

# --- Stats ---
total_tokens_used = 0
total_cost = 0.0
errors = []
batch_times = []


def extract_batch(indices, system_prompt):
    """Extract issues from a batch of reviews."""
    review_data = []
    for global_idx in indices:
        r = reviews[global_idx]
        hours = r.get("author", {}).get("playtime_forever", 0) / 60
        cats = r.get("ai_categories", [])
        review_data.append({
            "idx": global_idx,
            "hours": round(hours, 1),
            "cats": cats,
            "text": r["review"][:2000],
        })

    user_msg = (
        f"Extract specific issues from these {len(review_data)} reviews.\n\n"
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

    content = response.choices[0].message.content.strip()
    if content.startswith("```"):
        lines = content.split("\n")
        content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        content = content.strip()

    parsed = json.loads(content)
    return parsed, response.usage


def save_progress():
    """Save progress for resume capability."""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "results": results,
            "total_tokens": total_tokens_used,
            "errors": errors[-20:],
        }, f)


def process_stream(indices, sentiment_label, system_prompt):
    """Process a stream of reviews in batches."""
    global total_tokens_used, total_cost

    remaining = [i for i in indices if str(i) not in results]
    if not remaining:
        print(f"  All {sentiment_label} reviews already processed, skipping")
        return

    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n{'='*60}")
    print(f"Extracting issues from {len(remaining):,} {sentiment_label} reviews in {total_batches} batches")
    print(f"{'='*60}")

    for b_idx in range(total_batches):
        batch_start = b_idx * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(remaining))
        batch_indices = remaining[batch_start:batch_end]

        t0 = time.time()
        success = False

        for attempt in range(MAX_RETRIES):
            try:
                parsed, usage = extract_batch(batch_indices, system_prompt)

                for item in parsed:
                    idx = str(item.get("idx", ""))
                    issues = item.get("issues", [])

                    # Validate issue structure
                    clean_issues = []
                    for iss in issues:
                        if isinstance(iss, dict) and "text" in iss:
                            clean_issues.append({
                                "text": str(iss.get("text", ""))[:100],
                                "type": iss.get("type", "complaint") if iss.get("type") in ("complaint", "suggestion", "praise") else "complaint",
                                "entities": iss.get("entities", []) if isinstance(iss.get("entities"), list) else [],
                                "primary": bool(iss.get("primary", False)),
                                "season_ref": iss.get("season_ref") if isinstance(iss.get("season_ref"), str) else None,
                            })

                    results[idx] = clean_issues

                elapsed = time.time() - t0
                batch_times.append(elapsed)
                total_tokens_used += usage.total_tokens

                cost = (usage.prompt_tokens * 0.07 + usage.completion_tokens * 0.28) / 1_000_000
                total_cost += cost

                pct = (b_idx + 1) / total_batches * 100
                avg_time = sum(batch_times[-50:]) / len(batch_times[-50:])
                eta_min = (total_batches - b_idx - 1) * avg_time / 60

                issues_count = sum(len(results.get(str(gi), [])) for gi in batch_indices)

                print(
                    f"  [{sentiment_label}] {b_idx+1}/{total_batches} ({pct:.1f}%) | "
                    f"{len(parsed)} reviews | {issues_count} issues | {elapsed:.1f}s | "
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
            save_progress()

        time.sleep(DELAY_BETWEEN_BATCHES)


# --- Main ---
print(f"\nModel: {MODEL}")
print(f"Batch size: {BATCH_SIZE}")
to_process = len(pos_indices) + len(neg_indices)
print(f"Estimated cost: ~$10-15 for {to_process:,} reviews (skipping {len(skipped_indices):,} empty)")

# Process negative reviews first
process_stream(neg_indices, "NEG", SYSTEM_PROMPT_NEGATIVE)
save_progress()

# Process positive reviews
process_stream(pos_indices, "POS", SYSTEM_PROMPT_POSITIVE)
save_progress()

# --- Merge and save ---
print(f"\nExtraction complete!")
print(f"Total tokens: {total_tokens_used:,}")
print(f"Total cost: ${total_cost:.2f}")
print(f"Errors: {len(errors)}")
print(f"Reviews processed: {len(results):,} / {len(reviews):,}")

print(f"\nSaving to {OUTPUT_FILE}...")
output = []
for i, review in enumerate(reviews):
    issues = results.get(str(i), [])
    output.append({
        "idx": i,
        "voted_up": review["voted_up"],
        "language": review.get("language", "unknown"),
        "playtime_hours": round(review.get("author", {}).get("playtime_forever", 0) / 60, 1),
        "timestamp": review.get("timestamp_created", 0),
        "ai_categories": review.get("ai_categories", []),
        "issues": issues,
    })

tmp_file = OUTPUT_FILE + ".tmp"
with open(tmp_file, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False)
    f.flush()
    os.fsync(f.fileno())
os.replace(tmp_file, OUTPUT_FILE)
print(f"Saved {len(output):,} reviews to {OUTPUT_FILE}")

# --- Summary stats ---
total_issues = sum(len(r["issues"]) for r in output)
type_counts = {"complaint": 0, "suggestion": 0, "praise": 0}
entity_counts = {}
primary_count = 0
season_refs = {}

for r in output:
    for iss in r["issues"]:
        t = iss.get("type", "complaint")
        type_counts[t] = type_counts.get(t, 0) + 1
        if iss.get("primary"):
            primary_count += 1
        sr = iss.get("season_ref")
        if sr:
            season_refs[sr] = season_refs.get(sr, 0) + 1
        for e in iss.get("entities", []):
            entity_counts[e] = entity_counts.get(e, 0) + 1

print(f"\nTotal issues extracted: {total_issues:,}")
print(f"Avg issues per review: {total_issues/len(output):.2f}")
print(f"Primary issues: {primary_count:,}")
print(f"\nBy type:")
for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f"  {c:>8,}  {t}")

print(f"\nTop 30 entities:")
for e, c in sorted(entity_counts.items(), key=lambda x: -x[1])[:30]:
    print(f"  {c:>6,}  {e}")

print(f"\nSeason references:")
for s, c in sorted(season_refs.items()):
    print(f"  {c:>5,}  {s}")

no_issues = sum(1 for r in output if not r["issues"])
print(f"\nReviews with no issues: {no_issues:,} ({no_issues/len(output)*100:.1f}%)")
