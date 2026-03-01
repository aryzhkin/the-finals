"""
Stage 1: Category Discovery — test 3 models via PPQ API.
Send the same sample of 111 reviews to each model and compare
their proposed category taxonomies.
"""

import json
import time
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# --- Paths ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
PROMPTS_DIR = os.path.join(ROOT_DIR, "prompts")
RESULTS_DIR = os.path.join(ROOT_DIR, "tests", "test_results")

# --- Config ---
API_KEY = os.environ["PPQ_API_KEY"]
BASE_URL = os.environ.get("PPQ_BASE_URL", "https://api.ppq.ai")

MODELS = [
    "gpt-5-nano",
    "google/gemini-2.5-flash-lite",
    "openai/gpt-4o-mini",
]

# --- Load data ---
with open(os.path.join(DATA_DIR, "category_discovery_sample.json"), encoding="utf-8") as f:
    reviews = json.load(f)

with open(os.path.join(PROMPTS_DIR, "ai_classify_prompt.md"), encoding="utf-8") as f:
    prompt_doc = f.read()

# --- Build prompts ---
SYSTEM_PROMPT = """You are an expert game analytics researcher specializing in competitive FPS games. You are analyzing Steam reviews for THE FINALS — a free-to-play team-based FPS by Embark Studios.

Key game features you MUST know:
- 3v3v3v3 matches (four teams of three) — "third-partying" (a 3rd team swooping in to steal a fight/cashout) is a major community topic
- Destructible environments (buildings collapse, floors break, walls can be blown up)
- Cashout game mode: teams fight over a vault, carry it to a cashout station, and defend while it deposits money
- Three weight classes with unique abilities:
  * Light: fast, fragile, invisibility cloak, grapple hook, stun gun, dash, evasive
  * Medium: balanced, healing beam, recon senses, jump pad, defibrillator (revive), zipline
  * Heavy: tank, mesh shield, charge 'n' slam, goo gun, RPG, C4, dome shield
- Gadgets: mines, turrets, grenades, goo grenades, gas grenades, APS turret, barricades, glitch grenades, thermal bore, etc.
- Weapons: assault rifles, shotguns, swords/melee, sniper rifles, SMGs, LMGs, revolvers, throwing knives, etc.
- Free-to-play with battle pass and cosmetic shop
- Crossplay between PC and consoles
- Game modes: Cashout, Bank It, Power Shift, Terminal Attack, Quick Cash, Ranked (with seasonal rank resets)
- Seasons S1-S9 with meta shifts, weapon/gadget balance patches each season

Your task: analyze 100+ real Steam reviews (in multiple languages) and design the BEST possible category system for classifying all 247,000 reviews in the dataset.

CRITICAL: We need categories at the level of SPECIFIC GAME MECHANICS, not generic game design concepts. Think like a game developer reading player feedback — what specific things can they fix or improve?"""

USER_PROMPT = f"""Below are ~110 real Steam reviews for THE FINALS in 10 languages. Analyze them and propose an optimal category taxonomy.

IMPORTANT — GRANULARITY GUIDANCE:
We want categories tied to SPECIFIC GAME MECHANICS, not generic concepts. Examples:
- BAD: "Game Balance" → GOOD: "Third-Partying" (3rd team steals cashout), "Light Class OP", "Weapon Balance"
- BAD: "Map Design" → GOOD: "Spawn Points", "Map Destruction Issues"
- BAD: "Player Classes" → GOOD: "Light Class OP", "Heavy Class Underpowered"
- BAD: "Combat Issues" → GOOD: "Weapon X is OP", "Gadget Spam"
- BAD: "Progression" → GOOD: "Battle Pass Value", "Rank Reset Frustration"

SPECIAL REQUIREMENTS:
1. Create SEPARATE categories for WEAPON BALANCE and GADGET BALANCE complaints.
   - Players often complain about specific weapons being OP (e.g., "sword is broken", "AKM needs nerf")
     or specific gadgets being unfun (e.g., "mines everywhere", "turret spam", "RPG noob weapon").
   - We want to track which weapons/gadgets are controversial per season.
2. Consider SEPARATE categories for class-specific ability complaints (invisibility, healing beam, mesh shield, etc.) vs weapon/gadget complaints.
3. "Third-partying" (a third team interfering in your fight and stealing the cashout) is a MAJOR community complaint — it should have its own category if found in reviews.
4. Audio/sound design (footsteps, directional audio) is critical in FPS — consider as a separate category.
5. Revive/respawn mechanics are specific to THE FINALS — consider separately.
6. Different game modes may have distinct complaints — consider mode-specific categories if volume justifies it.

CATEGORY REQUIREMENTS:
1. Create SEPARATE lists for negative and positive categories
2. Each category should be:
   - Specific enough to be actionable for game developers
   - Broad enough to capture meaningful volume (not ultra-niche)
   - Clearly distinct from other categories (minimal overlap)
3. For each category provide:
   - Name (short, clear, using community/player language)
   - Description (1 sentence explaining what it covers)
   - 2-3 example phrases/patterns that would match
4. Also suggest if any categories should be SPLIT (too broad) or MERGED (too similar)
5. Consider that reviews are in 30 languages — categories should work cross-linguistically
6. Think about what a game developer would find USEFUL — what specific things can they fix or improve?

After proposing categories, also answer:
- Are there any recurring themes in these reviews that DON'T fit neatly into a single category?
- What percentage of reviews do you estimate would remain "unclassifiable" with your system?
- Any surprising patterns you noticed?
- Which weapons, gadgets, or abilities are mentioned most frequently (positive or negative)?

Reviews:
{json.dumps(reviews, ensure_ascii=False)}"""

# --- Run tests ---
client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

os.makedirs("test_results", exist_ok=True)

for model_id in MODELS:
    print(f"\n{'='*60}")
    print(f"Testing: {model_id}")
    print(f"{'='*60}")

    start = time.time()
    try:
        response = client.chat.completions.create(
            model=model_id,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT},
            ],
            temperature=0.3,
            max_tokens=8000,
        )

        elapsed = time.time() - start
        result = response.choices[0].message.content
        usage = response.usage

        print(f"Time: {elapsed:.1f}s")
        print(f"Input tokens: {usage.prompt_tokens:,}")
        print(f"Output tokens: {usage.completion_tokens:,}")
        print(f"Total tokens: {usage.total_tokens:,}")
        print(f"Response length: {len(result)} chars")
        print(f"First 200 chars: {result[:200]}...")

        # Save full response
        safe_name = model_id.replace("/", "_")
        with open(os.path.join(RESULTS_DIR, f"{safe_name}.md"), "w", encoding="utf-8") as f:
            f.write(f"# {model_id}\n\n")
            f.write(f"- Time: {elapsed:.1f}s\n")
            f.write(f"- Input tokens: {usage.prompt_tokens:,}\n")
            f.write(f"- Output tokens: {usage.completion_tokens:,}\n")
            f.write(f"- Total tokens: {usage.total_tokens:,}\n\n")
            f.write("## Response\n\n")
            f.write(result)

        print(f"Saved to test_results/{safe_name}.md")

    except Exception as e:
        elapsed = time.time() - start
        print(f"ERROR after {elapsed:.1f}s: {e}")
        safe_name = model_id.replace("/", "_")
        with open(os.path.join(RESULTS_DIR, f"{safe_name}.md"), "w", encoding="utf-8") as f:
            f.write(f"# {model_id}\n\nERROR: {e}\n")

    # Small delay between models
    time.sleep(2)

print("\n\nDone! Results in test_results/")
