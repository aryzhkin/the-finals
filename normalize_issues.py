"""
Post-processing normalization for Stage 2 issue texts.
Merges duplicate/near-duplicate issue texts by:
1. Prefix normalization (case, synonyms)
2. Suffix normalization (synonym groups)
3. Saves normalized data back to reviews_issues.json
"""

import json
import re
from collections import Counter

INPUT_FILE = "reviews_issues.json"
OUTPUT_FILE = "reviews_issues.json"  # overwrite in place

# --- 1. PREFIX MERGES ---
# Map variant prefixes to canonical form (case-insensitive matching)
PREFIX_MERGES = {
    # Class variants
    "light class": "Light",
    "light class op": "Light",
    "light class issues": "Light",
    "heavy class": "Heavy",
    "heavy class op": "Heavy",
    "heavy class issues": "Heavy",
    "medium class": "Medium",
    "medium class op": "Medium",
    "medium class issues": "Medium",

    # Game design variants
    "game design direction": "Game design",
    "game design": "Game design",

    # Game mode variants
    "game mode": "Game modes",
    "game modes": "Game modes",
    "game mode complaints": "Game modes",

    # Movement variants
    "movement & combat feel": "Movement",
    "movement & feel": "Movement",

    # Cheating variants
    "cheating & anti-cheat": "Cheating",

    # Servers
    "server performance & connectivity": "Servers",
    "server": "Servers",
    "server performance": "Servers",

    # Bug variants
    "bugs & technical issues": "Bugs",
    "bugs": "Bugs",
    "bug": "Bugs",
    "technical issues": "Bugs",

    # Performance
    "performance & optimization": "Performance",
    "performance": "Performance",
    "optimization": "Performance",

    # Weapon / gadget general
    "weapon imbalance": "Weapon balance",
    "weapon balance": "Weapon balance",
    "gadget imbalance": "Gadget balance",
    "gadget balance": "Gadget balance",

    # Matchmaking
    "matchmaking issues": "Matchmaking",

    # Content
    "content drought": "Content",

    # Player behavior
    "player behavior & toxicity": "Player behavior",
    "player behavior": "Player behavior",
    "players": "Player behavior",

    # Monetization
    "monetization & battle pass": "Monetization",
    "monetization": "Monetization",
    "battle pass": "Monetization",

    # Crossplay
    "crossplay & input balance": "Crossplay",
    "crossplay": "Crossplay",

    # Teamwork
    "teamwork & coordination": "Teamwork",
    "teamwork": "Teamwork",

    # Sound
    "audio & sound design": "Audio",
    "audio": "Audio",
    "sound & music": "Audio",
    "sound": "Audio",

    # Graphics
    "graphics & visuals": "Graphics",
    "visuals": "Graphics",

    # F2P
    "free-to-play value": "F2P",
    "free-to-play": "F2P",
    "f2p model": "F2P",

    # Destruction
    "destructible environments": "Destruction",
    "environment destruction": "Destruction",

    # Class system
    "class system & abilities": "Class system",
    "class system": "Class system",

    # Weapon variety
    "weapon & gadget variety": "Weapon variety",
    "weapon variety": "Weapon variety",

    # Region
    "region lock & china": "Region lock",
    "region lock": "Region lock",
    "region": "Region lock",

    # Linux
    "linux / steam deck": "Linux",
    "linux": "Linux",
    "steam deck": "Linux",

    # Progression
    "progression & rank resets": "Progression",
    "progression": "Progression",
    "rank": "Ranked",

    # Map
    "map design": "Maps",
    "maps": "Maps",
    "map": "Maps",

    # Revive
    "revive & respawn mechanics": "Respawn",
    "revive": "Respawn",
    "respawn": "Respawn",

    # Spawn
    "spawn points": "Spawns",
    "spawn": "Spawns",

    # ToS
    "terms of service": "ToS",
    "tos": "ToS",

    # Third-partying
    "third-partying": "Third-partying",
    "third partying": "Third-partying",
    "third party": "Third-partying",

    # AI voice
    "ai voice acting": "AI voices",
    "ai voice": "AI voices",
    "ai voices": "AI voices",

    # Low playerbase
    "low playerbase": "Playerbase",
    "playerbase": "Playerbase",
    "dead game": "Playerbase",

    # Game balance
    "game balance": "Game balance",
    "balance": "Game balance",

    # Account
    "account": "Account",
}

# --- 2. SUFFIX SYNONYM GROUPS ---
# Map variant suffixes to canonical form
SUFFIX_MERGES = {
    # Positive synonyms → canonical (only for gameplay/mode/class contexts)
    # NOTE: "good", "great" etc. handled contextually in normalize_text()
    "chaotic and fun": "chaotic",
    "chaotic fun": "chaotic",
    "fun chaos": "chaotic",
    "pure chaos": "chaotic",

    "fun and addictive": "fun and addictive",  # keep as is (dominant)

    "beautiful": "beautiful",
    "looks great": "beautiful",
    "looks good": "beautiful",
    "good looking": "beautiful",
    "well polished": "beautiful",
    "stunning": "beautiful",
    "gorgeous": "beautiful",
    "pretty": "beautiful",
    "good visuals": "beautiful",
    "visually appealing": "beautiful",

    "good feel": "satisfying",
    "feels great": "satisfying",
    "feels good": "satisfying",
    "well done": "satisfying",

    # Negative synonyms → canonical
    "terrible": "bad",
    "awful": "bad",
    "horrible": "bad",
    "garbage": "bad",
    "trash": "bad",
    "worst": "bad",
    "poor": "bad",
    "atrocious": "bad",
    "abysmal": "bad",

    "too strong": "overpowered",
    "op": "overpowered",
    "broken": "overpowered",  # in balance context
    "needs nerf": "overpowered",
    "no skill required": "overpowered",
    "no skill": "overpowered",

    "too weak": "underpowered",
    "useless": "underpowered",
    "needs buff": "underpowered",
    "nerfed": "underpowered",
    "overnerfed": "underpowered",
    "nerfed too hard": "underpowered",
    "weak": "underpowered",

    # NOTE: "unfun to play against" mappings are context-dependent,
    # handled in normalize_text() via PLAY_AGAINST_PREFIXES

    "unfun": "unfun",
    "not fun": "unfun",
    "boring": "unfun",
    "stale": "unfun",
    "tedious": "unfun",

    "no counterplay": "no counterplay",
    "uncounterable": "no counterplay",
    "no counter": "no counterplay",
    "can't counter": "no counterplay",

    # Infrastructure
    "lag": "high latency",
    "laggy": "high latency",
    "lags": "high latency",
    "lagging": "high latency",
    "latency": "high latency",
    "ping": "high latency",
    "high ping": "high latency",
    "rubber banding": "high latency",

    "crash": "crashes",
    "crashing": "crashes",
    "unstable": "crashes",
    "game crashes": "crashes",
    "game keeps crashing": "crashes",
    "frequent crashes": "crashes",
    "game crash": "crashes",
    "keeps crashing": "crashes",
    "constant crashes": "crashes",

    "disconnect": "disconnects",
    "disconnecting": "disconnects",
    "kicked": "disconnects",
    "connection issues": "disconnects",

    "game won't launch": "won't launch",
    "game not launching": "won't launch",
    "won't start": "won't launch",
    "doesn't launch": "won't launch",
    "not launching": "won't launch",
    "can't launch": "won't launch",

    # Cheating
    "hackers": "too many cheaters",
    "hacker": "too many cheaters",
    "rampant": "too many cheaters",
    "full of cheaters": "too many cheaters",
    "infested": "too many cheaters",
    "cheater": "too many cheaters",
    "cheaters": "too many cheaters",
    "cheaters everywhere": "too many cheaters",

    "ineffective anti-cheat": "ineffective",
    "anti-cheat ineffective": "ineffective",
    "anti-cheat is ineffective": "ineffective",
    "no anti-cheat": "ineffective",
    "aimbot": "aimbots",

    # Account
    "banned unfairly": "unfair ban",
    "banned for no reason": "unfair ban",
    "banned without reason": "unfair ban",
    "falsely banned": "unfair ban",
    "wrongly banned": "unfair ban",
    "false bans": "unfair ban",
    "false ban": "unfair ban",
    "unfair ban": "unfair ban",
    "banned": "banned",

    # Matchmaking — NOTE: these only apply contextually, handled in normalize_text()
    "unbalanced teams": "skill disparity",
    "unfair teams": "skill disparity",

    # Content
    "needs more maps": "needs more maps",
    "needs more modes": "needs more modes",
    "needs more game modes": "needs more modes",
    "needs more weapons": "needs more weapons",
    "needs more content": "needs more content",
    "needs more updates": "needs more content",
    "needs more": "needs more content",
    "game feels stale": "needs more content",
    "lack of content": "needs more content",
    "not enough content": "needs more content",

    # Performance
    "poorly optimized": "poor optimization",
    "unoptimized": "poor optimization",
    "not optimized": "poor optimization",
    "fps drops": "low FPS",
    "frame drops": "low FPS",
    "low fps": "low FPS",
    "fps issues": "low FPS",
    "frame rate drops": "low FPS",

    # Movement
    "slow": "too slow",
    "sluggish": "too slow",
    "janky": "clunky",
    "feels clunky": "clunky",

    # Bugs
    "buggy": "too many",
    "too many bugs": "too many",
    "lots of bugs": "too many",

    # Localization merges
    "localization for russian": "Russian localization",
    "russian language": "Russian localization",
    "russian localization": "Russian localization",
    "russian subtitles": "Russian localization",
}

# --- 3. FULL TEXT OVERRIDES ---
# For specific complete texts that need exact remapping
FULL_TEXT_OVERRIDES = {
    "Light Class: overpowered": "Light: overpowered",
    "Heavy Class: overpowered": "Heavy: overpowered",
    "Medium Class: overpowered": "Medium: overpowered",
    "Light Class: underpowered": "Light: underpowered",
    "Heavy Class: underpowered": "Heavy: underpowered",
    "Medium Class: underpowered": "Medium: underpowered",

    # Add/Content merges
    "Add: more game modes": "Add: more modes",
    "Add: new game modes": "Add: more modes",
    "Content: needs more game modes": "Add: more modes",
    "Content: needs more modes": "Add: more modes",
    "Game Modes: needs more": "Add: more modes",
    "Game Modes: needs more variety": "Add: more modes",
    "Content: needs more maps": "Add: more maps",
    "Content: needs more weapons": "Add: more weapons",
    "Add: new weapons": "Add: more weapons",
    "Content: needs more": "Add: more content",
    "Content: needs more updates": "Add: more content",
    "Content: needs more content": "Add: more content",
    "Content: game feels stale": "Add: more content",

    # Fix/suggestion merges
    "Fix: anti-cheat": "Fix: anti-cheat",
    "Anti-cheat: ineffective": "Anti-cheat: ineffective",

    # Localization
    "Add: localization for Russian": "Add: Russian localization",
    "Add: Russian language": "Add: Russian localization",
    "Add: Russian localization": "Add: Russian localization",
}


# Prefixes where "good/great/amazing" → "fun" is appropriate
FUN_PREFIXES = {
    "Gameplay", "Game modes", "3v3v3v3 Format", "Cashout", "Power Shift",
    "Quick Cash", "Bank It", "Terminal Attack", "Team Deathmatch", "Head2Head",
    "Point Break", "Ranked Cashout", "Light", "Medium", "Heavy",
    "Class system",
}

# Prefixes where "good/great" → "satisfying" is appropriate
SATISFYING_PREFIXES = {
    "Gunplay", "Movement", "Destruction", "Audio",
}

# Prefixes where "good/great" → "beautiful" is appropriate
BEAUTIFUL_PREFIXES = {
    "Graphics",
}

# Prefixes where "good/great" → "good" (keep as is)
GOOD_PREFIXES = {
    "Performance", "F2P", "Weapon variety", "Teamwork",
}

# Prefixes where "unbalanced/unfair" → "skill disparity"
SKILL_DISPARITY_PREFIXES = {
    "Matchmaking",
}

# Prefixes where "annoying/frustrating/cancer" → "unfun to play against"
# Only for combat entities (classes, weapons, gadgets), NOT for AI voices, Audio, etc.
PLAY_AGAINST_PREFIXES = {
    "Light", "Medium", "Heavy",
    "Sword", "Cloaking Device", "Mesh Shield", "Dome Shield",
    "RPG", "Throwing Knives", "Stun Gun", "Glitch Grenade",
    "Thermal Vision", "APS Turret", "Barricade", "C4",
    "Goo Gun", "Goo Grenade", "Pyro Grenade", "Smoke Grenade",
    "Gateway", "Winch Claw", "Charge 'N' Slam", "Dual Blades",
    "Third-partying", "Gadget balance",
}


def normalize_text(text):
    """Normalize an issue text string."""
    # 1. Check full text overrides first
    if text in FULL_TEXT_OVERRIDES:
        return FULL_TEXT_OVERRIDES[text]

    # 2. Split into prefix:suffix
    if ": " not in text:
        return text

    prefix, suffix = text.split(": ", 1)

    # 3. Normalize prefix
    prefix_lower = prefix.lower().strip()
    if prefix_lower in PREFIX_MERGES:
        prefix = PREFIX_MERGES[prefix_lower]
    else:
        # Fix case: capitalize first letter
        prefix = prefix[0].upper() + prefix[1:] if prefix else prefix

    # 4. Normalize suffix (generic mappings)
    suffix_lower = suffix.lower().strip()
    if suffix_lower in SUFFIX_MERGES:
        suffix = SUFFIX_MERGES[suffix_lower]

    # 5. Context-dependent suffix normalization
    suffix_lower = suffix.lower().strip()
    if suffix_lower in ("enjoyable", "great", "amazing", "excellent",
                        "awesome", "love it", "the best", "fantastic",
                        "incredible", "wonderful", "brilliant"):
        if prefix in FUN_PREFIXES:
            suffix = "fun"
        elif prefix in SATISFYING_PREFIXES:
            suffix = "satisfying"
        elif prefix in BEAUTIFUL_PREFIXES:
            suffix = "beautiful"
        elif prefix in GOOD_PREFIXES:
            suffix = "good"
    elif suffix_lower == "good":
        if prefix in FUN_PREFIXES:
            suffix = "fun"
        elif prefix in SATISFYING_PREFIXES:
            suffix = "satisfying"
        elif prefix in BEAUTIFUL_PREFIXES:
            suffix = "beautiful"
        # else keep "good"
    elif suffix_lower in ("unbalanced", "unfair"):
        if prefix in SKILL_DISPARITY_PREFIXES:
            suffix = "skill disparity"
        # else keep as is
    elif suffix_lower in ("annoying to play against", "frustrating to play against",
                          "cancer", "unfun to play against"):
        if prefix in PLAY_AGAINST_PREFIXES:
            suffix = "unfun to play against"
        else:
            suffix = "annoying"

    return f"{prefix}: {suffix}"


# --- Main ---
print("Loading issues...")
with open(INPUT_FILE, encoding="utf-8") as f:
    data = json.load(f)

# Count before
before_texts = Counter()
for r in data:
    for iss in r.get("issues", []):
        before_texts[iss.get("text", "")] += 1

print(f"Before: {len(before_texts):,} unique issue texts")

# Apply normalization
changes = 0
for r in data:
    for iss in r.get("issues", []):
        old = iss.get("text", "")
        new = normalize_text(old)
        if new != old:
            iss["text"] = new
            changes += 1

# Count after
after_texts = Counter()
for r in data:
    for iss in r.get("issues", []):
        after_texts[iss.get("text", "")] += 1

print(f"After:  {len(after_texts):,} unique issue texts")
print(f"Changes made: {changes:,}")
print(f"Texts reduced by: {len(before_texts) - len(after_texts):,} ({(1 - len(after_texts)/len(before_texts))*100:.1f}%)")

# Save
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False)
print(f"\nSaved to {OUTPUT_FILE}")

# Show top results after normalization
print("\n=== TOP 30 COMPLAINTS (after normalization) ===")
complaints = Counter()
suggestions = Counter()
praise_c = Counter()
for r in data:
    for iss in r.get("issues", []):
        t = iss.get("type", "")
        text = iss.get("text", "")
        if t == "complaint":
            complaints[text] += 1
        elif t == "suggestion":
            suggestions[text] += 1
        elif t == "praise":
            praise_c[text] += 1

for text, count in complaints.most_common(30):
    print(f"  {count:>6,}  {text}")

print("\n=== TOP 20 SUGGESTIONS (after normalization) ===")
for text, count in suggestions.most_common(20):
    print(f"  {count:>6,}  {text}")

print("\n=== TOP 20 PRAISE (after normalization) ===")
for text, count in praise_c.most_common(20):
    print(f"  {count:>6,}  {text}")

print(f"\nUnique complaints: {len(complaints):,}")
print(f"Unique suggestions: {len(suggestions):,}")
print(f"Unique praise: {len(praise_c):,}")
