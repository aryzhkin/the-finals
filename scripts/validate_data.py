"""
Validate existing AI pipeline data for completeness and integrity.
Checks:
1. idx coverage: do Stage 1 and Stage 2 outputs match the source reviews?
2. Entity name matching: what entities from AI output are unmatched in game_entities?
3. Category distribution sanity check.
"""

import json
import os
from collections import Counter

# --- Paths ---
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")

# --- Files ---
REVIEWS_FILE = os.path.join(DATA_DIR, "reviews_all.json")
STAGE1_FILE = os.path.join(DATA_DIR, "reviews_ai_classified.json")
STAGE2_FILE = os.path.join(DATA_DIR, "reviews_issues.json")
ENTITIES_FILE = os.path.join(DATA_DIR, "game_entities.json")

print("=" * 60)
print("DATA VALIDATION REPORT")
print("=" * 60)

# --- Load ---
print("\nLoading data...")
with open(REVIEWS_FILE, encoding="utf-8") as f:
    source_data = json.load(f)
source = source_data["reviews"] if isinstance(source_data, dict) and "reviews" in source_data else source_data
print(f"  Source reviews: {len(source):,}")

with open(STAGE1_FILE, encoding="utf-8") as f:
    stage1 = json.load(f)
print(f"  Stage 1 (classified): {len(stage1):,}")

with open(STAGE2_FILE, encoding="utf-8") as f:
    stage2 = json.load(f)
print(f"  Stage 2 (issues): {len(stage2):,}")

with open(ENTITIES_FILE, encoding="utf-8") as f:
    entities = json.load(f)

# Build canonical entity set from game_entities.json
canonical = set()
for cat, val in entities.items():
    if cat == "aliases":
        # aliases maps lowercase → canonical name
        for alias, target in val.items():
            canonical.add(alias.lower())
            if "context-dependent" not in target.lower() and " or " not in target:
                canonical.add(target.lower())
    elif isinstance(val, dict):
        for subkey, subval in val.items():
            if isinstance(subval, list):
                # weapons.Light = ["93R", "ARN-220", ...]
                for name in subval:
                    canonical.add(name.lower())
            elif isinstance(subval, dict):
                # classes.Light = {hp: 150, ...}
                canonical.add(subkey.lower())
            elif isinstance(subval, str):
                # game_modes.Cashout = "description"
                canonical.add(subkey.lower())
    elif isinstance(val, list):
        # arenas = ["Bernal", ...]
        for name in val:
            canonical.add(name.lower())

print(f"  Game entities: {len(canonical)} canonical names+aliases")

# --- 1. Coverage Validation ---
print("\n" + "=" * 60)
print("1. COVERAGE VALIDATION")
print("=" * 60)

# Stage 1
if len(stage1) == len(source):
    print(f"\n  Stage 1 count matches source: {len(stage1):,} ✓")
else:
    print(f"\n  Stage 1 count MISMATCH: {len(stage1):,} vs source {len(source):,} ✗")

classified = sum(1 for r in stage1 if r.get("categories") or r.get("ai_categories"))
empty_cats = len(stage1) - classified
print(f"  Classified: {classified:,} ({classified/len(stage1)*100:.1f}%)")
print(f"  Empty categories: {empty_cats:,} ({empty_cats/len(stage1)*100:.1f}%)")

# Confidence distribution
conf_dist = Counter()
for r in stage1:
    conf_dist[r.get("confidence", "none")] += 1
print(f"  Confidence: {dict(conf_dist)}")

# Stage 2
if len(stage2) == len(source):
    print(f"\n  Stage 2 count matches source: {len(stage2):,} ✓")
else:
    print(f"\n  Stage 2 count MISMATCH: {len(stage2):,} vs source {len(source):,} ✗")

with_issues = sum(1 for r in stage2 if r.get("issues"))
total_issues = sum(len(r.get("issues", [])) for r in stage2)
print(f"  Reviews with issues: {with_issues:,} ({with_issues/len(stage2)*100:.1f}%)")
print(f"  Total issues: {total_issues:,} (avg {total_issues/len(stage2):.2f}/review)")

# Timestamp alignment
mismatched = 0
sample_mismatches = []
for i in range(min(len(stage1), len(stage2))):
    ts1 = stage1[i].get("timestamp")
    ts2 = stage2[i].get("timestamp")
    if ts1 and ts2 and ts1 != ts2:
        mismatched += 1
        if len(sample_mismatches) < 5:
            sample_mismatches.append((i, ts1, ts2))
print(f"\n  Timestamp alignment (Stage1 vs Stage2): {mismatched} mismatches out of {min(len(stage1), len(stage2)):,}")
if mismatched == 0:
    print("  All timestamps match ✓")
else:
    print(f"  WARNING: {mismatched} misaligned ✗")
    for idx, ts1, ts2 in sample_mismatches:
        print(f"    idx={idx}: Stage1={ts1}, Stage2={ts2}")

# --- 2. Entity Name Validation ---
print("\n" + "=" * 60)
print("2. ENTITY NAME VALIDATION")
print("=" * 60)

entity_mentions = Counter()
unmatched_entities = Counter()
for r in stage2:
    for iss in r.get("issues", []):
        for ent in iss.get("entities", []):
            entity_mentions[ent] += 1
            if ent.lower() not in canonical:
                unmatched_entities[ent] += 1

total_mentions = sum(entity_mentions.values())
matched_mentions = total_mentions - sum(unmatched_entities.values())
print(f"\n  Total entity mentions: {total_mentions:,}")
print(f"  Matched to canonical: {matched_mentions:,} ({matched_mentions/total_mentions*100:.1f}%)")
print(f"  Unmatched: {sum(unmatched_entities.values()):,} ({sum(unmatched_entities.values())/total_mentions*100:.1f}%)")
print(f"  Unique unmatched names: {len(unmatched_entities)}")

if unmatched_entities:
    print(f"\n  Top 30 unmatched entities (candidates for aliases):")
    for ent, count in unmatched_entities.most_common(30):
        print(f"    {ent:40s} {count:>5,} mentions")

# --- 3. Category Distribution ---
print("\n" + "=" * 60)
print("3. CATEGORY DISTRIBUTION")
print("=" * 60)

cat_dist = Counter()
for r in stage1:
    for cat in (r.get("categories") or r.get("ai_categories") or []):
        cat_dist[cat] += 1

print(f"\n  Unique categories: {len(cat_dist)}")
for cat, count in cat_dist.most_common():
    print(f"    {cat:45s} {count:>7,}")

print("\n" + "=" * 60)
print("VALIDATION COMPLETE")
print("=" * 60)
