"""
Prepare pre-aggregated data for the static HTML dashboard.
Reads reviews_classified.json (for structural data) + reviews_issues.json
(AI categories + extracted issues) + patch_notes.json.
Categories come from AI Stage 1 classification, NOT regex.
"""

import ast
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone


# Mapping from AI Stage 1 categories to Stage 2 issue prefixes
# Used to show related specific issues in Category Deep-Dive
CATEGORY_ISSUE_PREFIXES = {
    # Negative categories
    "Cheating & Anti-Cheat": ["Cheating", "Anti-cheat"],
    "Matchmaking": ["Matchmaking"],
    "Server Performance & Connectivity": ["Servers"],
    "Weapon Imbalance": ["Weapon balance"],
    "Game Design Direction": ["Game design", "Game balance"],
    "Bugs & Technical Issues": ["Bugs"],
    "Performance & Optimization": ["Performance"],
    "Player Behavior & Toxicity": ["Player behavior"],
    "Game Mode Complaints": ["Game modes"],
    "Monetization & Battle Pass": ["Monetization"],
    "Light Class OP": ["Light"],
    "Light Class Issues": ["Light"],
    "Heavy Class OP": ["Heavy"],
    "Heavy Class Issues": ["Heavy"],
    "Medium Class OP": ["Medium"],
    "Medium Class Issues": ["Medium"],
    "Movement & Feel": ["Movement"],
    "Gadget Imbalance": ["Gadget balance"],
    "Region Lock & China": ["Region lock"],
    "Map Design": ["Maps"],
    "AI Voice Acting": ["AI voices"],
    "Progression & Rank Resets": ["Progression", "Ranked"],
    "Content Drought": ["Content", "Add"],
    "Crossplay & Input Balance": ["Crossplay"],
    "Low Playerbase": ["Playerbase"],
    "Third-Partying": ["Third-partying"],
    "Revive & Respawn Mechanics": ["Respawn"],
    "Spawn Points": ["Spawns"],
    "Linux / Steam Deck": ["Linux"],
    "Terms of Service": ["ToS"],
    "Audio & Sound Design": ["Audio"],
    # Positive categories
    "Fun & Addictive": ["Gameplay"],
    "Movement & Combat Feel": ["Movement", "Gunplay"],
    "Destructible Environments": ["Destruction"],
    "Teamwork & Coordination": ["Teamwork"],
    "Game Modes": ["Game modes", "Cashout", "Power Shift"],
    "Graphics & Visuals": ["Graphics"],
    "Free-to-Play Value": ["F2P"],
    "Class System & Abilities": ["Class system"],
    "Weapon & Gadget Variety": ["Weapon variety"],
    "Sound & Music": ["Audio"],
    "3v3v3v3 Format": ["3v3v3v3", "3v3v3v3 Format"],
}


def get_playtime_bracket(hours):
    """Compute playtime bracket from hours played."""
    if hours < 10:
        return "0-10h (Newcomer)"
    elif hours < 50:
        return "10-50h (Casual)"
    elif hours < 100:
        return "50-100h (Regular)"
    elif hours < 200:
        return "100-200h (Dedicated)"
    elif hours < 500:
        return "200-500h (Veteran)"
    elif hours < 1000:
        return "500-1000h (Hardcore)"
    else:
        return "1000h+ (No-lifer)"


def main():
    full_reviews = None  # will be loaded once if reviews_ai_classified.json exists

    print("Loading reviews (structural data)...")
    with open("reviews_classified.json", encoding="utf-8") as f:
        reviews = json.load(f)
    print(f"  {len(reviews)} reviews loaded")

    # Merge Chinese language variants into one
    for r in reviews:
        if r["language"] in ("tchinese", "schinese"):
            r["language"] = "chinese"

    with open("seasons.json", encoding="utf-8") as f:
        seasons_meta = json.load(f)

    with open("categories_final.json", encoding="utf-8") as f:
        cat_defs = json.load(f)
    NEG_CATEGORIES = set(cat_defs.get("negative", {}).keys())
    POS_CATEGORIES = set(cat_defs.get("positive", {}).keys())

    # Precompute season timestamps for consistent boundary logic
    seasons_ts = []
    for s in seasons_meta:
        start_ts = int(datetime.strptime(s["start"], "%Y-%m-%d").replace(
            tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(s["end"], "%Y-%m-%d").replace(
            tzinfo=timezone.utc).timestamp())
        seasons_ts.append((s["season"], start_ts, end_ts))

    # Load Stage 2 AI data (categories + issues)
    issues_data = []
    if os.path.exists("reviews_issues.json"):
        print("Loading AI data (Stage 1 categories + Stage 2 issues)...")
        with open("reviews_issues.json", encoding="utf-8") as f:
            issues_data = json.load(f)
        print(f"  {len(issues_data)} reviews loaded")

        # Verify data alignment
        if len(reviews) != len(issues_data):
            print(f"  WARNING: reviews count mismatch! classified={len(reviews)} issues={len(issues_data)}")
        # Spot-check timestamps to confirm same order
        mismatches = 0
        for check_idx in (0, len(reviews) // 4, len(reviews) // 2, len(reviews) - 1):
            if check_idx < len(issues_data):
                ts_c = reviews[check_idx].get("timestamp", 0)
                ts_i = issues_data[check_idx].get("timestamp", 0)
                if ts_c != ts_i:
                    mismatches += 1
        if mismatches:
            print(f"  WARNING: {mismatches}/4 timestamp spot-checks failed — data may be misaligned!")

        # Replace regex categories with AI categories
        replaced = 0
        for i, r in enumerate(reviews):
            if i < len(issues_data):
                ai_cats = issues_data[i].get("ai_categories", [])
                r["categories"] = ai_cats
                if ai_cats:
                    replaced += 1
        print(f"  {replaced:,} reviews got AI categories")

        # Backfill unclassified reviews from Stage 2 issues
        # If Stage 1 returned no categories but Stage 2 extracted issues,
        # infer categories from issue prefixes via reverse CATEGORY_ISSUE_PREFIXES
        # Split by sentiment to avoid assigning positive categories to negative reviews
        prefix_to_neg_cat = {}
        prefix_to_pos_cat = {}
        for cat, prefixes in CATEGORY_ISSUE_PREFIXES.items():
            target = prefix_to_neg_cat if cat in NEG_CATEGORIES else prefix_to_pos_cat
            for prefix in prefixes:
                target[prefix] = cat
        backfilled = 0
        for i, r in enumerate(reviews):
            if r["categories"] or i >= len(issues_data):
                continue
            issues = issues_data[i].get("issues", [])
            if not issues:
                continue
            inferred = set()
            prefix_map = prefix_to_pos_cat if r["sentiment"] == "positive" else prefix_to_neg_cat
            for iss in issues:
                text = iss.get("text", "")
                prefix = text.split(": ")[0] if ": " in text else ""
                if prefix in prefix_map:
                    inferred.add(prefix_map[prefix])
            if inferred:
                r["categories"] = list(inferred)
                backfilled += 1
        print(f"  {backfilled:,} unclassified reviews backfilled from Stage 2 issues")
    else:
        print("  WARNING: reviews_issues.json not found, using regex categories")

    # Recompute playtime brackets using playtime_at_review (not playtime_forever)
    if os.path.exists("reviews_ai_classified.json"):
        print("Loading full reviews for playtime_at_review...")
        with open("reviews_ai_classified.json", encoding="utf-8") as f:
            full_reviews = json.load(f)
        print(f"  {len(full_reviews)} full reviews loaded")
        recomputed = 0
        for i, r in enumerate(reviews):
            if i >= len(full_reviews):
                break
            author = full_reviews[i].get("author", {})
            if isinstance(author, str):
                author = ast.literal_eval(author)
            pt_at_review = float(author.get("playtime_at_review", 0))
            pt_forever = float(author.get("playtime_forever", 0))
            # Use playtime_at_review if available, else fall back to playtime_forever
            minutes = pt_at_review if pt_at_review > 0 else pt_forever
            hours = round(minutes / 60, 1)
            r["hours"] = hours
            r["playtime_bracket"] = get_playtime_bracket(hours)
            # Extract AI confidence for HC filter
            r["_hc"] = full_reviews[i].get("ai_confidence", "none") in ("high", "medium")
            recomputed += 1
        print(f"  {recomputed:,} reviews recomputed with playtime_at_review")
        # Count updated reviews (timestamp_updated != timestamp_created)
        updated_count = sum(
            1 for r in full_reviews
            if r.get("timestamp_updated", 0) != r.get("timestamp_created", 0)
            and r.get("timestamp_updated", 0) > 0
        )
        print(f"  {updated_count:,} reviews were updated ({round(updated_count / len(full_reviews) * 100, 1)}%)")
    else:
        updated_count = 0
        print("  WARNING: reviews_ai_classified.json not found, using existing playtime brackets")

    # Load patch notes
    patch_notes_data = []
    if os.path.exists("patch_notes.json"):
        print("Loading patch notes...")
        with open("patch_notes.json", encoding="utf-8") as f:
            pn = json.load(f)
            patch_notes_data = pn.get("patches", pn) if isinstance(pn, dict) else pn
        print(f"  {len(patch_notes_data)} patches loaded")

    # Load game entities for class mapping
    entity_classes = {}  # entity_name -> {class, type}
    if os.path.exists("game_entities.json"):
        with open("game_entities.json", encoding="utf-8") as f:
            ge = json.load(f)
        for etype in ("weapons", "specializations", "gadgets"):
            for cls, items in ge.get(etype, {}).items():
                for item in items:
                    entity_classes[item] = {"class": cls, "type": etype.rstrip("s")}
        print(f"  {len(entity_classes)} entity→class mappings loaded")

    # Load confidence stats
    confidence = {}
    if os.path.exists("stage1_stats.json"):
        with open("stage1_stats.json", encoding="utf-8") as f:
            stage1 = json.load(f)
            confidence = stage1.get("confidence", {})

    # -----------------------------------------------------------------------
    # 1. Overview stats
    # -----------------------------------------------------------------------
    pos = [r for r in reviews if r["sentiment"] == "positive"]
    neg = [r for r in reviews if r["sentiment"] == "negative"]

    classified = sum(1 for r in reviews if r.get("categories"))
    overview = {
        "total": len(reviews),
        "positive": len(pos),
        "negative": len(neg),
        "classified": classified,
        "unclassified": len(reviews) - classified,
        "languages": len(set(r["language"] for r in reviews)),
        "updated": updated_count,
        "confidence": confidence,
    }

    # -----------------------------------------------------------------------
    # 2. Category rankings (overall)
    # -----------------------------------------------------------------------
    neg_cats = Counter()
    pos_cats = Counter()
    for r in neg:
        for c in r["categories"]:
            neg_cats[c] += 1
    for r in pos:
        for c in r["categories"]:
            pos_cats[c] += 1

    neg_unclassified = sum(1 for r in neg if not r["categories"])
    pos_unclassified = sum(1 for r in pos if not r["categories"])

    category_rankings = {
        "negative": {
            "categories": dict(neg_cats.most_common()),
            "total": len(neg),
            "unclassified": neg_unclassified,
        },
        "positive": {
            "categories": dict(pos_cats.most_common()),
            "total": len(pos),
            "unclassified": pos_unclassified,
        },
    }

    # -----------------------------------------------------------------------
    # 3. Season health data + category × season × playtime cross-data
    # -----------------------------------------------------------------------
    BRACKET_LABEL = {
        "0-10h (Newcomer)": "Newcomer",
        "10-50h (Casual)": "Casual",
        "50-100h (Regular)": "Regular",
        "100-200h (Dedicated)": "Dedicated",
        "200-500h (Veteran)": "Veteran",
        "500-1000h (Hardcore)": "Hardcore",
        "1000h+ (No-lifer)": "No-lifer",
    }

    season_data = {}
    cat_season_pt_neg = defaultdict(lambda: defaultdict(Counter))
    cat_season_pt_pos = defaultdict(lambda: defaultdict(Counter))
    for r in reviews:
        s = r["season"]
        if s not in season_data:
            season_data[s] = {"positive": 0, "negative": 0,
                              "neg_cats": Counter(), "pos_cats": Counter()}
        bracket = BRACKET_LABEL.get(r.get("playtime_bracket", ""), "")
        if r["sentiment"] == "positive":
            season_data[s]["positive"] += 1
            for c in r["categories"]:
                season_data[s]["pos_cats"][c] += 1
                if bracket:
                    cat_season_pt_pos[c][s][bracket] += 1
        else:
            season_data[s]["negative"] += 1
            for c in r["categories"]:
                season_data[s]["neg_cats"][c] += 1
                if bracket:
                    cat_season_pt_neg[c][s][bracket] += 1

    season_health = {}
    for s, d in season_data.items():
        total = d["positive"] + d["negative"]
        season_health[s] = {
            "total": total,
            "positive": d["positive"],
            "negative": d["negative"],
            "approval": round(d["positive"] / total * 100, 1) if total else 0,
            "all_negative": dict(Counter(d["neg_cats"]).most_common()),
            "all_positive": dict(Counter(d["pos_cats"]).most_common()),
            "top_negative": dict(Counter(d["neg_cats"]).most_common(10)),
            "top_positive": dict(Counter(d["pos_cats"]).most_common(10)),
        }

    # -----------------------------------------------------------------------
    # 4. Monthly timeline (for review bombing & trends)
    # -----------------------------------------------------------------------
    monthly = defaultdict(lambda: {"positive": 0, "negative": 0})
    for r in reviews:
        monthly[r["month"]][r["sentiment"]] += 1

    # Daily for review bombing detection (with season + categories)
    daily = defaultdict(lambda: {"positive": 0, "negative": 0, "season": ""})
    daily_cats = defaultdict(Counter)  # day → {category: count} (negative only)
    for r in reviews:
        day = datetime.fromtimestamp(r["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d")
        daily[day][r["sentiment"]] += 1
        if not daily[day]["season"]:
            daily[day]["season"] = r["season"]
        if r["sentiment"] == "negative":
            for c in r["categories"]:
                daily_cats[day][c] += 1

    # -----------------------------------------------------------------------
    # HC overlay: key metrics for high+medium confidence reviews only
    # -----------------------------------------------------------------------
    hc_data = None
    hc_count = sum(1 for r in reviews if r.get("_hc"))
    if hc_count > 0 and hc_count < len(reviews):
        hc_reviews = [r for r in reviews if r.get("_hc")]
        hc_pos = [r for r in hc_reviews if r["sentiment"] == "positive"]
        hc_neg = [r for r in hc_reviews if r["sentiment"] == "negative"]
        print(f"  HC filter: {hc_count:,} reviews ({round(hc_count / len(reviews) * 100, 1)}%)")

        # HC overview
        hc_overview = {
            "total": len(hc_reviews),
            "positive": len(hc_pos),
            "negative": len(hc_neg),
        }

        # HC category rankings
        hc_neg_cats = Counter()
        hc_pos_cats = Counter()
        for r in hc_neg:
            for c in r["categories"]:
                hc_neg_cats[c] += 1
        for r in hc_pos:
            for c in r["categories"]:
                hc_pos_cats[c] += 1
        hc_category_rankings = {
            "negative": {
                "categories": dict(hc_neg_cats.most_common()),
                "total": len(hc_neg),
            },
            "positive": {
                "categories": dict(hc_pos_cats.most_common()),
                "total": len(hc_pos),
            },
        }

        # HC season health
        hc_season_data = {}
        for r in hc_reviews:
            s = r["season"]
            if s not in hc_season_data:
                hc_season_data[s] = {"positive": 0, "negative": 0,
                                     "neg_cats": Counter(), "pos_cats": Counter()}
            if r["sentiment"] == "positive":
                hc_season_data[s]["positive"] += 1
                for c in r["categories"]:
                    hc_season_data[s]["pos_cats"][c] += 1
            else:
                hc_season_data[s]["negative"] += 1
                for c in r["categories"]:
                    hc_season_data[s]["neg_cats"][c] += 1
        hc_season_health = {}
        for s in season_health:  # ensure all seasons present
            d = hc_season_data.get(s, {"positive": 0, "negative": 0,
                                       "neg_cats": Counter(), "pos_cats": Counter()})
            total = d["positive"] + d["negative"]
            hc_season_health[s] = {
                "total": total,
                "positive": d["positive"],
                "negative": d["negative"],
                "approval": round(d["positive"] / total * 100, 1) if total else 0,
                "all_negative": dict(Counter(d["neg_cats"]).most_common()),
                "all_positive": dict(Counter(d["pos_cats"]).most_common()),
                "top_negative": dict(Counter(d["neg_cats"]).most_common(10)),
                "top_positive": dict(Counter(d["pos_cats"]).most_common(10)),
            }

        # HC monthly timeline
        hc_monthly = defaultdict(lambda: {"positive": 0, "negative": 0})
        for r in hc_reviews:
            hc_monthly[r["month"]][r["sentiment"]] += 1

        hc_data = {
            "overview": hc_overview,
            "category_rankings": hc_category_rankings,
            "season_health": hc_season_health,
            "monthly_timeline": dict(sorted(hc_monthly.items())),
        }

    # -----------------------------------------------------------------------
    # 5. Playtime data (player journey)
    # -----------------------------------------------------------------------
    brackets_order = [
        "0-10h (Newcomer)", "10-50h (Casual)", "50-100h (Regular)",
        "100-200h (Dedicated)", "200-500h (Veteran)",
        "500-1000h (Hardcore)", "1000h+ (No-lifer)",
    ]
    playtime_data = {}
    for br in brackets_order:
        br_reviews = [r for r in reviews if r["playtime_bracket"] == br]
        br_pos = [r for r in br_reviews if r["sentiment"] == "positive"]
        br_neg = [r for r in br_reviews if r["sentiment"] == "negative"]
        total = len(br_reviews)
        neg_cats_br = Counter()
        pos_cats_br = Counter()
        for r in br_neg:
            for c in r["categories"]:
                neg_cats_br[c] += 1
        for r in br_pos:
            for c in r["categories"]:
                pos_cats_br[c] += 1
        playtime_data[br] = {
            "total": total,
            "positive": len(br_pos),
            "negative": len(br_neg),
            "approval": round(len(br_pos) / total * 100, 1) if total else 0,
            "top_negative": dict(neg_cats_br.most_common(10)),
            "top_positive": dict(pos_cats_br.most_common(10)),
        }

    # -----------------------------------------------------------------------
    # 6. Category by playtime heatmap (neg and pos)
    # -----------------------------------------------------------------------
    cat_playtime_neg = defaultdict(lambda: defaultdict(int))
    cat_playtime_pos = defaultdict(lambda: defaultdict(int))
    for r in neg:
        for c in r["categories"]:
            cat_playtime_neg[c][r["playtime_bracket"]] += 1
    for r in pos:
        for c in r["categories"]:
            cat_playtime_pos[c][r["playtime_bracket"]] += 1

    # -----------------------------------------------------------------------
    # 6b. Cohort data: season × playtime → approval + top categories
    # -----------------------------------------------------------------------
    cohort_raw = defaultdict(lambda: defaultdict(lambda: {
        "positive": 0, "negative": 0, "neg_cats": Counter()
    }))
    for r in reviews:
        cohort_raw[r["season"]][r["playtime_bracket"]]["positive" if r["sentiment"] == "positive" else "negative"] += 1
        if r["sentiment"] == "negative":
            for c in r["categories"]:
                cohort_raw[r["season"]][r["playtime_bracket"]]["neg_cats"][c] += 1

    cohort_data = {}
    for season in cohort_raw:
        cohort_data[season] = {}
        for bracket in cohort_raw[season]:
            d = cohort_raw[season][bracket]
            total = d["positive"] + d["negative"]
            if total < 5:
                continue
            cohort_data[season][bracket] = {
                "total": total,
                "positive": d["positive"],
                "negative": d["negative"],
                "approval": round(d["positive"] / total * 100, 1),
                "top_neg": dict(d["neg_cats"].most_common(5)),
            }

    # -----------------------------------------------------------------------
    # 7. Regional data (top languages)
    # -----------------------------------------------------------------------
    lang_data = defaultdict(lambda: {"positive": 0, "negative": 0,
                                      "neg_cats": Counter(), "pos_cats": Counter()})
    for r in reviews:
        lang = r["language"]
        lang_data[lang][r["sentiment"]] += 1
        if r["sentiment"] == "negative":
            for c in r["categories"]:
                lang_data[lang]["neg_cats"][c] += 1
        else:
            for c in r["categories"]:
                lang_data[lang]["pos_cats"][c] += 1

    regional = {}
    for lang, d in lang_data.items():
        total = d["positive"] + d["negative"]
        if total < 20:
            continue
        regional[lang] = {
            "total": total,
            "positive": d["positive"],
            "negative": d["negative"],
            "approval": round(d["positive"] / total * 100, 1) if total else 0,
            "top_negative": dict(Counter(d["neg_cats"]).most_common(5)),
            "top_positive": dict(Counter(d["pos_cats"]).most_common(5)),
        }

    # -----------------------------------------------------------------------
    # 8. Top voted reviews
    # -----------------------------------------------------------------------
    top_helpful = sorted(reviews, key=lambda r: r["votes_up"], reverse=True)[:100]
    top_funny = sorted(reviews, key=lambda r: r["votes_funny"], reverse=True)[:500]

    def _review_entry(r):
        return {"text": r["text"][:800], "votes_up": r["votes_up"],
                "votes_funny": r["votes_funny"], "sentiment": r["sentiment"],
                "hours": r["hours"], "language": r["language"],
                "season": r["season"], "categories": r["categories"]}

    top_reviews = {
        "most_helpful": [_review_entry(r) for r in top_helpful if r["votes_up"] > 0],
        "most_funny": [_review_entry(r) for r in top_funny if r["votes_funny"] > 0],
    }

    # -----------------------------------------------------------------------
    # 9. Recurring problems (categories that appear in 3+ seasons)
    # -----------------------------------------------------------------------
    cat_by_season = defaultdict(set)
    for r in neg:
        for c in r["categories"]:
            cat_by_season[c].add(r["season"])
    recurring = {cat: len(seasons) for cat, seasons in cat_by_season.items()
                 if len(seasons) >= 3}

    # -----------------------------------------------------------------------
    # 10. Category comparison (same topic praised vs complained)
    # -----------------------------------------------------------------------
    comparison_topics = {
        "Gameplay": {"neg": "Game Design Direction", "pos": "Fun & Addictive"},
        "Movement": {"neg": "Movement & Feel", "pos": "Movement & Combat Feel"},
        "Balance": {"neg": "Weapon Imbalance", "pos": "Weapon & Gadget Variety"},
        "Monetization": {"neg": "Monetization & Battle Pass", "pos": "Free-to-Play Value"},
        "Teamplay": {"neg": "Player Behavior & Toxicity", "pos": "Teamwork & Coordination"},
        "Game Modes": {"neg": "Game Mode Complaints", "pos": "Game Modes"},
    }
    comparisons = {}
    for topic, cats in comparison_topics.items():
        neg_count = neg_cats.get(cats["neg"], 0)
        pos_count = pos_cats.get(cats["pos"], 0)
        # Per-season breakdown
        by_season = {}
        for s, d in season_data.items():
            s_neg_total = d["negative"]
            s_pos_total = d["positive"]
            s_neg_count = d["neg_cats"].get(cats["neg"], 0)
            s_pos_count = d["pos_cats"].get(cats["pos"], 0)
            by_season[s] = {
                "neg_count": s_neg_count,
                "pos_count": s_pos_count,
                "neg_pct": round(s_neg_count / s_neg_total * 100, 1) if s_neg_total else 0,
                "pos_pct": round(s_pos_count / s_pos_total * 100, 1) if s_pos_total else 0,
            }
        # Top 3 example reviews (by votes_up) for each side
        # Filter out ASCII art / spam (low ratio of alphabetic characters)
        def _is_readable(r):
            t = r.get("text", "")
            if len(t) < 20:
                return False
            alpha = sum(1 for c in t if c.isalpha())
            return alpha / len(t) > 0.35
        neg_examples = sorted(
            [r for r in neg if cats["neg"] in r["categories"] and _is_readable(r)],
            key=lambda r: r["votes_up"], reverse=True,
        )[:3]
        pos_examples = sorted(
            [r for r in pos if cats["pos"] in r["categories"] and _is_readable(r)],
            key=lambda r: r["votes_up"], reverse=True,
        )[:3]
        def _review_summary(r):
            return {
                "text": r["text"][:600],
                "votes_up": r["votes_up"],
                "hours": r["hours"],
                "season": r["season"],
                "language": r["language"],
            }
        comparisons[topic] = {
            "neg_category": cats["neg"],
            "pos_category": cats["pos"],
            "neg_count": neg_count,
            "pos_count": pos_count,
            "neg_pct": round(neg_count / len(neg) * 100, 1) if neg else 0,
            "pos_pct": round(pos_count / len(pos) * 100, 1) if pos else 0,
            "by_season": by_season,
            "neg_examples": [_review_summary(r) for r in neg_examples],
            "pos_examples": [_review_summary(r) for r in pos_examples],
        }

    # -----------------------------------------------------------------------
    # 11. Trending & Fixed problems
    # Compare last 2 real seasons' negative category percentages
    # -----------------------------------------------------------------------
    real_seasons_ordered = [s["season"] for s in seasons_meta
                           if s["season"] in season_health
                           and s["season"] != "Off-season"]
    trending = []
    fixed = []
    _trends_curr_season = None
    if len(real_seasons_ordered) >= 2:
        prev_s = real_seasons_ordered[-2]
        curr_s = real_seasons_ordered[-1]
        prev_neg = season_data.get(prev_s, {"negative": 0, "neg_cats": Counter()})
        curr_neg = season_data.get(curr_s, {"negative": 0, "neg_cats": Counter()})
        prev_total = prev_neg["negative"] or 1
        curr_total = curr_neg["negative"] or 1
        all_neg_categories = set(prev_neg["neg_cats"]) | set(curr_neg["neg_cats"])
        deltas = []
        for cat in all_neg_categories:
            prev_pct = prev_neg["neg_cats"].get(cat, 0) / prev_total * 100
            curr_pct = curr_neg["neg_cats"].get(cat, 0) / curr_total * 100
            delta = curr_pct - prev_pct
            deltas.append({
                "category": cat,
                "prev_pct": round(prev_pct, 1),
                "curr_pct": round(curr_pct, 1),
                "delta": round(delta, 1),
                "prev_count": prev_neg["neg_cats"].get(cat, 0),
                "curr_count": curr_neg["neg_cats"].get(cat, 0),
            })
        deltas.sort(key=lambda x: x["delta"], reverse=True)
        trending = [d for d in deltas[:15] if d["delta"] > 0]
        fixed = [d for d in deltas[-15:][::-1] if d["delta"] < 0]

        # Attach top Stage 2 specific issues per trending/fixed category
        # Uses issues_by_season from Stage 2 (computed later, so we defer)
        _trends_curr_season = curr_s

    trends_data = {
        "prev_season": real_seasons_ordered[-2] if len(real_seasons_ordered) >= 2 else "",
        "curr_season": real_seasons_ordered[-1] if real_seasons_ordered else "",
        "trending": trending,
        "fixed": fixed,
    }

    # -----------------------------------------------------------------------
    # 12. Regional deviation from global average
    # -----------------------------------------------------------------------
    global_neg_total = len(neg) or 1
    global_neg_pcts = {cat: count / global_neg_total * 100
                       for cat, count in neg_cats.items()}
    regional_deviation = {}
    for lang, d in lang_data.items():
        total = d["positive"] + d["negative"]
        if total < 50:
            continue
        lang_neg_total = d["negative"] or 1
        deviations = {}
        for cat, _ in neg_cats.most_common(10):  # top 10 global categories
            lang_pct = d["neg_cats"].get(cat, 0) / lang_neg_total * 100
            global_pct = global_neg_pcts.get(cat, 0)
            diff = lang_pct - global_pct
            if abs(diff) >= 3:  # only significant deviations
                deviations[cat] = round(diff, 1)
        if deviations:
            regional_deviation[lang] = deviations

    # -----------------------------------------------------------------------
    # 12b. Cross-category correlation (co-occurrence in negative reviews)
    # -----------------------------------------------------------------------
    from itertools import combinations
    cat_cooccur = Counter()
    for r in neg:
        cats = sorted(set(r.get("categories", [])))
        if len(cats) >= 2:
            for pair in combinations(cats, 2):
                cat_cooccur[pair] += 1
    # Keep only strong links (>= 500 co-occurrences)
    category_correlations = [
        {"cat1": p[0], "cat2": p[1], "count": c,
         "pct1": round(c / neg_cats[p[0]] * 100, 1) if neg_cats[p[0]] else 0,
         "pct2": round(c / neg_cats[p[1]] * 100, 1) if neg_cats[p[1]] else 0}
        for p, c in cat_cooccur.most_common()
        if c >= 500
    ]

    # -----------------------------------------------------------------------
    # 13. Stage 2: AI-extracted issues (complaints, suggestions, praise)
    # -----------------------------------------------------------------------
    top_issues = {}
    issues_by_season = {}
    entity_tracking = {}
    issue_stats = {}
    category_issues = {}  # related issues per AI category

    if issues_data:
        print("Aggregating Stage 2 issues...")

        # Helper: determine season from timestamp (exclusive end, matches classify_reviews.py)
        def get_season(ts):
            for name, start_ts, end_ts in seasons_ts:
                if start_ts <= ts < end_ts:
                    return name
            return "Off-season"

        # Overall issue counts
        all_complaints = Counter()
        all_suggestions = Counter()
        all_praise = Counter()
        # Vote-weighted counters (sum of votes_up per issue)
        weighted_complaints = Counter()
        weighted_suggestions = Counter()
        weighted_praise = Counter()

        # Per-season issue counts
        season_issues = defaultdict(lambda: {
            "complaints": Counter(), "suggestions": Counter(), "praise": Counter()
        })
        # Per-month issue counts (for temporal drill-down)
        month_issues = defaultdict(Counter)

        # Language × category → top complaints (for language drill-down)
        lang_cat_issues = defaultdict(lambda: defaultdict(Counter))

        # Entity tracking: entity -> season -> {complaint, suggestion, praise}
        ent_season = defaultdict(lambda: defaultdict(lambda: Counter()))
        # Entity issues: entity -> type -> Counter(issue_text)
        ent_issues = defaultdict(lambda: defaultdict(Counter))
        # Entity issues by season: entity -> type -> text -> season -> count
        ent_issues_season = defaultdict(lambda: defaultdict(lambda: defaultdict(Counter)))

        total_issues_count = 0
        reviews_with_issues = 0

        for i, r in enumerate(issues_data):
            issues = r.get("issues", [])
            if not issues:
                continue
            reviews_with_issues += 1
            season = get_season(r["timestamp"])
            month = datetime.fromtimestamp(
                r["timestamp"], tz=timezone.utc).strftime("%Y-%m")
            # Get votes_up from full reviews (1:1 indexed)
            votes_up = 0
            if full_reviews and i < len(full_reviews):
                votes_up = int(full_reviews[i].get("votes_up", 0))

            lang = r.get("language", "")

            for iss in issues:
                text = iss.get("text", "")
                itype = iss.get("type", "")
                total_issues_count += 1

                if itype == "complaint":
                    all_complaints[text] += 1
                    weighted_complaints[text] += votes_up
                    season_issues[season]["complaints"][text] += 1
                    month_issues[month][text] += 1
                    if lang:
                        lang_cat_issues[lang]["_all"][text] += 1
                        for cat in r.get("ai_categories", []):
                            lang_cat_issues[lang][cat][text] += 1
                elif itype == "suggestion":
                    all_suggestions[text] += 1
                    weighted_suggestions[text] += votes_up
                    season_issues[season]["suggestions"][text] += 1
                elif itype == "praise":
                    all_praise[text] += 1
                    weighted_praise[text] += votes_up
                    season_issues[season]["praise"][text] += 1

                for entity in iss.get("entities", []):
                    ent_season[entity][season][itype] += 1
                    ent_issues[entity][itype][text] += 1
                    ent_issues_season[entity][itype][text][season] += 1

        # Top issues (overall) — include both count and vote-weighted count
        def issue_list(counter, weighted_counter, n):
            return [{"text": t, "count": c, "weighted": weighted_counter.get(t, 0)}
                    for t, c in counter.most_common(n)]

        top_issues = {
            "complaints": issue_list(all_complaints, weighted_complaints, 50),
            "suggestions": issue_list(all_suggestions, weighted_suggestions, 30),
            "praise": issue_list(all_praise, weighted_praise, 30),
        }

        # Issues by season (top 20 per type per season)
        for season, counters in season_issues.items():
            issues_by_season[season] = {
                "complaints": [{"text": t, "count": c}
                               for t, c in counters["complaints"].most_common(20)],
                "suggestions": [{"text": t, "count": c}
                                for t, c in counters["suggestions"].most_common(15)],
                "praise": [{"text": t, "count": c}
                           for t, c in counters["praise"].most_common(15)],
            }

        # Issues by month (top 10 complaints per month, for temporal drill-down)
        issues_by_month = {}
        for month, counter in month_issues.items():
            top = counter.most_common(10)
            if top:
                issues_by_month[month] = [{"text": t, "count": c} for t, c in top]

        # Per-language top issues (top 10 overall + top 5 per category)
        lang_top_issues = {}
        for lang, cats in lang_cat_issues.items():
            all_counter = cats.get("_all", Counter())
            if all_counter.most_common(1) and all_counter.most_common(1)[0][1] < 5:
                continue
            entry = {
                "top": [{"text": t, "count": c} for t, c in all_counter.most_common(10)],
            }
            # Top 5 issues per top negative categories for this language
            for cat, counter in cats.items():
                if cat == "_all":
                    continue
                top5 = counter.most_common(5)
                if top5 and top5[0][1] >= 3:
                    entry[cat] = [{"text": t, "count": c} for t, c in top5]
            lang_top_issues[lang] = entry

        # Season-to-season issue diffs
        season_diffs = {}
        for i in range(1, len(real_seasons_ordered)):
            prev_s = real_seasons_ordered[i - 1]
            curr_s = real_seasons_ordered[i]
            prev_c = season_issues.get(prev_s, {})
            curr_c = season_issues.get(curr_s, {})
            prev_complaints = prev_c.get("complaints", Counter()) if isinstance(prev_c, dict) else Counter()
            curr_complaints = curr_c.get("complaints", Counter()) if isinstance(curr_c, dict) else Counter()

            new_issues = []
            gone_issues = []
            movers_up = []
            movers_down = []

            all_texts = set(prev_complaints) | set(curr_complaints)
            for text in all_texts:
                pc = prev_complaints.get(text, 0)
                cc = curr_complaints.get(text, 0)
                if pc == 0 and cc >= 5:
                    new_issues.append({"text": text, "count": cc})
                elif cc == 0 and pc >= 5:
                    gone_issues.append({"text": text, "prev_count": pc})
                elif pc >= 5 and cc >= 5:
                    delta_pct = round((cc - pc) / pc * 100)
                    if delta_pct >= 50:
                        movers_up.append({"text": text, "prev": pc, "curr": cc, "delta_pct": delta_pct})
                    elif delta_pct <= -50:
                        movers_down.append({"text": text, "prev": pc, "curr": cc, "delta_pct": delta_pct})

            new_issues.sort(key=lambda x: x["count"], reverse=True)
            gone_issues.sort(key=lambda x: x["prev_count"], reverse=True)
            movers_up.sort(key=lambda x: x["delta_pct"], reverse=True)
            movers_down.sort(key=lambda x: x["delta_pct"])

            season_diffs[curr_s] = {
                "prev": prev_s,
                "new_issues": new_issues[:15],
                "gone_issues": gone_issues[:15],
                "movers_up": movers_up[:10],
                "movers_down": movers_down[:10],
            }

        # Entity tracking (top entities by total mentions)
        ent_totals = {e: sum(sum(c.values()) for c in seasons.values())
                      for e, seasons in ent_season.items()}
        top_entities = sorted(ent_totals, key=ent_totals.get, reverse=True)[:80]

        # Build season order for entity data
        season_names = [s["season"] for s in seasons_meta]

        for entity in top_entities:
            # Top issues per type for this entity
            ei = ent_issues[entity]
            ent_top_issues = {}
            for tp, label in [("complaint", "complaints"), ("suggestion", "suggestions"), ("praise", "praise")]:
                top = ei[tp].most_common(10)
                if top:
                    eis = ent_issues_season[entity][tp]
                    ent_top_issues[label] = [
                        {"text": t, "count": c,
                         "by_season": {s: cnt for s, cnt in eis[t].items() if cnt > 0}}
                        for t, c in top
                    ]

            entity_tracking[entity] = {
                "total": ent_totals[entity],
                "by_season": {},
                "issues": ent_top_issues,
            }
            for sn in season_names:
                counts = ent_season[entity].get(sn, Counter())
                if any(counts.values()):
                    entity_tracking[entity]["by_season"][sn] = dict(counts)

        issue_stats = {
            "total_issues": total_issues_count,
            "reviews_with_issues": reviews_with_issues,
            "total_reviews": len(issues_data),
            "total_complaints": sum(all_complaints.values()),
            "total_suggestions": sum(all_suggestions.values()),
            "total_praise": sum(all_praise.values()),
            "unique_complaints": len(all_complaints),
            "unique_suggestions": len(all_suggestions),
            "unique_praise": len(all_praise),
        }
        print(f"  {total_issues_count:,} issues aggregated "
              f"({sum(all_complaints.values()):,} complaints, "
              f"{sum(all_suggestions.values()):,} suggestions, "
              f"{sum(all_praise.values()):,} praise)")
        print(f"  {len(top_entities)} entities tracked across seasons")

        # Build related issues per AI category (for Category Deep-Dive)
        # Use sentiment-appropriate issues: complaints+suggestions for neg, praise for pos
        neg_issues_combined = all_complaints + all_suggestions
        for cat, prefixes in CATEGORY_ISSUE_PREFIXES.items():
            source = neg_issues_combined if cat in NEG_CATEGORIES else all_praise
            related = []
            for text, count in source.most_common():
                issue_prefix = text.split(": ")[0] if ": " in text else ""
                if issue_prefix in prefixes:
                    related.append({"text": text, "count": count})
                    if len(related) >= 10:
                        break
            if related:
                category_issues[cat] = related

        # Report CATEGORY_ISSUE_PREFIXES coverage
        all_cats = set(neg_cats.keys()) | set(pos_cats.keys())
        mapped_cats = set(CATEGORY_ISSUE_PREFIXES.keys())
        unmapped = all_cats - mapped_cats
        empty_mapped = mapped_cats - set(category_issues.keys())
        if unmapped:
            print(f"  CATEGORY_ISSUE_PREFIXES: {len(unmapped)} categories without prefix mapping: {unmapped}")
        if empty_mapped:
            print(f"  CATEGORY_ISSUE_PREFIXES: {len(empty_mapped)} mapped categories yielded 0 issues: {empty_mapped}")
        print(f"  Category issues coverage: {len(category_issues)}/{len(all_cats)} categories")

    # Enrich trending/fixed with top specific issues from current season
    if issues_data and (trending or fixed):
        curr_s_complaints = season_issues.get(_trends_curr_season, {})
        if isinstance(curr_s_complaints, dict):
            curr_complaints = curr_s_complaints.get("complaints", Counter())
        else:
            curr_complaints = Counter()
        for item in trending + fixed:
            cat = item["category"]
            prefixes = CATEGORY_ISSUE_PREFIXES.get(cat, [])
            top_specific = []
            for text, count in curr_complaints.most_common():
                issue_prefix = text.split(": ")[0] if ": " in text else ""
                if issue_prefix in prefixes:
                    top_specific.append({"text": text, "count": count})
                    if len(top_specific) >= 3:
                        break
            item["top_issues"] = top_specific

    # -----------------------------------------------------------------------
    # 14. Patch notes (for correlation)
    # -----------------------------------------------------------------------
    patch_notes_out = []
    if patch_notes_data:
        for patch in patch_notes_data:
            patch_notes_out.append({
                "season": patch.get("season", ""),
                "version": patch.get("version", ""),
                "date": patch.get("date", ""),
                "balance_changes": patch.get("balance_changes", []),
                "new_content": patch.get("new_content", []),
                "bug_fixes": patch.get("bug_fixes", []),
            })

    # -----------------------------------------------------------------------
    # 14b. Patch impact — pre/post season complaint comparison per entity
    # -----------------------------------------------------------------------
    patch_impact = []
    if patch_notes_data and entity_tracking:
        # Build entity name lookup (same fuzzy logic as frontend findEntity)
        et_keys = list(entity_tracking.keys())
        def find_entity(item_name):
            if item_name in entity_tracking:
                return item_name
            for k in et_keys:
                if k.startswith(item_name) or item_name.startswith(k):
                    return k
            return None

        for patch in patch_notes_data:
            season_code = patch.get("season", "")  # e.g. "S3"
            season_num = season_code.replace("S", "")
            season_name = "Season " + season_num  # e.g. "Season 3"
            prev_num = int(season_num) - 1 if season_num.isdigit() else None
            if prev_num is not None and prev_num >= 0:
                # Match actual season name (e.g. "Season 0 (Beta)")
                prev_name = next(
                    (s["season"] for s in seasons_meta if s["season"].startswith("Season " + str(prev_num))),
                    "Season " + str(prev_num)
                )
            else:
                prev_name = None

            for change in patch.get("balance_changes", []):
                entity_key = find_entity(change.get("item", ""))
                if not entity_key:
                    continue
                by_s = entity_tracking[entity_key].get("by_season", {})
                after = (by_s.get(season_name, {}) or {}).get("complaint", 0)
                before = (by_s.get(prev_name, {}) or {}).get("complaint", 0) if prev_name else 0
                delta_pct = round((after - before) / before * 100) if before > 0 else None
                patch_impact.append({
                    "season": season_code,
                    "version": patch.get("version", ""),
                    "date": patch.get("date", ""),
                    "entity": entity_key,
                    "type": change.get("type", ""),
                    "details": change.get("details", ""),
                    "before": before,
                    "after": after,
                    "delta_pct": delta_pct,
                })

    # -----------------------------------------------------------------------
    # 15. Issue samples — real review texts for each issue in category_issues
    # -----------------------------------------------------------------------
    issue_samples = {}
    if issues_data and category_issues:
        print("Building issue samples...")
        # Collect all issue texts we need samples for
        needed_issues = set()
        for cat, items in category_issues.items():
            for item in items:
                needed_issues.add(item["text"])
        # Also include top_issues (all-time) for CI charts
        for kind in ("complaints", "suggestions", "praise"):
            for item in top_issues.get(kind, []):
                needed_issues.add(item["text"])
        # Also include entity issues for Patch Notes "What Players Say"
        for entity_data in entity_tracking.values():
            for kind in ("complaints", "suggestions", "praise"):
                for item in entity_data.get("issues", {}).get(kind, []):
                    needed_issues.add(item["text"])
        # Issues by season (Season Health top issues)
        for season_data_items in issues_by_season.values():
            for kind in ("complaints", "suggestions", "praise"):
                for item in season_data_items.get(kind, []):
                    needed_issues.add(item["text"])
        # Season diffs (new/gone/movers)
        for diff in season_diffs.values():
            for item in diff.get("new_issues", []):
                needed_issues.add(item["text"])
            for item in diff.get("gone_issues", []):
                needed_issues.add(item["text"])
            for item in diff.get("movers_up", []):
                needed_issues.add(item["text"])
            for item in diff.get("movers_down", []):
                needed_issues.add(item["text"])
        # Language-specific top issues (Regional Analysis)
        for lang_entry in lang_top_issues.values():
            for key, items in lang_entry.items():
                if isinstance(items, list):
                    for item in items:
                        needed_issues.add(item["text"])
        # Issues by month (Review Bombing drill-down)
        for month_items in issues_by_month.values():
            for item in month_items:
                needed_issues.add(item["text"])
        # Trending/fixed top issues (Community Insights trends)
        for item in trends_data.get("trending", []) + trends_data.get("fixed", []):
            for iss in item.get("top_issues", []):
                needed_issues.add(iss["text"])

        # Build issue_text → [review_idx, ...] mapping + idx → season lookup
        issue_to_idxs = defaultdict(list)
        idx_to_season = {}
        for rev in issues_data:
            idx_to_season[rev["idx"]] = get_season(rev["timestamp"])
            for issue in rev.get("issues", []):
                t = issue.get("text", "")
                if t in needed_issues:
                    issue_to_idxs[t].append(rev["idx"])

        # Use already-loaded full reviews (from playtime_at_review step)
        if full_reviews is None:
            print("  Loading review texts for samples...")
            with open("reviews_ai_classified.json", encoding="utf-8") as f:
                full_reviews = json.load(f)

        # Playtime brackets (must match frontend RE_BRACKETS)
        PT_BRACKETS = [
            ("Newcomer", 0, 10),
            ("Casual", 10, 50),
            ("Regular", 50, 100),
            ("Dedicated", 100, 200),
            ("Veteran", 200, 500),
            ("Hardcore", 500, 1000),
            ("No-lifer", 1000, float("inf")),
        ]
        issue_playtime = {}  # issue_text → {bracket_label: count}
        issue_playtime_by_season = {}  # issue_text → {season → {bracket_label: count}}

        import random
        random.seed(42)
        for issue_text, idxs in issue_to_idxs.items():
            # Collect all candidates and compute playtime distribution
            candidates = []
            pt_dist = Counter()
            pt_by_season = defaultdict(Counter)  # season → bracket → count
            for idx in idxs:
                if idx >= len(full_reviews):
                    continue
                rev = full_reviews[idx]
                text = rev.get("review", "").strip()
                lang = rev.get("language", "")
                if not text:
                    continue

                # Extract playtime for distribution (all reviews)
                hours = 0
                try:
                    author = rev.get("author", {})
                    if isinstance(author, str):
                        author = ast.literal_eval(author)
                    hours = round(float(author.get("playtime_at_review", 0)) / 60, 1)
                except (ValueError, TypeError):
                    pass
                season = idx_to_season.get(idx, "Off-season")
                for label, lo, hi in PT_BRACKETS:
                    if lo <= hours < hi:
                        pt_dist[label] += 1
                        pt_by_season[season][label] += 1
                        break

                # Score: prefer medium length (no language bias)
                length_ok = 60 <= len(text) <= 600
                score = (1 if length_ok else 0)
                candidates.append((score, idx, text, lang, rev, hours))

            if pt_dist:
                issue_playtime[issue_text] = dict(pt_dist)
            if pt_by_season:
                issue_playtime_by_season[issue_text] = {
                    s: dict(c) for s, c in pt_by_season.items()
                }

            # Stratified sampling: guarantee playtime diversity
            # 1. Group candidates by bracket
            by_bracket = defaultdict(list)
            for c in candidates:
                h = c[5]  # hours
                for label, lo, hi in PT_BRACKETS:
                    if lo <= h < hi:
                        by_bracket[label].append(c)
                        break

            # 2. Sort each bracket by score desc, shuffle top for variety
            for label in by_bracket:
                by_bracket[label].sort(key=lambda x: -x[0])
                top = by_bracket[label][:20]
                random.shuffle(top)
                by_bracket[label] = top

            # 3. Round-robin: pick from each bracket proportionally
            # At least 1 per bracket (if available), rest proportional
            TARGET = 15
            present = [l for l, _, _ in PT_BRACKETS if by_bracket.get(l)]
            if not present:
                issue_samples[issue_text] = []
                continue

            # Allocate slots proportional to real distribution
            total_pop = sum(pt_dist.get(l, 0) for l in present)
            slots = {}
            remaining = TARGET
            for l in present:
                # At least 1, rest proportional
                slots[l] = 1
                remaining -= 1
            # Distribute remaining proportionally
            if remaining > 0 and total_pop > 0:
                for l in present:
                    extra = round((pt_dist.get(l, 0) / total_pop) * remaining)
                    slots[l] += extra
                # Adjust if total != TARGET
                total_slots = sum(slots.values())
                diff = TARGET - total_slots
                if diff != 0:
                    # Add/remove from largest bracket
                    largest = max(present, key=lambda l: pt_dist.get(l, 0))
                    slots[largest] += diff

            samples = []
            for l in present:
                n = min(slots.get(l, 1), len(by_bracket[l]))
                for c in by_bracket[l][:n]:
                    _, idx, text, lang, rev, hours = c
                    samples.append({
                        "text": text[:500],
                        "hours": hours,
                        "up": str(rev.get("voted_up", "True")) == "True",
                        "lang": lang,
                        "ts": rev.get("timestamp_created", 0),
                        "season": idx_to_season.get(idx, "Off-season"),
                    })

            if samples:
                issue_samples[issue_text] = samples

        del full_reviews  # free memory
        print(f"  {len(issue_samples)} issues with samples")

    # -----------------------------------------------------------------------
    # Assemble and save
    # -----------------------------------------------------------------------
    dashboard = {
        "overview": overview,
        "category_rankings": category_rankings,
        "season_health": season_health,
        "seasons_meta": seasons_meta,
        "monthly_timeline": dict(sorted(monthly.items())),
        "daily_timeline": dict(sorted(daily.items())),
        "daily_cats": {d: dict(cats.most_common(5)) for d, cats in daily_cats.items()
                       if daily[d]["positive"] + daily[d]["negative"] >= 10},
        "playtime_data": playtime_data,
        "playtime_order": brackets_order,
        "cat_playtime_neg": {k: dict(v) for k, v in cat_playtime_neg.items()},
        "cat_playtime_pos": {k: dict(v) for k, v in cat_playtime_pos.items()},
        "cat_season_pt_neg": {cat: {s: dict(brs) for s, brs in seasons.items()} for cat, seasons in cat_season_pt_neg.items()},
        "cat_season_pt_pos": {cat: {s: dict(brs) for s, brs in seasons.items()} for cat, seasons in cat_season_pt_pos.items()},
        "cohort_data": cohort_data,
        "regional": regional,
        "top_reviews": top_reviews,
        "recurring_problems": recurring,
        "comparisons": comparisons,
        "trends": trends_data,
        "season_diffs": season_diffs if issues_data else {},
        "regional_deviation": regional_deviation,
        "category_correlations": category_correlations,
        "issues_by_month": issues_by_month if issues_data else {},
        "lang_top_issues": lang_top_issues if issues_data else {},
        # Stage 2 data
        "top_issues": top_issues,
        "issues_by_season": issues_by_season,
        "entity_tracking": entity_tracking,
        "entity_classes": entity_classes,
        "issue_stats": issue_stats,
        "category_issues": category_issues,
        "issue_playtime": issue_playtime if issues_data and category_issues else {},
        "issue_playtime_by_season": issue_playtime_by_season if issues_data and category_issues else {},
        # Patch notes
        "patch_notes": patch_notes_out,
        "patch_impact": patch_impact,
        # SteamCharts avg concurrent players (monthly) — source: steamcharts.com
        "player_count": {
            "2023-12": 242399, "2024-01": 116913, "2024-02": 49060,
            "2024-03": 48226, "2024-04": 27273, "2024-05": 20910,
            "2024-06": 31623, "2024-07": 16720, "2024-08": 16929,
            "2024-09": 29264, "2024-10": 24975, "2024-11": 21731,
            "2024-12": 23431, "2025-01": 18121, "2025-02": 19499,
            "2025-03": 27295, "2025-04": 20271, "2025-05": 19849,
            "2025-06": 28835, "2025-07": 19426, "2025-08": 25801,
            "2025-09": 32758, "2025-10": 24326, "2025-11": 16742,
            "2025-12": 12835,
            "2026-01": 11969, "2026-02": 11872,
        },
    }
    if hc_data:
        dashboard["hc"] = hc_data

    with open("docs/dashboard_data.json", "w", encoding="utf-8") as f:
        json.dump(dashboard, f, ensure_ascii=False)

    size = os.path.getsize("docs/dashboard_data.json") / 1024
    print(f"Saved docs/dashboard_data.json ({size:.0f} KB)")

    # Save issue_samples as separate file (lazy-loaded by frontend)
    if issue_samples:
        with open("docs/issue_samples.json", "w", encoding="utf-8") as f:
            json.dump(issue_samples, f, ensure_ascii=False)
        ssize = os.path.getsize("docs/issue_samples.json") / 1024
        print(f"Saved docs/issue_samples.json ({ssize:.0f} KB)")

    print()


if __name__ == "__main__":
    main()
