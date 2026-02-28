"""
Analyze unclassified Steam reviews for THE FINALS to find common themes
that are missing from the current category system.

Loads reviews_classified.json, filters to unclassified reviews,
and performs keyword frequency analysis + bigram/trigram extraction.
"""

import json
import re
import sys
from collections import Counter

# ---- Configuration ----

MIN_REVIEW_LENGTH = 20  # Skip very short reviews (memes, "1st", etc.)

# Specific game-related terms to count in unclassified reviews
SPECIFIC_TERMS = {
    # Dead game / playerbase
    "dead game": r"\bdead\s*game\b",
    "dead": r"\bdead\b",
    "playerbase": r"\bplayer\s*base\b",
    "population": r"\bpopulation\b",
    "queue time / wait time": r"\b(?:queue|wait)\s*(?:time|times)\b",
    "long queue": r"\b(?:long|forever)\s*(?:queue|wait|matchmak)\b",
    "game is dying": r"\bgame\b.*\b(?:dying|di(?:ed|es))\b",

    # Region / China
    "region lock": r"\bregion\s*lock\b",
    "china / chinese": r"\b(?:china|chinese)\b",
    "asia": r"\basia(?:n)?\b",
    "vpn": r"\bvpn\b",

    # Crossplay / platform
    "crossplay": r"\bcross\s*play\b",
    "console": r"\bconsole\b",
    "controller": r"\bcontroller\b",
    "pc": r"\bpc\b",
    "aim assist": r"\baim\s*assist\b",
    "mouse": r"\bmouse\b",
    "keyboard": r"\bkeyboard\b",
    "ps4 / ps5": r"\bps[45]\b",
    "xbox": r"\bxbox\b",

    # Content drought
    "content": r"\bcontent\b",
    "update": r"\bupdate[s]?\b",
    "new map": r"\bnew\s*map[s]?\b",
    "lack of content": r"\b(?:lack|no|missing)\b.*\bcontent\b",
    "stale": r"\bstale\b",
    "boring": r"\bboring\b",
    "repetitive": r"\brepetiti(?:ve|ous)\b",
    "nothing new": r"\bnothing\s*new\b",
    "same thing": r"\bsame\s*thing\b",

    # TTK
    "ttk": r"\bttk\b",
    "time to kill": r"\btime\s*to\s*kill\b",
    "bullet sponge": r"\bbullet\s*spong[ey]\b",
    "spongey / spongy": r"\bspong[ey]\b",

    # SBMM
    "sbmm": r"\bsbmm\b",
    "skill based": r"\bskill\s*based\b",
    "matchmaking": r"\bmatchmak(?:ing|er)\b",
    "sweaty / sweat": r"\bsweat(?:y|s|ing)?\b",
    "tryhard": r"\btry\s*hard[s]?\b",

    # Solo / team
    "solo": r"\bsolo\b",
    "solo queue": r"\bsolo\s*queue\b",
    "friends": r"\bfriend[s]?\b",
    "team": r"\bteam\b",
    "duo": r"\bduo[s]?\b",

    # Monetization terms
    "f2p / free to play": r"\b(?:f2p|free\s*to\s*play)\b",
    "pay / paid": r"\b(?:pay|paid)\b",
    "skin / skins": r"\bskin[s]?\b",
    "cosmetic": r"\bcosmetic[s]?\b",
    "battle pass": r"\bbattle\s*pass\b",
    "season": r"\bseason\b",
    "shop / store": r"\b(?:shop|store)\b",
    "overpriced": r"\boverpriced\b",

    # Balance terms
    "nerf": r"\bnerf(?:ed|s|ing)?\b",
    "buff": r"\bbuff(?:ed|s|ing)?\b",
    "patch": r"\bpatch(?:es)?\b",
    "meta": r"\bmeta\b",
    "op / overpowered": r"\b(?:op|overpowered)\b",
    "broken": r"\bbroken\b",

    # General sentiment
    "fun": r"\bfun\b",
    "addictive": r"\baddic(?:ting|tive|ted)\b",
    "unique": r"\bunique\b",
    "original": r"\boriginal\b",
    "potential": r"\bpotential\b",
    "disappointing": r"\bdisappoint(?:ing|ed|ment)?\b",
    "frustrating": r"\bfrustrat(?:ing|ed|ion)?\b",
    "annoying": r"\bannoying\b",

    # Misc
    "AI / bot": r"\b(?:ai\b|bots?\b)",
    "announcer": r"\bannouncer\b",
    "voice / audio": r"\b(?:voice|audio)\b",
    "tos / terms of service": r"\b(?:tos|terms\s*of\s*service)\b",
    "abandon / leave / quit": r"\b(?:abandon|leav(?:e|ing)|quit(?:ting)?)\b",
    "uninstall": r"\buninstall(?:ed|ing)?\b",
    "refund": r"\brefund(?:ed|ing)?\b",
    "trash": r"\btrash\b",
    "garbage": r"\bgarbage\b",
    "worst": r"\bworst\b",
    "ruined": r"\bruin(?:ed|ing|s)?\b",
    "toxic": r"\btoxic(?:ity)?\b",
    "grind": r"\bgrind(?:ing|y)?\b",
    "progression": r"\bprogression\b",
    "rank / ranked": r"\brank(?:ed|s|ing)?\b",
    "casual": r"\bcasual\b",
    "competitive": r"\bcompetitiv(?:e|ely)\b",
    "tournament": r"\btournament[s]?\b",
    "esport": r"\besport[s]?\b",
    "linux": r"\blinux\b",
    "steam deck": r"\bsteam\s*deck\b",
    "anti-cheat (general)": r"\banti\s*cheat\b",
    "easy anti-cheat / eac": r"\b(?:eac|easy\s*anti\s*cheat)\b",
    "sound / music": r"\b(?:sound(?:track)?|music)\b",
    "map / maps": r"\bmap[s]?\b",
    "destroy / destruction": r"\b(?:destroy|destruction|destructi(?:ble|on))\b",
    "steal / stolen": r"\b(?:steal(?:ing)?|stolen)\b",
    "third party": r"\bthird\s*part(?:y|ies)\b",
    "invest": r"\binvest(?:ed|ment|ing)?\b",
    "wasted": r"\bwast(?:e[ds]?|ing)\b",
    "nostalgia / old version": r"\b(?:nostalgia|old\s*version|beta\s*was\s*better|used\s*to\s*be)\b",
    "early access / beta": r"\b(?:early\s*access|beta|alpha)\b",
}

# Themed searches for potential new categories
THEME_SEARCHES = {
    "Dead game / low playerbase / dying": [
        r"\bdead\s*game\b",
        r"\bgame\b.*\b(?:dying|di(?:ed|es))\b",
        r"\bplayer\s*(?:count|base)\b.*(?:low|dead|dying|drop|declin|shrink)",
        r"\bno\s*(?:one|body)\b.*\bplay\b",
        r"\bdying\b",
        r"\bgame\s*is\s*dead\b",
        r"\b(?:low|empty|dead)\s*(?:server|lobby|lobbies|queue)\b",
        r"\blong\s*(?:queue|wait|matchmak)\b",
        r"\bcan.?t\s*find\s*(?:a\s*)?(?:match|game|lobby|player)\b",
    ],
    "Region lock / China / VPN": [
        r"\bregion\s*lock\b",
        r"\bchina\b.*(?:cheat|hack|lag|ruin|player|flood|invad)",
        r"\bchinese\b.*(?:cheat|hack|player|server)",
        r"\bvpn\b",
        r"\bregion\b.*(?:lock|block|restrict|limit|separ)",
    ],
    "Crossplay / console vs PC / aim assist": [
        r"\bcross\s*play\b",
        r"\baim\s*assist\b",
        r"\bconsole\b.*\bpc\b",
        r"\bpc\b.*\bconsole\b",
        r"\bcontroller\b.*(?:advantage|unfair|aimbot|aim\s*assist|broken|op)",
        r"\bmouse\b.*\bcontroller\b",
        r"\bcontroller\b.*\bmouse\b",
        r"\binput\s*(?:method|type|based)\b",
    ],
    "Content drought / lack of updates / maps": [
        r"\b(?:lack|no|missing|need|want)\b.*\bcontent\b",
        r"\b(?:lack|no|missing|need|want)\b.*\b(?:new\s*)?map[s]?\b",
        r"\b(?:lack|no|missing)\b.*\bupdate[s]?\b",
        r"\bnothing\s*new\b",
        r"\bsame\s*(?:thing|map|mode|stuff)\b.*(?:over|again|repeat)",
        r"\bstale\b",
        r"\bgets?\s*(?:old|boring|repetitive|stale)\b",
        r"\bneed\s*(?:more|new)\s*(?:content|map|mode|weapon|gun)\b",
    ],
    "TTK complaints (in unclassified)": [
        r"\bttk\b",
        r"\btime\s*to\s*kill\b",
        r"\bbullet\s*spong[ey]\b",
        r"\bspong[ey]\b",
        r"\btoo\s*(?:many|much)\s*(?:health|hp|hit)\b",
        r"\b(?:takes?|need)\s*(?:too\s*)?(?:long|forever|many\s*(?:bullet|shot|hit))\s*(?:to\s*)?kill\b",
    ],
    "SBMM complaints (in unclassified)": [
        r"\bsbmm\b",
        r"\bskill\s*based\s*match\b",
        r"\bmmr\b",
        r"\bsweaty?\b.*(?:lobby|match|game)",
        r"\btryhard\b",
    ],
    "Controller vs mouse / input fairness": [
        r"\bcontroller\b.*(?:advantage|unfair|broken|op|aimbot|aim\s*assist)",
        r"\baim\s*assist\b.*(?:broken|op|unfair|strong|ridiculous|insane)",
        r"\binput\b.*(?:fair|unfair|balance|advantage)",
        r"\bm[&n]?k\b.*\bcontroller\b",
        r"\bcontroller\b.*\bm[&n]?k\b",
    ],
    "Toxic / griefing / team sabotage": [
        r"\btoxic(?:ity)?\b",
        r"\bgrief(?:er|ing|s)?\b",
        r"\btroll(?:ing|s|er)?\b",
        r"\bteam\s*kill(?:ing|er|ed)?\b",
        r"\bsabotag(?:e|ing)\b",
        r"\bthrow(?:ing|er|s)?\b.*(?:game|match)",
        r"\bafk\b",
    ],
    "Linux / Steam Deck / OS compatibility": [
        r"\blinux\b",
        r"\bsteam\s*deck\b",
        r"\bproton\b",
        r"\bwindows\s*only\b",
        r"\b(?:os|operating\s*system)\b.*(?:support|compat)",
    ],
    "Nostalgia / game was better before": [
        r"\bused\s*to\s*be\b.*(?:good|great|fun|better|amazing)",
        r"\bbeta\b.*(?:was|were)\s*(?:better|good|more\s*fun)",
        r"\b(?:season\s*[012]|s[012])\b.*(?:was|were)\s*(?:better|good|fun|peak)",
        r"\bthey\s*(?:ruined|changed|killed|destroyed)\b",
        r"\bnot\s*(?:the\s*)?same\s*game\b",
        r"\bgame\b.*(?:changed|different).*(?:worse|bad|not\s*fun)",
    ],
    "Frustrating / not fun anymore": [
        r"\bfrustrat(?:ing|ed|ion)\b",
        r"\bannoying\b",
        r"\bnot\s*fun\b",
        r"\bno\s*(?:longer|more)\s*fun\b",
        r"\bstopped?\s*(?:being|having)\s*fun\b",
        r"\b(?:unfun|un-fun)\b",
        r"\bstress(?:ful|ing)?\b",
        r"\brage\s*(?:quit|inducing|game)?\b",
    ],
    "Wasted potential": [
        r"\bpotential\b",
        r"\bwast(?:ed|ing)\s*potential\b",
        r"\bcould\s*(?:be|have\s*been)\s*(?:so\s*much\s*)?(?:great|good|better|amazing)\b",
        r"\bgreat\s*(?:concept|idea)\b.*(?:but|however|poor|bad)",
        r"\b(?:good|great)\s*(?:game|concept)\b.*\bbut\b",
    ],
    "Stealing mechanic frustration": [
        r"\bsteal(?:ing)?\b.*(?:frustrat|annoy|unfair|stupid|broken|bs|bullshit|hate)",
        r"\blast\s*(?:second|minute)\b.*\bsteal\b",
        r"\bstolen\b.*(?:vault|cashout|cash|win|game|match)",
        r"\bthird\s*part(?:y|ied|ies)\b",
        r"\b3rd\s*party\b",
    ],
    "Short review / meme / no substance": [
        # Just to understand the "noise" — very short reviews
    ],
}


def extract_ngrams(text, n=2):
    """Extract n-grams from cleaned text."""
    # Basic cleanup
    text = text.lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[^a-z\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    words = text.split()
    # Filter out very common stop words
    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "shall", "can", "need", "dare", "ought",
        "used", "to", "of", "in", "for", "on", "with", "at", "by", "from",
        "as", "into", "through", "during", "before", "after", "above",
        "below", "between", "out", "off", "over", "under", "again",
        "further", "then", "once", "here", "there", "when", "where", "why",
        "how", "all", "both", "each", "few", "more", "most", "other",
        "some", "such", "no", "nor", "not", "only", "own", "same", "so",
        "than", "too", "very", "just", "don", "now", "and", "but", "or",
        "if", "while", "that", "this", "it", "its", "i", "me", "my",
        "we", "our", "you", "your", "he", "him", "his", "she", "her",
        "they", "them", "their", "what", "which", "who", "whom",
        "up", "about", "s", "t", "re", "ve", "d", "ll", "m",
        "really", "much", "get", "got", "like", "also", "even",
    }
    words = [w for w in words if w not in stop_words and len(w) > 1]
    if len(words) < n:
        return []
    return [" ".join(words[i:i+n]) for i in range(len(words) - n + 1)]


def main():
    print("Loading reviews_classified.json ...")
    with open("/home/aryzhkin/projects/the-finals/reviews_classified.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Total reviews: {len(data)}")

    # Split by sentiment
    neg_reviews = [r for r in data if r["sentiment"] == "negative"]
    pos_reviews = [r for r in data if r["sentiment"] == "positive"]

    # Unclassified
    uncl_neg = [r for r in neg_reviews if not r["categories"]]
    uncl_pos = [r for r in pos_reviews if not r["categories"]]
    uncl_all = uncl_neg + uncl_pos

    print(f"\nNegative reviews: {len(neg_reviews)} total, {len(uncl_neg)} unclassified ({len(uncl_neg)/len(neg_reviews)*100:.1f}%)")
    print(f"Positive reviews: {len(pos_reviews)} total, {len(uncl_pos)} unclassified ({len(uncl_pos)/len(pos_reviews)*100:.1f}%)")
    print(f"All unclassified: {len(uncl_all)}")

    # Filter out very short reviews to focus on substantive ones
    uncl_neg_long = [r for r in uncl_neg if len(r["text"]) >= MIN_REVIEW_LENGTH]
    uncl_pos_long = [r for r in uncl_pos if len(r["text"]) >= MIN_REVIEW_LENGTH]
    uncl_all_long = uncl_neg_long + uncl_pos_long

    short_neg = len(uncl_neg) - len(uncl_neg_long)
    short_pos = len(uncl_pos) - len(uncl_pos_long)

    print(f"\nShort reviews (<{MIN_REVIEW_LENGTH} chars) excluded: {short_neg} neg, {short_pos} pos")
    print(f"Substantive unclassified: {len(uncl_neg_long)} neg, {len(uncl_pos_long)} pos, {len(uncl_all_long)} total")

    # ---- Language breakdown of unclassified ----
    print(f"\n{'='*70}")
    print("LANGUAGE BREAKDOWN OF UNCLASSIFIED REVIEWS")
    print(f"{'='*70}")
    lang_counter_neg = Counter(r["language"] for r in uncl_neg)
    lang_counter_pos = Counter(r["language"] for r in uncl_pos)
    print("\nTop 15 languages (negative unclassified):")
    for lang, count in lang_counter_neg.most_common(15):
        print(f"  {lang:<20s} {count:5d}  ({count/len(uncl_neg)*100:.1f}%)")
    print("\nTop 15 languages (positive unclassified):")
    for lang, count in lang_counter_pos.most_common(15):
        print(f"  {lang:<20s} {count:5d}  ({count/len(uncl_pos)*100:.1f}%)")

    # ---- Specific term frequency in unclassified reviews ----
    print(f"\n{'='*70}")
    print("SPECIFIC TERM FREQUENCY IN UNCLASSIFIED REVIEWS (substantive only)")
    print(f"{'='*70}")

    term_counts_neg = {}
    term_counts_pos = {}
    term_counts_all = {}

    for term_name, pattern in SPECIFIC_TERMS.items():
        regex = re.compile(pattern, re.IGNORECASE)
        cn = sum(1 for r in uncl_neg_long if regex.search(r["text"]))
        cp = sum(1 for r in uncl_pos_long if regex.search(r["text"]))
        term_counts_neg[term_name] = cn
        term_counts_pos[term_name] = cp
        term_counts_all[term_name] = cn + cp

    # Sort by total count descending
    sorted_terms = sorted(term_counts_all.items(), key=lambda x: -x[1])

    print(f"\n{'Term':<40s} {'Neg':>6s} {'Pos':>6s} {'Total':>6s}")
    print("-" * 60)
    for term, total in sorted_terms:
        if total > 0:
            print(f"  {term:<38s} {term_counts_neg[term]:>6d} {term_counts_pos[term]:>6d} {total:>6d}")

    # ---- Theme searches ----
    print(f"\n{'='*70}")
    print("THEME ANALYSIS — POTENTIAL MISSING CATEGORIES")
    print(f"{'='*70}")

    for theme_name, patterns in THEME_SEARCHES.items():
        if not patterns:
            continue
        regexes = [re.compile(p, re.IGNORECASE) for p in patterns]
        matches_neg = set()
        matches_pos = set()
        for i, r in enumerate(uncl_neg_long):
            for rx in regexes:
                if rx.search(r["text"]):
                    matches_neg.add(i)
                    break
        for i, r in enumerate(uncl_pos_long):
            for rx in regexes:
                if rx.search(r["text"]):
                    matches_pos.add(i)
                    break
        total = len(matches_neg) + len(matches_pos)
        print(f"\n--- {theme_name} ---")
        print(f"  Neg: {len(matches_neg)}, Pos: {len(matches_pos)}, Total: {total}")

        # Show a few sample texts
        samples_neg = [uncl_neg_long[i]["text"][:200] for i in list(matches_neg)[:3]]
        samples_pos = [uncl_pos_long[i]["text"][:200] for i in list(matches_pos)[:3]]
        if samples_neg:
            print(f"  Sample negative reviews:")
            for s in samples_neg:
                print(f"    > {s.strip()!r}")
        if samples_pos:
            print(f"  Sample positive reviews:")
            for s in samples_pos:
                print(f"    > {s.strip()!r}")

    # ---- Bigram analysis (English reviews only for meaningful n-grams) ----
    print(f"\n{'='*70}")
    print("TOP 50 BIGRAMS — UNCLASSIFIED NEGATIVE REVIEWS (English, >=20 chars)")
    print(f"{'='*70}")

    eng_uncl_neg = [r for r in uncl_neg_long if r["language"] == "english"]
    eng_uncl_pos = [r for r in uncl_pos_long if r["language"] == "english"]

    print(f"  (Analyzing {len(eng_uncl_neg)} English unclassified negative reviews)")

    bigram_counter_neg = Counter()
    for r in eng_uncl_neg:
        for bg in extract_ngrams(r["text"], 2):
            bigram_counter_neg[bg] += 1

    for i, (bg, count) in enumerate(bigram_counter_neg.most_common(50), 1):
        print(f"  {i:2d}. {bg:<35s} {count:5d}")

    print(f"\n{'='*70}")
    print("TOP 50 BIGRAMS — UNCLASSIFIED POSITIVE REVIEWS (English, >=20 chars)")
    print(f"{'='*70}")

    print(f"  (Analyzing {len(eng_uncl_pos)} English unclassified positive reviews)")

    bigram_counter_pos = Counter()
    for r in eng_uncl_pos:
        for bg in extract_ngrams(r["text"], 2):
            bigram_counter_pos[bg] += 1

    for i, (bg, count) in enumerate(bigram_counter_pos.most_common(50), 1):
        print(f"  {i:2d}. {bg:<35s} {count:5d}")

    # ---- Trigram analysis ----
    print(f"\n{'='*70}")
    print("TOP 40 TRIGRAMS — UNCLASSIFIED NEGATIVE REVIEWS (English)")
    print(f"{'='*70}")

    trigram_counter_neg = Counter()
    for r in eng_uncl_neg:
        for tg in extract_ngrams(r["text"], 3):
            trigram_counter_neg[tg] += 1

    for i, (tg, count) in enumerate(trigram_counter_neg.most_common(40), 1):
        print(f"  {i:2d}. {tg:<45s} {count:5d}")

    print(f"\n{'='*70}")
    print("TOP 40 TRIGRAMS — UNCLASSIFIED POSITIVE REVIEWS (English)")
    print(f"{'='*70}")

    trigram_counter_pos = Counter()
    for r in eng_uncl_pos:
        for tg in extract_ngrams(r["text"], 3):
            trigram_counter_pos[tg] += 1

    for i, (tg, count) in enumerate(trigram_counter_pos.most_common(40), 1):
        print(f"  {i:2d}. {tg:<45s} {count:5d}")

    # ---- Review length distribution ----
    print(f"\n{'='*70}")
    print("REVIEW LENGTH DISTRIBUTION — ALL UNCLASSIFIED")
    print(f"{'='*70}")

    lengths = [len(r["text"]) for r in uncl_all]
    brackets = [
        (0, 5, "0-5 chars (empty/meme)"),
        (5, 20, "5-20 chars (very short)"),
        (20, 50, "20-50 chars (short)"),
        (50, 100, "50-100 chars"),
        (100, 200, "100-200 chars"),
        (200, 500, "200-500 chars"),
        (500, 1000, "500-1000 chars"),
        (1000, 99999, "1000+ chars"),
    ]
    for lo, hi, label in brackets:
        cnt = sum(1 for l in lengths if lo <= l < hi)
        print(f"  {label:<30s} {cnt:6d}  ({cnt/len(lengths)*100:.1f}%)")

    # ---- Some sample unclassified long negative reviews ----
    print(f"\n{'='*70}")
    print("SAMPLE UNCLASSIFIED NEGATIVE REVIEWS (English, >200 chars)")
    print(f"{'='*70}")
    long_neg = sorted(
        [r for r in eng_uncl_neg if len(r["text"]) > 200],
        key=lambda r: -r["votes_up"]
    )
    for i, r in enumerate(long_neg[:20]):
        print(f"\n  [{i+1}] votes_up={r['votes_up']}, hours={r['hours']}, season={r['season']}")
        print(f"      {r['text'][:300].strip()!r}")

    print(f"\n{'='*70}")
    print("SAMPLE UNCLASSIFIED POSITIVE REVIEWS (English, >200 chars, most upvoted)")
    print(f"{'='*70}")
    long_pos = sorted(
        [r for r in eng_uncl_pos if len(r["text"]) > 200],
        key=lambda r: -r["votes_up"]
    )
    for i, r in enumerate(long_pos[:20]):
        print(f"\n  [{i+1}] votes_up={r['votes_up']}, hours={r['hours']}, season={r['season']}")
        print(f"      {r['text'][:300].strip()!r}")


if __name__ == "__main__":
    main()
