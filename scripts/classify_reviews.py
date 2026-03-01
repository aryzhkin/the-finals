"""
Keyword-based review classifier for THE FINALS Steam reviews.

Reads reviews_merged.json, classifies each review into reason categories,
and outputs aggregated results with breakdowns by month and playtime bracket.
"""

import json
import os
import re
import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Category definitions: (category_name, [keyword_patterns])
# Each pattern is compiled as a case-insensitive regex.
# A review can match multiple categories.
# ---------------------------------------------------------------------------

NEGATIVE_CATEGORIES = {
    "Cheaters & Hackers": [
        r"\bcheat(?:er|s|ing)?\b", r"\bhack(?:er|s|ing|ed)?\b",
        r"\baimbot\b", r"\bwallhack\b", r"\bwall\s*hack\b",
        r"\bcronus\b", r"\bzen\b.*cronus|cronus.*\bzen\b",
        r"\bчит(?:ер|еры|ы|)\b", r"\bхак(?:ер|еры)?\b",
        r"\bаимбот\b", r"\bвх\b",
        r"\bexploit(?:s|ing|er)?\b",
    ],
    "Matchmaking & SBMM": [
        r"\bmatchmak(?:ing|er)\b", r"\bsbmm\b", r"\bmmr\b",
        r"\bskill.based.match", r"\bunfair.match",
        r"matched.*(higher|better|sweaty|pro|level\s*\d)",
        r"(higher|better).*(level|rank|player).*match",
        r"\bstomp(?:ed|ing|s)?\b",
        r"\bsmurf(?:s|ing)?\b",
        r"\bподбор\b.*игрок", r"\bбаланс\b.*команд",
        r"\bматчмейк\b", r"\bсбмм\b",
        r"lobby.*(sweat|tryhard|try.hard)",
        r"\btop\s*500\b", r"\bruby\b.*player",
        r"against.*(top|best|highest|pro)\s*(player|rank|level)",
        r"(?:casual|unranked).*(?:sweat|tryhard|try.hard|pro|top.500)",
        r"\bкидают?\b.*(?:к|про|могут|высок)",
        r"\bсбмм\b.*(?:нет|отсутств)",
        r"\bранг\w*\b.*(?:высок|низк|дисбаланс)",
        r"\bпопущ\w*\b",
    ],
    "Weapon & Class Balance": [
        r"\bunbalanc(?:ed|e)\b", r"\bimbalanc(?:ed|e)\b",
        r"\b(?:op|overpowered)\b.*(?:melee|sword|katana|knife|dagger|hammer|rpg|flamethrower|flame|shotgun|smg|minigun)",
        r"(?:melee|sword|katana|knife|dagger|hammer|rpg|flamethrower|flame|shotgun).*\b(?:op|overpowered|broken|cheese|ridiculous|ruin)\b",
        r"\bmelee\b.*(?:beat|kill|one.?shot|2.?shot|two.?shot|broken|op\b|overpower|ridiculous)",
        r"\bbalance\b", r"\bбаланс\b",
        r"\bnerfs?\b", r"\bbuffs?\b",
        r"(?:heavy|light|medium).*(?:broken|op\b|overpowered|imbalance|unbalance|useless|bother)",
        r"\bclass\b.*(?:imbalanc|unbalanc|broken|unfair)",
        r"\bone.?shot\b.*(?:broken|op|weapon|kill|unfair)",
        r"\bsword[s]?\b.*(?:ruin|broken|op|hate|remove|stupid|disgust)",
        r"\bdagger[s]?\b.*(?:ruin|broken|op|hate|remove)",
        r"\binvis(?:ible|ibility)?\b.*(?:broken|op|stupid|unfair|hate|cheese)",
        r"\bпушк\w*\b.*(?:горох|дисбаланс|имба|оп\b)",
        r"\bимб\w+\b",
        r"\bодин\w*\b.*(?:класс|пушк)",
        r"\bгорох\w*\b",
    ],
    "Performance & Optimization": [
        r"\bfps\b", r"\boptimi[sz](?:ation|ed|e)\b",
        r"\bstutter(?:s|ing)?\b", r"\bscreen.?tear(?:ing)?\b",
        r"\bframe.?(?:rate|drop|s)\b",
        r"\brun[s]?\s+(?:like|bad|terrible|awful|poor|crap|shit|trash)",
        r"\bслайдшоу\b", r"\bоптимизац\b", r"\bфриз\b",
        r"\bтормоз(?:ит|а|ы)?\b",
        r"\bhigh.?(?:temp|cpu|gpu)", r"\b\d+\s*°",
        r"\bslideshow\b",
        r"\bплохая оптимизация\b",
        r"\bмыльн\w+\b",
    ],
    "Network & Servers": [
        r"\bpacket.?loss\b", r"\brubber.?band(?:ing)?\b",
        r"\bdesync\b", r"\bnetcode\b", r"\bnet\s*code\b",
        r"\b(?:server|network).?(?:lag|issue|problem|error|crash|disconnect)",
        r"\bdisconnect(?:ed|ing|s|ion)?\b",
        r"\blag(?:s|gy|ging|ged)?\b",
        r"\bping\b.*(?:high|bad|terrible|spike)",
        r"\blatency\b",
        r"\bлаг(?:и|ает|ов)?\b", r"\bпинг\b",
        r"\bunable.to.sync\b",
        r"\bconnect(?:ing)?\s*screen\b",
        r"\bразрыв\w*\b",
        r"\bрегистрац\w*\b.*попадан",
    ],
    "Anti-cheat & False Bans": [
        r"\bfalse.?ban\b", r"\bbanned?\b.*(?:no.?reason|unfair|unjust|false|innocent|nothing|wrong)",
        r"(?:no.?reason|unfair|unjust|false|innocent|nothing|wrong).*\bbanned?\b",
        r"\banti.?cheat\b.*(?:broken|bad|garbage|terrible|trash|awful)",
        r"\bпермач\b", r"\bзабанили\b", r"\bзабан\w*\b",
        r"\bблокировк\w*\b",
        r"\bbanned?\b.*\bcheating\b.*(?:never|didn|don|not|no)",
        r"\bfalse(?:ly)?\s*(?:ban|accus|flag|perman)",
        r"\bbanned?\b.*(?:permanent|perma)",
        r"\bban(?:ned)?\b.*(?:for nothing|wrongly|wrongful|without reason)",
        r"\bsuspend(?:ed)?\b.*(?:account|unfair|no reason|unjust)",
        r"\bban\b.*(?:модификац|reason)",
        r"\banticheat\b",
        r"\banticheater?\b",
    ],
    "Customer Support": [
        r"\bsupport\b.*(?:bad|terrible|awful|garbage|trash|useless|ignore|no.help|bot.response|doesn.t.help|won.t.help|automat|copy.paste)",
        r"\bтехподдержк\w*\b", r"\bтп\b.*(?:не отвечает|игнор)",
        r"\bticket\b.*(?:ignore|no.response|bot|automat|copy.paste)",
        r"\bembark\b.*(?:ignore|doesn.t.care|don.t.care|not.help|won.t)",
        r"\bcopy.?paste\b.*(?:response|support|reply)",
        r"\bautomated\b.*(?:response|reply|support)",
        r"\bbot\s*response\b",
    ],
    "TTK (Time to Kill)": [
        r"\bttk\b",
        r"\btime.to.kill\b",
        r"\b(?:full|whole|entire)\s*mag(?:azine)?\b",
        r"\btoo\s+(?:long|much|many)\s+(?:to\s+)?kill\b",
        r"\bbullet.?spong(?:e|y)\b",
        r"\bтткш\b", r"\bубить\b.*(?:долго|невозможно)",
        r"\bhigh\s+ttk\b",
        r"\bтtk\b",
        r"\bтtк\b",
        r"\bклип\b.*(?:не хватает|кончает|мало)",
        r"\brun\s*out\s*of\s*(?:ammo|clip|mag)\b",
    ],
    "Teammates & Solo Experience": [
        r"\bteammate[s]?\b.*(?:bad|awful|terrible|braindead|brain.dead|trash|useless|worst|garbage|stupid|lobotom)",
        r"(?:bad|awful|terrible|braindead|brain.dead|trash|useless).*\bteammate",
        r"\bsolo\b.*(?:unplayable|impossible|terrible|bad|awful|can.t|don.t|suck)",
        r"\bno\s+teammate[s]?\b",
        r"\bleaver[s]?\b", r"\bleaving\b.*(?:match|game|mid)",
        r"\bdon.?t\s+have\s+(?:a\s+)?(?:team|friend|squad|group)\b",
        r"\bwithout\s+(?:a\s+)?(?:team|friend|squad|premade)\b",
        r"\bneed\s+(?:a\s+)?(?:team|friend|squad|premade|3)\b.*\bdon.t\b",
        r"\bunless\s+you\s+have\s+(?:two|2|three|3)\s+friend",
        r"\bплохие\s+(?:тиммейт|союзник|напарник)\b",
    ],
    "Developer Direction": [
        r"\barc.?raiders?\b",
        r"\bdev(?:s|eloper)?.*(?:don.t care|doesn.t care|abandon|neglect|busy|focus|gave up|lazy|incompeten|lost|ruin|kill)",
        r"\bembark\b.*(?:don.t care|doesn.t care|abandon|neglect|gave up|killed|ruin|incompeten)",
        r"\bразраб\w*\b.*(?:забива|плев|забил|бросил|не.делают|убил)",
        r"\bdead.?game\b.*dev",
        r"\bonly\b.*(?:skin|cosmetic|shop|store).*(?:update|add|new|content)",
        r"\bno\s+(?:real\s+)?(?:update|content|new)\b",
        r"\bтольк\w*\b.*(?:скин|магазин|шоп|фантик)",
    ],
    "Game Design & Modes": [
        r"\bcashout\b.*(?:bad|broken|stupid|unfair|terrible|mess|hate|suck|steal)",
        r"\bvault\b.*(?:steal|stolen|last.second|unfair|stupid|broken)",
        r"\bgame.?mode[s]?\b.*(?:bad|boring|stale|lack|limited|stupid|terrible)",
        r"\bovertime\b",
        r"\bpowershift\b.*(?:bad|boring|terrible|stupid|hate)",
        r"\brepetiti(?:ve|ous)\b",
        r"\bбатл\s*пас\b.*(?:копипаст|одинаков)",
    ],
    "Crashes & Bugs": [
        r"\bcrash(?:es|ed|ing)?\b",
        r"\bbug(?:s|gy)?\b",
        r"\bfreez(?:e[s]?|ing)\b",
        r"\berror\b.*(?:code|system|crash|integrity)",
        r"\bкраш\b", r"\bбаг(?:и|ов)?\b",
        r"\bcan.t.rejoin\b",
        r"\bsettings?\s*reset\b",
        r"\bглюч\b",
        r"\bbroken\b.*(?:replay|system|menu|UI)",
        r"\bошибк\w+\b",
        r"\bне\s*(?:удается|могу)\s*зайти\b",
    ],
    "Low Playerbase": [
        r"\bdead\s*game\b", r"\bdying\s*game\b",
        r"\bno\s*(?:one|body)\s*play(?:s|ing)?\b",
        r"\bplayer.?(?:count|base)\b.*(?:low|dead|dying|drop|declin)",
        r"\bonline\b.*(?:dead|dying|мертв)",
        r"\bмертв\b", r"\bонлайн\b.*(?:мертв|низк|нет)",
        r"\bno\s*wonder\s*no\s*(?:one|body|1)\b",
        r"\bgame\b.*\bdi(?:ed|ying)\b",
    ],
    "Monetization": [
        r"\bpay.?to.?win\b", r"\bp2w\b",
        r"\bbattle.?pass\b.*(?:bad|greedy|expensive|awful|terrible|worst|scam|ridiculous|overpriced)",
        r"\bmicrotransaction[s]?\b.*(?:bad|greedy|expensive|predatory|awful)",
        r"\b(?:skin|cosmetic)s?\b.*(?:expensive|overpriced|greedy|predatory|ridiculous|absurd|pricing|pric)",
        r"\bmonetiz\w+\b.*(?:bad|greedy|predatory|awful|terrible)",
        r"\bдонат\b",
        r"\bpredatory\b",
        r"\bfomo\b",
        r"\boverpriced\b",
        r"\bбатл\s*пас\b.*(?:цен|дорог|грабеж)",
    ],
    "Visibility & Chaos": [
        r"\bvisib(?:ility|le)\b.*(?:bad|terrible|awful|trash|poor|can.t see|zero|no)",
        r"\bcan.t\s+see\b", r"\bwhere\b.*\bwho\b",
        r"\btoo\s+(?:chaotic|much\s+chaos)\b",
        r"\bхаос\b", r"\bне\s*видно\b",
        r"\bclutter\b",
    ],
    "New Player Experience": [
        r"\bnew\s*player\b.*(?:bad|terrible|hostile|hard|rough|steep|unfriendly|punish)",
        r"\blearning\s*curve\b",
        r"\bconfus(?:ing|ed)\b",
        r"\bsteep\b.*\bcurve\b",
        r"\bnoob\b.*(?:unfriendly|hostile|punish)",
        r"\bpunish(?:ing)?\b.*(?:new|beginn|noob|start)",
        r"\bdon.t\s+have\s+all\s+(?:the\s+)?(?:gun|weapon|gadget|unlock)",
    ],
    "Denuvo / DRM": [
        r"\bdenuvo\b",
        r"\bdrm\b",
        r"\bkernel\b.*(?:anti|driver|level)",
        r"\bзащит\w*\b.*(?:denuvo|drm)",
    ],
}

POSITIVE_CATEGORIES = {
    "Fun & Addictive": [
        r"\bfun\b", r"\baddic(?:ting|tive|ted)\b",
        r"\benjoy(?:able|ed|ing|s)?\b",
        r"\blove\b.*(?:game|this|it|play)",
        r"\bblast\b",
        r"\bкайф\b", r"\bлюбим\b",
        r"\bdopamine\b", r"\bserotonin\b",
        r"\bcan.t\s+stop\b",
        r"\bfav(?:orite|ourite)\b",
        r"\b(?:so|very|really|super|insanely|incredibly)\s+good\b",
        r"\bamazing\s+game\b",
        r"\bgreat\s+game\b",
        r"\bgood\s+game\b",
        r"\bpretty\s+good\b",
        r"\bbest\s+(?:fps|shooter|game)\b",
        r"\b(?:10|9|8)\/10\b",
        r"\b(?:10|9|8)\s+out\s+of\s+10\b",
        r"\bpeak\b(?:\s+game|\s+fps|\s+shooter)?",
        r"\bвесёл\w+\b", r"\bвесел\w+\b",
        r"\bкруто\b", r"\bтоп\b",
        r"\bбомб\w*\b",
        r"\bлучш\w+\b.*(?:игр|шутер|fps)",
        r"\brecommend\b",
    ],
    "Destructible Environment": [
        r"\bdestruct(?:ible|ion|ive)\b",
        r"\bdestr(?:oy|oying)\b.*(?:environ|building|wall|floor|map|everything)",
        r"\beverything\b.*\b(?:destroy|break|destruct)\b",
        r"\bразруш\w+\b",
        r"\bphysics\b",
    ],
    "Unique / Fresh FPS": [
        r"\bunique\b", r"\bfresh\b",
        r"\bno\s+other\b.*(?:game|fps|shooter)",
        r"\bnothing\s+(?:like|else)\b",
        r"\bdifferent\b.*(?:fps|shooter|game)",
        r"\brefresh(?:ing)?\b",
        r"\bуникальн\w+\b",
        r"\bnext.?gen\b",
        r"\bone\s+of\s+a\s+kind\b",
        r"\boriginal\b",
        r"\binnovati(?:ve|on|ng)\b",
        r"\bинновац\w+\b", r"\bиниваци\w+\b",
        r"\bslept\s+on\b",
    ],
    "Movement & Fluidity": [
        r"\bmovement\b.*(?:great|good|amazing|best|smooth|fluid|clean|insane|incredible|fast|cool)",
        r"(?:great|good|amazing|best|smooth|fluid|clean|insane|incredible).*\bmovement\b",
        r"\bfluid\b",
        r"\bfast.?paced\b",
        r"\bдинамик\w+\b", r"\bдинамичн\w+\b",
        r"\bшустр\w+\b",
    ],
    "Gunplay": [
        r"\bgunplay\b",
        r"\bgun[s]?\b.*(?:feel|great|good|satisf|clean|tight|snappy|responsive)",
        r"\bshooting\b.*(?:feel|great|good|satisf|clean|tight)",
        r"\bweapon[s]?\b.*(?:feel|great|good|satisf|balanc)",
        r"\brecoil\b.*(?:good|satisf|fair)",
        r"\baim\b.*(?:great|satisf|reward)",
        r"\bстрельб\w+\b.*(?:хорош|круто|кайф|отлич)",
    ],
    "Graphics & Visuals": [
        r"\bgraphic[s]?\b.*(?:great|good|amazing|beautiful|stunning|incredible|best|gorgeous|nice)",
        r"(?:great|good|amazing|beautiful|stunning|incredible|gorgeous).*\bgraphic",
        r"\bvisual(?:s|ly)?\b.*(?:great|good|amazing|beautiful|stunning|incredible)",
        r"\bграфик\w+\b.*(?:отлич|хорош|круто|ахуен|красив)",
        r"\bbeautiful\b",
        r"\bstunning\b",
        r"\bgorgeous\b",
        r"\bsound\s*design\b",
    ],
    "Free-to-Play Value": [
        r"\bfree\b.*(?:play|great|good|amazing|worth|can.t.believe|baffles|crazy|game)",
        r"\bf2p\b",
        r"\bfree.?to.?play\b",
        r"\bno\s+pay.?to.?win\b", r"\bnot\s+p2w\b",
        r"\bit.s\s+free\b",
    ],
    "Game Modes": [
        r"\bgame\s*mode[s]?\b.*(?:great|good|fun|amazing|interesting|varied|different|cool)",
        r"\bcashout\b.*(?:great|good|fun|cool|interesting|love)",
        r"\bmode[s]?\b.*(?:great|good|fun|really|variety|different)",
        r"\bрежим\w*\b.*(?:интересн|круто|хорош)",
    ],
    "Teamwork": [
        r"\bteamwork\b", r"\bteam.?play\b",
        r"\bteam\b.*(?:matter|key|important|great|fun|work|focus)",
        r"\bwith\s+friends?\b",
        r"\bкоманд\w+\b.*(?:игр|важн|шутер)",
        r"\bс\s+друзь\w+\b",
    ],
    "Customization & Skins": [
        r"\bcustomi[sz](?:ation|able|e)\b",
        r"\bskin[s]?\b.*(?:great|good|cool|amazing|best|top|nice|awesome|crazy|lut)",
        r"\bcosmetic[s]?\b.*(?:great|good|cool|amazing|best)",
        r"\bскин\w*\b.*(?:топ|круто|хорош|лют)",
        r"\bкастомизац\w*\b",
        r"\bкосметик\w*\b",
    ],
    "Good Performance": [
        r"\brun[s]?\b.*(?:great|good|well|smooth|fine|perfect|solid)",
        r"\boptimi[sz](?:ed|ation)\b.*(?:great|good|well)",
        r"\bwell.?optimi[sz]ed\b",
        r"\bsmooth\b",
        r"\bperformance\b.*(?:great|good|solid|smooth|rock)",
        r"\bоптимизир\w+\b.*(?:хорош|отлич)",
        r"\bхорош\w*\b.*оптимизир",
    ],
    "Fair Monetization": [
        r"\bbattle.?pass\b.*(?:great|good|worth|fair|generous|best|value|awesome|stupid.?good|amazing)",
        r"\bnot\b.*\bpay.?to.?win\b",
        r"\bfair\b.*\bmonetiz\b",
        r"\b(?:earn|free)\b.*\bpremium\b",
        r"\bmultibucks?\b",
        r"\bmicrotransaction[s]?\b.*(?:not|fair|good|great|reasonable)",
    ],
    "Dynamic & Chaotic": [
        r"\bchaos\b", r"\bchaotic\b",
        r"\bdynamic\b",
        r"\bevery\s+(?:match|game)\b.*\bdifferent\b",
        r"\bnever\s+(?:the\s+)?same\b",
        r"\bхаос\b",
        r"\bэкшон\b", r"\bэкшн\b",
        r"\bсдвг\b",
    ],
    "Class & Gadget System": [
        r"\bgadget[s]?\b.*(?:great|good|cool|fun|amazing|interesting)",
        r"\bclass(?:es)?\b.*(?:great|good|cool|fun|interesting|varied|different)",
        r"\b(?:heavy|light|medium)\b.*(?:great|fun|cool|unique|love)",
        r"\babilit(?:y|ies)\b.*(?:great|good|cool|fun|unique|interesting)",
        r"\bloadout[s]?\b",
        r"\bplaystyle[s]?\b",
    ],
    "Strategic Depth": [
        r"\bstrateg(?:y|ic|ical)\b",
        r"\bnot\s+just\s+(?:aim|shoot|reflex)",
        r"\bcreativ(?:e|ity)\b",
        r"\bthink(?:ing)?\b.*(?:required|needed|matters|game)",
        r"\bbrain\b.*(?:required|needed|use)",
        r"\bskill\s*(?:gap|ceiling)\b",
    ],
    "Underrated": [
        r"\bunderrated\b",
        r"\bunderappreciat\w+\b",
        r"\bdeserve[s]?\s+more\b",
        r"\bhidden\s+gem\b",
        r"\bcriminally\b",
        r"\bнедооценен\w*\b",
        r"\bslept\s+on\b",
    ],
}


def compile_patterns(cat_dict):
    """Compile regex patterns for each category."""
    compiled = {}
    for cat, patterns in cat_dict.items():
        compiled[cat] = [re.compile(p, re.IGNORECASE) for p in patterns]
    return compiled


def classify_review(text, compiled_cats):
    """Return list of matching categories for a review text."""
    matches = []
    for cat, patterns in compiled_cats.items():
        for p in patterns:
            if p.search(text):
                matches.append(cat)
                break
    return matches


def get_playtime_bracket(hours):
    """Assign a playtime bracket label."""
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


def get_month_key(timestamp):
    """Convert unix timestamp to YYYY-MM string."""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m")


def load_seasons(filepath=None):
    if filepath is None:
        filepath = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "seasons.json")
    """Load season definitions and return list of (season, start_ts, end_ts)."""
    with open(filepath, encoding="utf-8") as f:
        seasons = json.load(f)
    result = []
    for s in seasons:
        start_ts = int(datetime.strptime(s["start"], "%Y-%m-%d").replace(
            tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(s["end"], "%Y-%m-%d").replace(
            tzinfo=timezone.utc).timestamp())
        result.append((s["season"], start_ts, end_ts))
    return result


def get_season(timestamp, seasons):
    """Return the season name for a given timestamp."""
    for name, start_ts, end_ts in seasons:
        if start_ts <= timestamp < end_ts:
            return name
    return "Off-season"


def main():
    import argparse
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root_dir, "data")

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=os.path.join(data_dir, "reviews_all.json"),
                        help="Input reviews JSON file")
    parser.add_argument("--output", default=os.path.join(data_dir, "reviews_classified.json"),
                        help="Output classified JSON file")
    args = parser.parse_args()

    with open(args.input, encoding="utf-8") as f:
        data = json.load(f)

    reviews = data["reviews"]
    print(f"Loaded {len(reviews)} reviews from {args.input}\n")

    seasons = load_seasons()
    neg_compiled = compile_patterns(NEGATIVE_CATEGORIES)
    pos_compiled = compile_patterns(POSITIVE_CATEGORIES)

    # Classify each review
    classified = []
    for r in reviews:
        text = r.get("review", "")
        voted_up = r.get("voted_up", True)
        author = r.get("author", {})
        hours = author.get("playtime_forever", 0) / 60
        bracket = get_playtime_bracket(hours)
        ts = r.get("timestamp_created", 0)
        month = get_month_key(ts)
        season = get_season(ts, seasons)
        lang = r.get("language", "")

        if voted_up:
            cats = classify_review(text, pos_compiled)
            sentiment = "positive"
        else:
            cats = classify_review(text, neg_compiled)
            sentiment = "negative"

        classified.append({
            "id": r.get("recommendationid", ""),
            "sentiment": sentiment,
            "categories": cats,
            "playtime_bracket": bracket,
            "month": month,
            "season": season,
            "language": lang,
            "hours": round(hours, 1),
            "votes_up": r.get("votes_up", 0),
            "votes_funny": r.get("votes_funny", 0),
            "timestamp": ts,
            "text": text,
        })

    # Save classified data
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(classified, f, ensure_ascii=False)

    # -----------------------------------------------------------------------
    # Aggregate results
    # -----------------------------------------------------------------------
    pos_reviews = [c for c in classified if c["sentiment"] == "positive"]
    neg_reviews = [c for c in classified if c["sentiment"] == "negative"]

    print("=" * 70)
    print(f"OVERALL: {len(pos_reviews)} positive, {len(neg_reviews)} negative")
    print("=" * 70)

    # --- Top 25 negative reasons ---
    print(f"\n{'='*70}")
    print("TOP NEGATIVE REASONS (overall)")
    print(f"{'='*70}")
    neg_counter = Counter()
    for c in neg_reviews:
        for cat in c["categories"]:
            neg_counter[cat] += 1
    unclassified_neg = sum(1 for c in neg_reviews if not c["categories"])
    print(f"  (Unclassified: {unclassified_neg} / {len(neg_reviews)} = "
          f"{unclassified_neg/len(neg_reviews)*100:.1f}%)\n")
    for i, (cat, count) in enumerate(neg_counter.most_common(25), 1):
        pct = count / len(neg_reviews) * 100
        print(f"  {i:2d}. {cat:<35s} {count:5d}  ({pct:5.1f}%)")

    # --- Top 25 positive reasons ---
    print(f"\n{'='*70}")
    print("TOP POSITIVE REASONS (overall)")
    print(f"{'='*70}")
    pos_counter = Counter()
    for c in pos_reviews:
        for cat in c["categories"]:
            pos_counter[cat] += 1
    unclassified_pos = sum(1 for c in pos_reviews if not c["categories"])
    print(f"  (Unclassified: {unclassified_pos} / {len(pos_reviews)} = "
          f"{unclassified_pos/len(pos_reviews)*100:.1f}%)\n")
    for i, (cat, count) in enumerate(pos_counter.most_common(25), 1):
        pct = count / len(pos_reviews) * 100
        print(f"  {i:2d}. {cat:<35s} {count:5d}  ({pct:5.1f}%)")

    # --- Breakdown by month ---
    months = sorted(set(c["month"] for c in classified))

    print(f"\n{'='*70}")
    print("NEGATIVE REASONS BY MONTH")
    print(f"{'='*70}")
    for month in months:
        month_neg = [c for c in neg_reviews if c["month"] == month]
        if not month_neg:
            continue
        counter = Counter()
        for c in month_neg:
            for cat in c["categories"]:
                counter[cat] += 1
        print(f"\n  --- {month} ({len(month_neg)} negative reviews) ---")
        for i, (cat, count) in enumerate(counter.most_common(10), 1):
            pct = count / len(month_neg) * 100
            print(f"    {i:2d}. {cat:<35s} {count:4d}  ({pct:5.1f}%)")

    print(f"\n{'='*70}")
    print("POSITIVE REASONS BY MONTH")
    print(f"{'='*70}")
    for month in months:
        month_pos = [c for c in pos_reviews if c["month"] == month]
        if not month_pos:
            continue
        counter = Counter()
        for c in month_pos:
            for cat in c["categories"]:
                counter[cat] += 1
        print(f"\n  --- {month} ({len(month_pos)} positive reviews) ---")
        for i, (cat, count) in enumerate(counter.most_common(10), 1):
            pct = count / len(month_pos) * 100
            print(f"    {i:2d}. {cat:<35s} {count:4d}  ({pct:5.1f}%)")

    # --- Breakdown by playtime bracket ---
    brackets = [
        "0-10h (Newcomer)", "10-50h (Casual)", "50-100h (Regular)",
        "100-200h (Dedicated)", "200-500h (Veteran)",
        "500-1000h (Hardcore)", "1000h+ (No-lifer)",
    ]

    print(f"\n{'='*70}")
    print("NEGATIVE REASONS BY PLAYTIME")
    print(f"{'='*70}")
    for bracket in brackets:
        br_neg = [c for c in neg_reviews if c["playtime_bracket"] == bracket]
        if not br_neg:
            continue
        counter = Counter()
        for c in br_neg:
            for cat in c["categories"]:
                counter[cat] += 1
        print(f"\n  --- {bracket} ({len(br_neg)} negative reviews) ---")
        for i, (cat, count) in enumerate(counter.most_common(10), 1):
            pct = count / len(br_neg) * 100
            print(f"    {i:2d}. {cat:<35s} {count:4d}  ({pct:5.1f}%)")

    print(f"\n{'='*70}")
    print("POSITIVE REASONS BY PLAYTIME")
    print(f"{'='*70}")
    for bracket in brackets:
        br_pos = [c for c in pos_reviews if c["playtime_bracket"] == bracket]
        if not br_pos:
            continue
        counter = Counter()
        for c in br_pos:
            for cat in c["categories"]:
                counter[cat] += 1
        print(f"\n  --- {bracket} ({len(br_pos)} positive reviews) ---")
        for i, (cat, count) in enumerate(counter.most_common(10), 1):
            pct = count / len(br_pos) * 100
            print(f"    {i:2d}. {cat:<35s} {count:4d}  ({pct:5.1f}%)")

    # --- Save CSV summary ---
    with open(os.path.join(data_dir, "review_categories_summary.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["sentiment", "month", "playtime_bracket", "category", "count"])
        for sentiment, reviews_list in [("negative", neg_reviews), ("positive", pos_reviews)]:
            by_month_bracket = defaultdict(Counter)
            for c in reviews_list:
                key = (c["month"], c["playtime_bracket"])
                for cat in c["categories"]:
                    by_month_bracket[key][cat] += 1
            for (month, bracket), counter in sorted(by_month_bracket.items()):
                for cat, count in counter.most_common():
                    writer.writerow([sentiment, month, bracket, cat, count])

    print(f"\n\nSaved reviews_classified.json and review_categories_summary.csv")


if __name__ == "__main__":
    main()
