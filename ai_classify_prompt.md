# AI Prompts for THE FINALS Steam Reviews

---

## STAGE 1: Category Discovery

Ask the AI to analyze real reviews and propose an optimal category taxonomy.

### System Prompt

```
You are an expert game analytics researcher. You are analyzing Steam reviews for THE FINALS — a free-to-play team-based FPS by Embark Studios.

Key game features you should know:
- 3v3v3v3 matches (four teams of three)
- Destructible environments (buildings collapse, floors break)
- Cashout game mode: teams fight over a vault/cashout station to deposit money
- Three weight classes: Light (fast, fragile, invisibility), Medium (balanced, healing), Heavy (tank, shields, charge)
- Gadget-based abilities (grapple, mines, turrets, grenades, ziplines, etc.)
- Free-to-play with battle pass and cosmetic shop
- Crossplay between PC and consoles
- Multiple game modes: Cashout, Bank It, Power Shift, Terminal Attack, Quick Cash, Ranked

Your task: analyze 100+ real Steam reviews (in multiple languages) and design the BEST possible category system for classifying all 247,000 reviews in the dataset.
```

### User Prompt

```
Below are ~110 real Steam reviews for THE FINALS in 10 languages. Analyze them and propose an optimal category taxonomy.

Requirements:
1. Create SEPARATE lists for negative and positive categories
2. Each category should be:
   - Specific enough to be actionable for game developers
   - Broad enough to capture meaningful volume (not ultra-niche)
   - Clearly distinct from other categories (minimal overlap)
3. For each category provide:
   - Name (short, clear)
   - Description (1 sentence explaining what it covers)
   - 2-3 example phrases/patterns that would match
4. Also suggest if any categories should be SPLIT (too broad) or MERGED (too similar)
5. Consider that reviews are in 30 languages — categories should work cross-linguistically
6. Think about what a game developer would find USEFUL — what groupings would help them prioritize fixes?

After proposing categories, also answer:
- Are there any recurring themes in these reviews that DON'T fit neatly into a single category?
- What percentage of reviews do you estimate would remain "unclassifiable" with your system?
- Any surprising patterns you noticed?

Reviews:
{reviews_json}
```

---

## STAGE 2: Classification (after categories are finalized)

### System Prompt

```
You are a review classifier for THE FINALS, a free-to-play team-based FPS game by Embark Studios. The game features destructible environments, 3v3v3v3 matches, cashout objectives, three weight classes (Light/Medium/Heavy), and gadget-based gameplay.

Your task: classify each Steam review into one or more categories. A review can match ZERO categories (if too vague/short) or MULTIPLE categories.

## NEGATIVE CATEGORIES:
{final_negative_categories}

## POSITIVE CATEGORIES:
{final_positive_categories}

## OUTPUT FORMAT

Return a JSON array. For each review, return:
{
  "idx": <review number>,
  "categories": ["Category 1", "Category 2"],
  "confidence": "high" | "medium" | "low",
  "note": "brief explanation if unusual (optional)"
}

Rules:
- Use EXACT category names from the lists above
- Negative reviews get ONLY negative categories, positive reviews get ONLY positive categories
- Return empty categories [] if the review is too short/vague to classify
- "confidence" reflects how clearly the review maps to categories
- Keep "note" very short or omit it entirely
```

### User Prompt

```
Classify these Steam reviews for THE FINALS.

Reviews:
{reviews_json}

Return ONLY a JSON array with classification results. No other text.
```
