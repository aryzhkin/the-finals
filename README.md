# THE FINALS Steam Reviews Analysis

Interactive dashboard analyzing **247,453 Steam reviews** for [THE FINALS](https://store.steampowered.com/app/2073850/THE_FINALS/) using a two-stage AI pipeline.

**[Live Dashboard](https://andruha.github.io/the-finals/)** (GitHub Pages)

## What's Inside

- **42-category AI classification** (30 negative, 12 positive) across all 29 languages
- **440K extracted issues** with game entity normalization
- **Season-by-season tracking** (Seasons 0-9) with patch correlation
- **Playtime cohort analysis** — how sentiment shifts from newcomers to veterans
- **Language & regional breakdowns** with per-language top issues
- **Review Explorer** — drill down to real reviews by category, issue, season, and playtime

## Pipeline Architecture

```
scrape_all_reviews.py     Parallel Steam API scraper → reviews_all.json (247K reviews)
        ↓
classify_ai.py            Stage 1: AI category classification → reviews_ai_classified.json
        ↓
extract_issues.py         Stage 2: Issue extraction with entity normalization → reviews_issues.json
        ↓
normalize_issues.py       Post-processing: synonym merging, deduplication
        ↓
prepare_dashboard_data.py Pre-aggregation → docs/dashboard_data.json + docs/issue_samples.json
        ↓
docs/index.html           Static HTML + Plotly.js dashboard (GitHub Pages)
```

**AI Model:** `google/gemini-2.5-flash-lite` via [PayPerQ](https://ppq.ai) (OpenAI-compatible API). Total cost: **$9.30** for all 247K reviews.

## Quick Start

### Prerequisites
- Python 3.10+
- A PayPerQ API key (or any OpenAI-compatible endpoint)

### Setup

```bash
git clone https://github.com/andruha/the-finals.git
cd the-finals
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your API key
```

### 1. Scrape Reviews

```bash
python3 scrape_all_reviews.py
```

Downloads all Steam reviews for THE FINALS (appid 2073850). Takes ~15 minutes. Outputs `reviews_all.json`.

### 2. AI Classification (Stage 1)

```bash
python3 classify_ai.py
```

Classifies each review into 42 categories. Supports resume via `classify_progress.json`. Cost: ~$3.30 for 247K reviews.

### 3. Issue Extraction (Stage 2)

```bash
python3 extract_issues.py
```

Extracts specific complaints, suggestions, and praise with game entity normalization. Supports resume. Cost: ~$6.00.

### 4. Normalize & Aggregate

```bash
python3 normalize_issues.py
python3 prepare_dashboard_data.py
```

### 5. View Dashboard

```bash
cd docs && python3 -m http.server 8080
# Open http://localhost:8080
```

## Project Structure

| File | Description |
|------|-------------|
| `scrape_all_reviews.py` | Parallel Steam API scraper |
| `classify_ai.py` | Stage 1: AI category classification |
| `extract_issues.py` | Stage 2: AI issue extraction |
| `normalize_issues.py` | Post-processing normalization |
| `prepare_dashboard_data.py` | Pre-aggregation for dashboard |
| `validate_data.py` | Data integrity validation |
| `categories_final.json` | 42 category definitions |
| `game_entities.json` | Canonical game entities + multilingual aliases |
| `patch_notes.json` | 9 season-opening patch notes |
| `seasons.json` | Season date ranges (S0-S9) |
| `docs/index.html` | Static HTML dashboard |
| `docs/dashboard_data.json` | Pre-aggregated dashboard data |
| `docs/issue_samples.json` | Anonymized review samples |

## Data & Privacy

- **Raw review data** (`reviews_*.json`) is gitignored — scrape your own copy
- **Published data** (`docs/`) contains only aggregated statistics and anonymized samples (no Steam IDs, no usernames)
- **API keys** are loaded from `.env` (gitignored)

## Adapting for Other Games

1. Change `APP_ID` in `scrape_all_reviews.py` to your game's Steam appid
2. Update `categories_final.json` with categories relevant to your game
3. Update `game_entities.json` with your game's weapons, abilities, etc.
4. Update `seasons.json` with your game's season dates
5. Run the full pipeline

## Credits

- **Game:** [THE FINALS](https://www.reachthefinals.com/) by Embark Studios
- **Reviews data:** [Steam Store Reviews API](https://partner.steamgames.com/doc/store/getreviews)
- **AI:** [PayPerQ](https://ppq.ai) (google/gemini-2.5-flash-lite)
- **Dashboard:** Plotly.js, vanilla HTML/CSS/JS
