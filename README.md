# Fang’s Searchable Simon Papers Archive

A research archive system for [Herbert A. Simon's papers](https://digitalcollections.library.cmu.edu/) from the Carnegie Mellon University Digital Collections. Scrapes, processes, and provides full-text search over thousands of archival documents including correspondence, manuscripts, reports, and other materials.

The pipeline downloads paper metadata and PDFs from CMU, extracts text via OCR, generates AI-powered summaries and tags, and serves everything through a web interface with faceted search and an archive browser.

This was built with Claude Code.

## Setup

**Requirements:** Python 3.10+, Tesseract (optional, for scanned PDFs)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file:

```
DEEPSEEK_API_KEY=your_key_here
ANTHROPIC_API_KEY=your_key_here
```

- **DeepSeek**: Used for paper analysis (summaries/tags)
- **Anthropic**: Used as an analysis fallback

## Usage

### Data Pipeline

Run these in order to build the database from scratch:

```bash
# 1. Initialize the database
python run.py init

# 2. Scrape paper metadata from CMU Digital Collections
python run.py scrape

# 3. Download PDFs
python run.py download

# 4. Extract text from PDFs via OCR
python run.py ocr

# 5. Generate AI summaries, tags, and language detection
python run.py analyze
```

Alternatively, step 3+4 can be combined with `stream-ocr`, which processes PDFs directly from CMU without storing them locally:

```bash
python run.py stream-ocr
```

### Web Interface

```bash
python run.py serve --port 5000
```

Features:
- **Full-text search** with FTS5, fuzzy (LIKE wildcards), and regex modes
- **Faceted filtering** by series, item type, date range, box/folder, language, and tags
- **Archive browser** organized by physical box/folder structure
- **Paper detail view** with PDF viewer, AI summary, and related papers
- **Star/bookmark** papers for later reference

### Other Commands

```bash
python run.py stats                    # Database statistics
python run.py download --stats         # Download progress
python run.py ocr --stats              # OCR progress
python run.py analyze --stats          # Analysis progress
```

## Project Structure

```
run.py                  CLI entry point
db/
  database.py           SQLite database operations, FTS5 search
scraper/
  scraper.py            CMU Digital Collections scraper
  download_pdfs.py      PDF downloader
  ocr_pdfs.py           Local PDF text extraction (PyMuPDF + Tesseract)
  stream_ocr.py         Stream-and-OCR without local storage
  analyze_papers.py     AI analysis (DeepSeek / Anthropic)
  summarize_archive.py  Box/folder summary generation
  normalize_tags.py     Tag normalization utilities
web/
  app.py                Flask application
  templates/            Jinja2 templates
  static/               Static assets
deploy/                 Nginx + systemd deployment configs
```

## Deployment

See [deploy/DEPLOY.md](deploy/DEPLOY.md) for production deployment with Gunicorn + Nginx on a Linux server.
