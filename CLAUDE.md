# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a research archive system for Herbert Simon's papers from CMU Digital Collections. It scrapes, processes, and provides search capabilities for Simon's papers including OCR text extraction, AI-powered analysis, and semantic search.

## Common Commands

```bash
# Initialize the database
python run.py init

# Scrape papers from CMU Digital Collections
python run.py scrape [--delay 0.5] [--test]

# Start the web server (Flask)
python run.py serve [--port 5000] [--debug]

# Download PDFs from CMU
python run.py download [--limit N] [--delay 0.5] [--stats]

# OCR local PDFs and extract text
python run.py ocr [--limit N] [--force-ocr] [--verbose] [--stats]

# Stream OCR (process directly from CMU without local storage)
python run.py stream-ocr [--limit N] [--delay 0.5] [--verbose]

# Analyze papers with AI (extract summaries, tags, language)
python run.py analyze [--limit N] [--delay 0.5] [--verbose] [--stats]

# Semantic search using DeepSeek Reasoner
python run.py search "query" [--max-candidates 150] [--prefilter keyword] [--verbose]

# Show database statistics
python run.py stats
```

## Architecture

### Data Pipeline

1. **Scraping** (`scraper/scraper.py`): Fetches paper metadata from CMU Digital Collections, extracts box/folder/document IDs from thumbnail URLs
2. **PDF Download** (`scraper/download_pdfs.py`): Downloads PDFs using box/folder/bundle/document identifiers
3. **OCR** (`scraper/ocr_pdfs.py`, `scraper/stream_ocr.py`): Extracts text using PyMuPDF or Tesseract
4. **Analysis** (`scraper/analyze_papers.py`): Uses DeepSeek API (with Anthropic fallback) to generate summaries, tags, and detect language
5. **Search** (`scraper/semantic_search.py`): DeepSeek Reasoner for semantic search; (`scraper/research_assistant.py`): Claude Sonnet for Q&A

### Database

SQLite database at `db/simon_papers.db` with:
- `papers` table: metadata, OCR text, AI summaries, tags, starred status
- `papers_fts` FTS5 virtual table for full-text search
- `archive_summaries` table for box/folder summaries

Key columns in `papers`: `node_id`, `title`, `date`, `series`, `item_type`, `box_number`, `folder_number`, `bundle_number`, `document_number`, `text_content`, `summary`, `tags` (JSON), `language`, `ocr_status`, `analysis_status`

### Web Interface

Flask app (`web/app.py`) providing:
- Main search with faceted filtering (series, type, date, box/folder)
- Search modes: normal FTS5, fuzzy (LIKE with wildcards), regex
- Archive browser by physical structure
- Paper detail view with PDF viewing
- Research assistant chat (Claude Sonnet)
- Star/bookmark functionality

## Environment Variables

Required in `.env`:
- `DEEPSEEK_API_KEY`: For analysis and semantic search
- `ANTHROPIC_API_KEY`: For research assistant and analysis fallback

## Key Patterns

- CMU PDF URL format: `https://digitalcollections.library.cmu.edu/files/simon/box{BOX}/fld{FOLDER}/bdl{BUNDLE}/Simon_box{BOX}_fld{FOLDER}_bdl{BUNDLE}_doc{DOC}.pdf`
- Archive ID extraction from thumbnail filenames: `Simon_box00069_fld05305_bdl0001_doc0001.jpg`
- Database functions are centralized in `db/__init__.py` and `db/database.py`
