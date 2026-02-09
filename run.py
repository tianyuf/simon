#!/usr/bin/env python
"""Main entry point for Herbert Simon Papers database."""

import argparse
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))


def main():
    parser = argparse.ArgumentParser(description="Herbert Simon Papers Database")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Scrape command
    scrape_parser = subparsers.add_parser("scrape", help="Scrape papers from CMU")
    scrape_parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    scrape_parser.add_argument("--test", action="store_true", help="Test mode: only fetch first page")

    # Serve command
    serve_parser = subparsers.add_parser("serve", help="Start the web server")
    serve_parser.add_argument("--port", type=int, default=5000, help="Port to run on")
    serve_parser.add_argument("--debug", action="store_true", help="Enable debug mode")

    # Init command
    subparsers.add_parser("init", help="Initialize the database")

    # Stats command
    subparsers.add_parser("stats", help="Show database statistics")

    # Download PDFs command
    download_parser = subparsers.add_parser("download", help="Download PDFs from CMU")
    download_parser.add_argument("--limit", type=int, help="Limit number of PDFs to download")
    download_parser.add_argument("--delay", type=float, default=0.5, help="Delay between downloads (seconds)")
    download_parser.add_argument("--no-resume", action="store_true", help="Don't skip existing files")
    download_parser.add_argument("--stats", action="store_true", help="Show download statistics only")

    # OCR command (local PDFs)
    ocr_parser = subparsers.add_parser("ocr", help="OCR local PDFs and extract text")
    ocr_parser.add_argument("--limit", type=int, help="Limit number of PDFs to process")
    ocr_parser.add_argument("--force-ocr", action="store_true", help="Force OCR even if native text exists")
    ocr_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    ocr_parser.add_argument("--stats", action="store_true", help="Show OCR statistics")
    ocr_parser.add_argument("--search", type=str, help="Search within extracted text")

    # Stream OCR command (no local storage)
    stream_parser = subparsers.add_parser("stream-ocr", help="Stream PDFs from CMU and OCR (no local storage)")
    stream_parser.add_argument("--limit", type=int, help="Limit number of PDFs to process")
    stream_parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    stream_parser.add_argument("--force-ocr", action="store_true", help="Force OCR even if native text exists")
    stream_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    stream_parser.add_argument("--stats", action="store_true", help="Show OCR statistics")

    # Analyze command (AI analysis of OCR'd papers)
    analyze_parser = subparsers.add_parser("analyze", help="Analyze OCR'd papers with AI (summaries, tags, language)")
    analyze_parser.add_argument("--limit", type=int, help="Limit number of papers to analyze")
    analyze_parser.add_argument("--delay", type=float, default=0.5, help="Delay between API calls (seconds)")
    analyze_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    analyze_parser.add_argument("--stats", action="store_true", help="Show analysis statistics")

    # Semantic search command (DeepSeek Reasoner)
    search_parser = subparsers.add_parser("search", help="Semantic search using DeepSeek Reasoner")
    search_parser.add_argument("query", nargs="?", help="Search query (omit for interactive mode)")
    search_parser.add_argument("--max-candidates", type=int, default=150, help="Max papers to analyze")
    search_parser.add_argument("--prefilter", "-p", type=str, help="Pre-filter with keyword search first")
    search_parser.add_argument("--no-text", action="store_true", help="Exclude text content excerpts")
    search_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress and reasoning")
    search_parser.add_argument("--json", action="store_true", help="Output results as JSON")

    args = parser.parse_args()

    if args.command == "scrape":
        from scraper.scraper import scrape_and_save, fetch_page, parse_search_results
        from db import init_db

        if args.test:
            init_db()
            print("Test mode: fetching first page only")
            html = fetch_page(0)
            if html:
                papers = parse_search_results(html)
                print(f"Found {len(papers)} papers on first page")
                for p in papers[:3]:
                    print(f"  - {p.get('title', 'N/A')[:60]}...")
                    print(f"    Date: {p.get('date', 'N/A')}, Series: {p.get('series', 'N/A')}")
        else:
            scrape_and_save(delay=args.delay)

    elif args.command == "serve":
        from web.app import app
        from db import init_db
        init_db()
        print(f"Starting server at http://localhost:{args.port}")
        app.run(debug=args.debug, port=args.port)

    elif args.command == "init":
        from db import init_db
        init_db()

    elif args.command == "stats":
        from db import get_facets
        facets = get_facets()
        print(f"\nHerbert Simon Papers Database")
        print(f"{'=' * 40}")
        print(f"Total papers: {facets['total']}")
        print(f"\nBy Series:")
        for series, count in facets['series']:
            print(f"  {series}: {count}")
        print(f"\nBy Item Type (top 10):")
        for item_type, count in facets['item_types'][:10]:
            print(f"  {item_type}: {count}")
        if facets['years']:
            print(f"\nDate range: {facets['years'][0][0]} - {facets['years'][-1][0]}")

    elif args.command == "download":
        from scraper.download_pdfs import download_all_pdfs, get_download_stats
        if args.stats:
            get_download_stats()
        else:
            download_all_pdfs(
                limit=args.limit,
                delay=args.delay,
                resume=not args.no_resume
            )

    elif args.command == "ocr":
        from scraper.ocr_pdfs import ocr_all_pdfs, get_ocr_stats, search_text_content
        if args.stats:
            get_ocr_stats()
        elif args.search:
            search_text_content(args.search)
        else:
            ocr_all_pdfs(
                limit=args.limit,
                force_ocr=args.force_ocr,
                verbose=args.verbose
            )

    elif args.command == "stream-ocr":
        from scraper.stream_ocr import stream_ocr_all, get_streaming_ocr_stats
        if args.stats:
            get_streaming_ocr_stats()
        else:
            stream_ocr_all(
                limit=args.limit,
                delay=args.delay,
                force_ocr=args.force_ocr,
                verbose=args.verbose
            )

    elif args.command == "analyze":
        from scraper.analyze_papers import analyze_all_papers, get_analysis_stats
        if args.stats:
            get_analysis_stats()
        else:
            analyze_all_papers(
                limit=args.limit,
                delay=args.delay,
                verbose=args.verbose
            )

    elif args.command == "search":
        from scraper.semantic_search import semantic_search, print_search_results, interactive_search
        import json as json_module

        if args.query:
            papers, reasoning = semantic_search(
                args.query,
                max_candidates=args.max_candidates,
                prefilter=args.prefilter,
                include_text=not args.no_text,
                verbose=args.verbose
            )

            if args.json:
                output = {
                    "query": args.query,
                    "reasoning": reasoning,
                    "count": len(papers),
                    "results": papers
                }
                print(json_module.dumps(output, indent=2, default=str))
            else:
                print_search_results(papers, reasoning, args.query)
        else:
            interactive_search()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
