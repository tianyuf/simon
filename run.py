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
    serve_parser.add_argument("--host", type=str, default=None,
                             help="Host to bind to (default: 127.0.0.1, env: HOST)")
    serve_parser.add_argument("--port", type=int, default=None,
                             help="Port to run on (default: 5000, env: PORT)")
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

    # Load finding aid guide
    guide_parser = subparsers.add_parser("load-guide", help="Load finding aid data from guide file")
    guide_parser.add_argument("--guide-path", type=str, default="guide", help="Path to the guide file")

    # R2 mirror command
    r2_parser = subparsers.add_parser("r2-mirror", help="Mirror PDFs to Cloudflare R2")
    r2_parser.add_argument("--limit", type=int, help="Limit number of PDFs to upload")
    r2_parser.add_argument("--dry-run", action="store_true", help="Show what would be done without uploading")
    r2_parser.add_argument("--stats", action="store_true", help="Show R2 mirror statistics")
    r2_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    r2_parser.add_argument("--verify", type=int, metavar="PAPER_ID", help="Verify a specific paper's R2 upload")
    r2_parser.add_argument("--stream", action="store_true", help="Stream PDFs directly from CMU (no local storage)")
    r2_parser.add_argument("--delay", type=float, default=0.5, help="Delay between uploads in seconds (streaming mode)")

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
        import os
        from web.app import app
        from db import init_db
        init_db()

        # Get settings from args or environment variables
        host = args.host or os.environ.get('HOST', '127.0.0.1')
        port = args.port or int(os.environ.get('PORT', 5000))
        debug = args.debug or os.environ.get('DEBUG', '').lower() in ('1', 'true', 'yes')

        print(f"Starting server at http://{host}:{port}")
        app.run(debug=debug, host=host, port=port)

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

    elif args.command == "load-guide":
        from scraper.parse_guide import parse_guide, print_summary
        from db import init_db, load_finding_aid, insert_missing_papers

        init_db()
        print(f"Parsing finding aid from: {args.guide_path}")
        boxes, folders = parse_guide(args.guide_path)
        print_summary(boxes, folders)

        print("\nLoading into database...")
        load_finding_aid(boxes, folders)

        from db import get_missing_from_collection
        data = get_missing_from_collection()
        stats = data['stats']
        print(f"\nDigital Collection Coverage:")
        print(f"  Boxes: {stats['digitized_boxes']}/{stats['total_boxes']} "
              f"({stats['missing_boxes']} missing)")
        print(f"  Folders: {stats['digitized_folders']}/{stats['total_folders']} "
              f"({stats['missing_folders']} missing)")

        print("\nCreating paper entries for missing folders...")
        count = insert_missing_papers()
        print(f"  Inserted {count} placeholder entries into papers table")

    elif args.command == "r2-mirror":
        from scraper.r2_mirror import mirror_all_pdfs, get_r2_mirror_stats, verify_r2_upload

        if args.stats:
            get_r2_mirror_stats()
        elif args.verify:
            exists = verify_r2_upload(args.verify)
            print(f"Paper {args.verify}: {'Found' if exists else 'Not found'} in R2")
        else:
            mirror_all_pdfs(
                limit=args.limit,
                dry_run=args.dry_run,
                verbose=args.verbose,
                stream=args.stream,
                delay=args.delay
            )

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
