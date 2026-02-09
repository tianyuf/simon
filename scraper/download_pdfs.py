"""Download PDFs from CMU Digital Collections."""

import os
import sys
import time
import requests
from pathlib import Path
from tqdm import tqdm
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import init_db, get_papers_for_download, update_local_pdf_path

# Base URL for PDF downloads
PDF_BASE_URL = "http://iiif.library.cmu.edu/file"

# Directory to store PDFs
PDF_DIR = Path(__file__).parent.parent / "pdfs"


def construct_doc_id(box: int, folder: int, bundle: int, doc: int) -> str:
    """Construct document ID from archive numbers."""
    return f"Simon_box{box:05d}_fld{folder:05d}_bdl{bundle:04d}_doc{doc:04d}"


def construct_pdf_url(doc_id: str) -> str:
    """Construct PDF URL from document ID."""
    return f"{PDF_BASE_URL}/{doc_id}/{doc_id}.pdf"


def download_pdf(url: str, dest_path: Path, timeout: int = 30) -> bool:
    """Download a PDF from URL to destination path."""
    try:
        response = requests.get(url, timeout=timeout, verify=False, stream=True)
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '')
            if 'pdf' in content_type.lower() or response.headers.get('content-length', '0') != '0':
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dest_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
        return False
    except Exception as e:
        print(f"\nError downloading {url}: {e}")
        return False


def download_all_pdfs(
    limit: int = None,
    delay: float = 0.5,
    resume: bool = True
):
    """Download all PDFs that haven't been downloaded yet."""
    # Initialize database (adds new columns if needed)
    init_db()

    # Create PDF directory
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    # Get papers to download
    papers = get_papers_for_download(limit=limit)

    if not papers:
        print("No papers to download (all PDFs already downloaded or no archive info)")
        return

    print(f"Found {len(papers)} papers to download")

    downloaded = 0
    failed = 0
    skipped = 0

    for paper in tqdm(papers, desc="Downloading PDFs"):
        doc_id = construct_doc_id(
            paper['box_number'],
            paper['folder_number'],
            paper['bundle_number'],
            paper['document_number']
        )

        # Organize by box/folder
        relative_path = f"box{paper['box_number']:05d}/folder{paper['folder_number']:05d}/{doc_id}.pdf"
        dest_path = PDF_DIR / relative_path

        # Skip if already exists (for resume functionality)
        if resume and dest_path.exists() and dest_path.stat().st_size > 0:
            update_local_pdf_path(paper['id'], relative_path)
            skipped += 1
            continue

        # Construct URL and download
        pdf_url = construct_pdf_url(doc_id)

        if download_pdf(pdf_url, dest_path):
            update_local_pdf_path(paper['id'], relative_path)
            downloaded += 1
        else:
            failed += 1

        # Rate limiting
        time.sleep(delay)

    print(f"\nDownload complete:")
    print(f"  Downloaded: {downloaded}")
    print(f"  Skipped (already exists): {skipped}")
    print(f"  Failed: {failed}")


def get_download_stats():
    """Get statistics about PDF downloads."""
    from db import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    # Total papers with archive info
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE box_number IS NOT NULL
          AND folder_number IS NOT NULL
          AND bundle_number IS NOT NULL
          AND document_number IS NOT NULL
    """)
    total_with_archive = cursor.fetchone()[0]

    # Papers with local PDF
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE local_pdf_path IS NOT NULL AND local_pdf_path != ''
    """)
    with_local_pdf = cursor.fetchone()[0]

    # Total PDF size
    total_size = 0
    if PDF_DIR.exists():
        for pdf_file in PDF_DIR.rglob("*.pdf"):
            total_size += pdf_file.stat().st_size

    conn.close()

    print(f"PDF Download Statistics:")
    print(f"  Papers with archive info: {total_with_archive}")
    print(f"  PDFs downloaded: {with_local_pdf}")
    print(f"  Remaining: {total_with_archive - with_local_pdf}")
    print(f"  Total size: {total_size / (1024*1024*1024):.2f} GB")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download PDFs from CMU Digital Collections")
    parser.add_argument("--limit", type=int, help="Limit number of PDFs to download")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between downloads (seconds)")
    parser.add_argument("--stats", action="store_true", help="Show download statistics")
    parser.add_argument("--no-resume", action="store_true", help="Don't skip existing files")

    args = parser.parse_args()

    if args.stats:
        get_download_stats()
    else:
        download_all_pdfs(
            limit=args.limit,
            delay=args.delay,
            resume=not args.no_resume
        )
