"""Stream PDFs from CMU and OCR without saving to disk."""

import io
import sys
import time
import tempfile
import requests
from pathlib import Path
from tqdm import tqdm
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import init_db, get_papers_for_streaming_ocr, update_text_content, update_ocr_status

# Base URL for PDF downloads
PDF_BASE_URL = "http://iiif.library.cmu.edu/file"

# Try to import PDF processing libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

try:
    from pdf2image import convert_from_bytes
    import pytesseract
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False


def construct_doc_id(box: int, folder: int, bundle: int, doc: int) -> str:
    """Construct document ID from archive numbers."""
    return f"Simon_box{box:05d}_fld{folder:05d}_bdl{bundle:04d}_doc{doc:04d}"


def construct_pdf_url(doc_id: str) -> str:
    """Construct PDF URL from document ID."""
    return f"{PDF_BASE_URL}/{doc_id}/{doc_id}.pdf"


def fetch_pdf_bytes(url: str, timeout: int = 30) -> bytes | None:
    """Fetch PDF from URL and return as bytes."""
    try:
        response = requests.get(url, timeout=timeout, verify=False)
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '')
            if 'pdf' in content_type.lower() or len(response.content) > 100:
                return response.content
        return None
    except Exception as e:
        return None


def extract_text_from_bytes_pymupdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyMuPDF."""
    if not PYMUPDF_AVAILABLE:
        return ""

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        return "\n".join(text_parts).strip()
    except Exception as e:
        return ""


def extract_text_from_bytes_tesseract(pdf_bytes: bytes, languages: str = "eng+chi_sim+chi_tra") -> str:
    """Extract text from PDF bytes using Tesseract OCR."""
    if not TESSERACT_AVAILABLE:
        return ""

    try:
        # Convert PDF bytes to images
        images = convert_from_bytes(pdf_bytes, dpi=200)

        text_parts = []
        for image in images:
            text = pytesseract.image_to_string(image, lang=languages)
            text_parts.append(text)

        return "\n".join(text_parts).strip()
    except Exception as e:
        return ""


def extract_text_from_bytes(pdf_bytes: bytes, force_ocr: bool = False) -> tuple[str, str]:
    """
    Extract text from PDF bytes.
    Returns (text, method) where method is 'native', 'ocr', or 'failed'.
    """
    # First try native extraction
    if not force_ocr and PYMUPDF_AVAILABLE:
        text = extract_text_from_bytes_pymupdf(pdf_bytes)
        if len(text.strip()) > 50:
            return text, 'native'

    # Fall back to OCR
    if TESSERACT_AVAILABLE:
        text = extract_text_from_bytes_tesseract(pdf_bytes)
        if text.strip():
            return text, 'ocr'

    return "", 'failed'


def stream_ocr_all(
    limit: int = None,
    delay: float = 0.5,
    force_ocr: bool = False,
    verbose: bool = False
):
    """Stream PDFs from CMU and OCR without saving to disk."""
    # Initialize database
    init_db()

    # Check dependencies
    if not PYMUPDF_AVAILABLE and not TESSERACT_AVAILABLE:
        print("Error: Neither PyMuPDF nor Tesseract is available.")
        print("Install with: pip install PyMuPDF pdf2image pytesseract")
        return

    print(f"Available methods: ", end="")
    if PYMUPDF_AVAILABLE:
        print("PyMuPDF (native text) ", end="")
    if TESSERACT_AVAILABLE:
        print("Tesseract (OCR)", end="")
    print("\n")

    # Get papers to process
    papers = get_papers_for_streaming_ocr(limit=limit)

    if not papers:
        print("No papers to process (all already OCR'd or no archive info)")
        return

    print(f"Found {len(papers)} papers to process (streaming from CMU)")

    processed = 0
    native_count = 0
    ocr_count = 0
    fetch_failed = 0
    ocr_failed = 0

    for paper in tqdm(papers, desc="Streaming & OCR"):
        doc_id = construct_doc_id(
            paper['box_number'],
            paper['folder_number'],
            paper['bundle_number'],
            paper['document_number']
        )
        pdf_url = construct_pdf_url(doc_id)

        # Fetch PDF into memory
        pdf_bytes = fetch_pdf_bytes(pdf_url)

        if pdf_bytes is None:
            update_ocr_status(paper['id'], 'fetch_failed')
            fetch_failed += 1
            if verbose:
                print(f"\n  Fetch failed: {paper['title'][:50]}...")
            time.sleep(delay)
            continue

        # Extract text
        text, method = extract_text_from_bytes(pdf_bytes, force_ocr=force_ocr)

        if text:
            update_text_content(paper['id'], text, 'completed')
            processed += 1
            if method == 'native':
                native_count += 1
            else:
                ocr_count += 1

            if verbose:
                preview = text[:100].replace('\n', ' ')
                print(f"\n  {paper['title'][:50]}...")
                print(f"    Method: {method}, Length: {len(text)} chars")
                print(f"    Preview: {preview}...")
        else:
            update_ocr_status(paper['id'], 'failed')
            ocr_failed += 1
            if verbose:
                print(f"\n  OCR failed: {paper['title'][:50]}...")

        # Rate limiting
        time.sleep(delay)

    print(f"\nStreaming OCR complete:")
    print(f"  Processed: {processed}")
    print(f"    Native text extraction: {native_count}")
    print(f"    OCR: {ocr_count}")
    print(f"  Fetch failed: {fetch_failed}")
    print(f"  OCR failed: {ocr_failed}")


def get_streaming_ocr_stats():
    """Get statistics about OCR processing."""
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

    # OCR status breakdown
    cursor.execute("""
        SELECT ocr_status, COUNT(*) as count
        FROM papers
        WHERE box_number IS NOT NULL
        GROUP BY ocr_status
    """)
    status_counts = {row['ocr_status'] or 'pending': row['count'] for row in cursor.fetchall()}

    # Papers with text content
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
    """)
    with_text = cursor.fetchone()[0]

    # Average text length
    cursor.execute("""
        SELECT AVG(LENGTH(text_content)) as avg_len
        FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
    """)
    avg_len = cursor.fetchone()['avg_len'] or 0

    conn.close()

    print(f"Streaming OCR Statistics:")
    print(f"  Papers with archive info: {total_with_archive}")
    print(f"  Status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")
    print(f"  Papers with extracted text: {with_text}")
    print(f"  Average text length: {avg_len:.0f} chars")
    print(f"  Remaining to process: {status_counts.get('pending', 0)}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stream PDFs from CMU and OCR without saving to disk")
    parser.add_argument("--limit", type=int, help="Limit number of PDFs to process")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
    parser.add_argument("--force-ocr", action="store_true", help="Force OCR even if native text exists")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument("--stats", action="store_true", help="Show OCR statistics")

    args = parser.parse_args()

    if args.stats:
        get_streaming_ocr_stats()
    else:
        stream_ocr_all(
            limit=args.limit,
            delay=args.delay,
            force_ocr=args.force_ocr,
            verbose=args.verbose
        )
