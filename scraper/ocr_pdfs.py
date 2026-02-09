"""OCR PDFs and extract text content for search."""

import sys
from pathlib import Path
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import init_db, get_papers_for_ocr, update_text_content, update_ocr_status

# PDF directory
PDF_DIR = Path(__file__).parent.parent / "pdfs"

# Try to import PDF processing libraries
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

try:
    from pdf2image import convert_from_path
    import pytesseract
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False


def extract_text_pymupdf(pdf_path: Path) -> str:
    """Extract text from PDF using PyMuPDF (fast, works for PDFs with text layer)."""
    if not PYMUPDF_AVAILABLE:
        return ""

    try:
        doc = fitz.open(pdf_path)
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        return "\n".join(text_parts).strip()
    except Exception as e:
        print(f"\nPyMuPDF error for {pdf_path}: {e}")
        return ""


def extract_text_tesseract(pdf_path: Path, languages: str = "eng+chi_sim+chi_tra") -> str:
    """Extract text from PDF using Tesseract OCR (slower, works for scanned documents)."""
    if not TESSERACT_AVAILABLE:
        return ""

    try:
        # Convert PDF to images
        images = convert_from_path(pdf_path, dpi=200)

        text_parts = []
        for i, image in enumerate(images):
            # Run OCR on each page
            text = pytesseract.image_to_string(image, lang=languages)
            text_parts.append(text)

        return "\n".join(text_parts).strip()
    except Exception as e:
        print(f"\nTesseract error for {pdf_path}: {e}")
        return ""


def extract_text_from_pdf(pdf_path: Path, force_ocr: bool = False) -> tuple[str, str]:
    """
    Extract text from PDF.
    Returns (text, method) where method is 'native', 'ocr', or 'failed'.

    Strategy:
    1. Try native text extraction first (fast)
    2. If no text or very little text, fall back to OCR
    """
    # First try native extraction
    if not force_ocr and PYMUPDF_AVAILABLE:
        text = extract_text_pymupdf(pdf_path)
        # If we got meaningful text (more than 50 chars), use it
        if len(text.strip()) > 50:
            return text, 'native'

    # Fall back to OCR
    if TESSERACT_AVAILABLE:
        text = extract_text_tesseract(pdf_path)
        if text.strip():
            return text, 'ocr'

    return "", 'failed'


def ocr_all_pdfs(
    limit: int = None,
    force_ocr: bool = False,
    verbose: bool = False
):
    """OCR all PDFs that haven't been processed yet."""
    # Initialize database (adds new columns if needed)
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
    print()

    # Get papers to OCR
    papers = get_papers_for_ocr(limit=limit)

    if not papers:
        print("No papers to OCR (all PDFs already processed or no local PDFs)")
        return

    print(f"Found {len(papers)} papers to process")

    processed = 0
    native_count = 0
    ocr_count = 0
    failed = 0

    for paper in tqdm(papers, desc="Processing PDFs"):
        pdf_path = PDF_DIR / paper['local_pdf_path']

        if not pdf_path.exists():
            update_ocr_status(paper['id'], 'no_pdf')
            failed += 1
            continue

        text, method = extract_text_from_pdf(pdf_path, force_ocr=force_ocr)

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
            failed += 1
            if verbose:
                print(f"\n  Failed: {paper['title'][:50]}...")

    print(f"\nOCR complete:")
    print(f"  Processed: {processed}")
    print(f"    Native text extraction: {native_count}")
    print(f"    OCR: {ocr_count}")
    print(f"  Failed: {failed}")


def get_ocr_stats():
    """Get statistics about OCR processing."""
    from db import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    # Total papers with local PDF
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE local_pdf_path IS NOT NULL AND local_pdf_path != ''
    """)
    total_with_pdf = cursor.fetchone()[0]

    # OCR status breakdown
    cursor.execute("""
        SELECT ocr_status, COUNT(*) as count
        FROM papers
        WHERE local_pdf_path IS NOT NULL AND local_pdf_path != ''
        GROUP BY ocr_status
    """)
    status_counts = {row['ocr_status'] or 'pending': row['count'] for row in cursor.fetchall()}

    # Average text length
    cursor.execute("""
        SELECT AVG(LENGTH(text_content)) as avg_len
        FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
    """)
    avg_len = cursor.fetchone()['avg_len'] or 0

    # Papers with text content
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
    """)
    with_text = cursor.fetchone()[0]

    conn.close()

    print(f"OCR Statistics:")
    print(f"  Papers with local PDF: {total_with_pdf}")
    print(f"  Status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")
    print(f"  Papers with extracted text: {with_text}")
    print(f"  Average text length: {avg_len:.0f} chars")


def search_text_content(query: str, limit: int = 10):
    """Search within extracted text content (for testing)."""
    from db import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT p.id, p.title, p.text_content
        FROM papers p
        JOIN papers_fts fts ON p.id = fts.rowid
        WHERE papers_fts MATCH ?
        LIMIT ?
    """, (query, limit))

    results = cursor.fetchall()
    conn.close()

    print(f"Search results for '{query}':")
    for row in results:
        print(f"\n  [{row['id']}] {row['title'][:60]}...")
        if row['text_content']:
            # Find and show context around the match
            text = row['text_content'].lower()
            query_lower = query.lower()
            pos = text.find(query_lower)
            if pos >= 0:
                start = max(0, pos - 50)
                end = min(len(text), pos + len(query) + 50)
                context = row['text_content'][start:end].replace('\n', ' ')
                print(f"    ...{context}...")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OCR PDFs and extract text")
    parser.add_argument("--limit", type=int, help="Limit number of PDFs to process")
    parser.add_argument("--force-ocr", action="store_true", help="Force OCR even if native text exists")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument("--stats", action="store_true", help="Show OCR statistics")
    parser.add_argument("--search", type=str, help="Search within extracted text")

    args = parser.parse_args()

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
