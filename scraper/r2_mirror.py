"""Cloudflare R2 mirroring for PDF files.

R2 is S3-compatible, so we use boto3 to interact with it.
Environment variables required:
- R2_ACCOUNT_ID: Cloudflare account ID
- R2_ACCESS_KEY_ID: R2 access key ID
- R2_SECRET_ACCESS_KEY: R2 secret access key
- R2_BUCKET_NAME: Name of the R2 bucket
- R2_PUBLIC_URL (optional): Custom public URL for serving files (e.g., https://cdn.example.com)
"""

import os
import sys
import io
import requests
import urllib3
from pathlib import Path
from typing import Optional
from tqdm import tqdm

# Disable SSL warnings for CMU downloads
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import (
    init_db, get_papers_for_r2_upload, update_r2_key, get_r2_stats,
    get_papers_for_r2_streaming
)

# CMU PDF download URL pattern (from download_pdfs.py)
PDF_BASE_URL = "http://iiif.library.cmu.edu/file"

# PDF directory
PDF_DIR = Path(__file__).parent.parent / "pdfs"

# R2 Configuration from environment
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME')
R2_PUBLIC_URL = os.environ.get('R2_PUBLIC_URL', '')

# Check if R2 is configured
R2_AVAILABLE = bool(R2_PUBLIC_URL or (R2_ACCOUNT_ID and R2_BUCKET_NAME))


def get_r2_client():
    """Create and return an S3 client configured for Cloudflare R2."""
    import boto3
    from botocore.config import Config

    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME]):
        raise ValueError(
            "Missing R2 credentials. Please set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, "
            "R2_SECRET_ACCESS_KEY, and R2_BUCKET_NAME environment variables."
        )

    # R2 endpoint URL
    endpoint_url = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

    # Create S3 client with R2-compatible config
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto',  # R2 uses 'auto' as region
        config=Config(
            retries={'max_attempts': 3},
            connect_timeout=10,
            read_timeout=30,
        )
    )

    return s3


def construct_r2_key(box: int, folder: int, bundle: int, doc: int) -> str:
    """Construct the R2 key (path) for a document.

    Follows the same structure as local storage:
    box00069/folder05305/Simon_box00069_fld05305_bdl0001_doc0001.pdf
    """
    doc_id = f"Simon_box{box:05d}_fld{folder:05d}_bdl{bundle:04d}_doc{doc:04d}"
    return f"box{box:05d}/folder{folder:05d}/{doc_id}.pdf"


def upload_pdf_to_r2(
    local_path: Path,
    r2_key: str,
    s3_client=None,
    dry_run: bool = False
) -> bool:
    """Upload a PDF file to R2.

    Args:
        local_path: Path to the local PDF file
        r2_key: The key (path) in the R2 bucket
        s3_client: Optional pre-configured S3 client
        dry_run: If True, don't actually upload (for testing)

    Returns:
        True if successful, False otherwise
    """
    if not local_path.exists():
        print(f"Local file not found: {local_path}")
        return False

    if dry_run:
        print(f"[DRY RUN] Would upload {local_path} to R2 key: {r2_key}")
        return True

    try:
        if s3_client is None:
            s3_client = get_r2_client()

        # Upload with metadata
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            ExtraArgs={
                'ContentType': 'application/pdf',
                'Metadata': {
                    'source': 'herbert-simon-papers-archive',
                    'box': r2_key.split('/')[0],
                    'folder': r2_key.split('/')[1] if len(r2_key.split('/')) > 1 else '',
                }
            }
        )
        return True

    except Exception as e:
        print(f"\nError uploading {local_path} to R2: {e}")
        return False


def construct_doc_id(box: int, folder: int, bundle: int, doc: int) -> str:
    """Construct document ID from archive numbers."""
    return f"Simon_box{box:05d}_fld{folder:05d}_bdl{bundle:04d}_doc{doc:04d}"


def construct_pdf_url(doc_id: str) -> str:
    """Construct PDF URL from document ID."""
    return f"{PDF_BASE_URL}/{doc_id}/{doc_id}.pdf"


def stream_upload_to_r2(
    box: int,
    folder: int,
    bundle: int,
    doc: int,
    s3_client=None,
    dry_run: bool = False,
    timeout: int = 60
) -> tuple[bool, str]:
    """Stream a PDF directly from CMU to R2 without local storage.

    Args:
        box: Box number
        folder: Folder number
        bundle: Bundle number
        doc: Document number
        s3_client: Optional pre-configured S3 client
        dry_run: If True, don't actually upload
        timeout: Request timeout in seconds

    Returns:
        Tuple of (success: bool, r2_key: str)
    """
    doc_id = construct_doc_id(box, folder, bundle, doc)
    pdf_url = construct_pdf_url(doc_id)
    r2_key = construct_r2_key(box, folder, bundle, doc)

    if dry_run:
        print(f"[DRY RUN] Would stream {pdf_url} to R2 key: {r2_key}")
        return True, r2_key

    try:
        if s3_client is None:
            s3_client = get_r2_client()

        # Download from CMU to memory buffer
        response = requests.get(pdf_url, timeout=timeout, verify=False, stream=True)
        if response.status_code != 200:
            print(f"\nFailed to download from CMU: {pdf_url} (status {response.status_code})")
            return False, r2_key

        # Check content type and size
        content_type = response.headers.get('content-type', '')
        content_length = response.headers.get('content-length')

        if 'pdf' not in content_type.lower() and content_length == '0':
            print(f"\nNot a valid PDF: {pdf_url}")
            return False, r2_key

        # Stream to memory buffer
        pdf_buffer = io.BytesIO()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                pdf_buffer.write(chunk)

        pdf_buffer.seek(0)

        # Upload buffer to R2
        s3_client.upload_fileobj(
            Fileobj=pdf_buffer,
            Bucket=R2_BUCKET_NAME,
            Key=r2_key,
            ExtraArgs={
                'ContentType': 'application/pdf',
                'Metadata': {
                    'source': 'herbert-simon-papers-archive',
                    'box': f"box{box:05d}",
                    'folder': f"folder{folder:05d}",
                    'doc_id': doc_id,
                }
            }
        )

        return True, r2_key

    except requests.exceptions.Timeout:
        print(f"\nTimeout downloading {pdf_url}")
        return False, r2_key
    except Exception as e:
        print(f"\nError streaming {pdf_url} to R2: {e}")
        return False, r2_key


def mirror_all_pdfs(
    limit: int = None,
    dry_run: bool = False,
    verbose: bool = False,
    stream: bool = False,
    delay: float = 0.5
):
    """Upload PDFs to R2 from local storage or by streaming from CMU.

    Args:
        limit: Maximum number of PDFs to upload (None for all)
        dry_run: If True, show what would be done without uploading
        verbose: Show detailed output
        stream: If True, stream directly from CMU without local storage
        delay: Delay between uploads in seconds (for rate limiting)
    """
    # Initialize database (adds new columns if needed)
    init_db()

    # Check R2 credentials
    try:
        s3_client = get_r2_client()
    except ValueError as e:
        print(f"Error: {e}")
        return

    if stream:
        # Streaming mode: get papers that need R2 upload (from CMU directly)
        papers = get_papers_for_r2_streaming(limit=limit)
        source_desc = "from CMU (streaming)"
    else:
        # Local mode: get papers with local PDFs that need R2 upload
        papers = get_papers_for_r2_upload(limit=limit)
        source_desc = "from local storage"

    if not papers:
        print(f"No papers to upload (all PDFs already mirrored to R2)")
        return

    print(f"Found {len(papers)} papers to upload to R2 {source_desc}")
    if dry_run:
        print("DRY RUN MODE: No actual uploads will occur")
    if stream:
        print(f"Streaming mode: downloading from CMU and uploading directly to R2")
        print(f"Rate limiting: {delay}s delay between uploads")

    uploaded = 0
    failed = 0
    skipped = 0
    import time

    for paper in tqdm(papers, desc="Uploading to R2"):
        # Construct R2 key following archive structure
        r2_key = construct_r2_key(
            paper['box_number'],
            paper['folder_number'],
            paper['bundle_number'],
            paper['document_number']
        )

        if stream:
            # Streaming mode: download from CMU and upload directly to R2
            success, returned_key = stream_upload_to_r2(
                paper['box_number'],
                paper['folder_number'],
                paper['bundle_number'],
                paper['document_number'],
                s3_client=s3_client,
                dry_run=dry_run
            )
            if success:
                if not dry_run:
                    update_r2_key(paper['id'], returned_key)
                    # Also update OCR status to indicate PDF was processed
                    from db import update_ocr_status
                    update_ocr_status(paper['id'], 'r2_mirrored')
                uploaded += 1
            else:
                failed += 1

            # Rate limiting between uploads
            time.sleep(delay)
        else:
            # Local mode: upload from local file
            local_relative = paper['local_pdf_path']
            local_path = PDF_DIR / local_relative

            if not local_path.exists():
                if verbose:
                    print(f"\nSkipping {paper['id']}: local file not found at {local_path}")
                skipped += 1
                continue

            # Upload to R2
            if upload_pdf_to_r2(local_path, r2_key, s3_client=s3_client, dry_run=dry_run):
                if not dry_run:
                    update_r2_key(paper['id'], r2_key)
                uploaded += 1
            else:
                failed += 1

    print(f"\nUpload complete:")
    print(f"  Uploaded: {uploaded}")
    if not stream:
        print(f"  Skipped (file not found): {skipped}")
    print(f"  Failed: {failed}")

    if dry_run:
        print("\nThis was a dry run. No files were actually uploaded.")
        print("Run without --dry-run to perform actual uploads.")


def get_r2_url(r2_key: str) -> str:
    """Get the public URL for an R2 object.

    Uses R2_PUBLIC_URL if set, otherwise constructs the R2 dev URL.
    """
    if R2_PUBLIC_URL:
        # Custom domain (e.g., CDN)
        return f"{R2_PUBLIC_URL.rstrip('/')}/{r2_key}"
    else:
        # Standard R2 dev URL
        return f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com/{R2_BUCKET_NAME}/{r2_key}"


def get_r2_mirror_stats():
    """Display statistics about R2 mirroring."""
    stats = get_r2_stats()

    print(f"R2 Mirror Statistics:")
    print(f"  PDFs with local copy: {stats['total_with_local']}")
    print(f"  PDFs mirrored to R2: {stats['uploaded_to_r2']}")
    print(f"  Remaining to upload: {stats['remaining']}")

    if stats['total_with_local'] > 0:
        percent = (stats['uploaded_to_r2'] / stats['total_with_local']) * 100
        print(f"  Completion: {percent:.1f}%")


def verify_r2_upload(paper_id: int) -> bool:
    """Verify that a paper's PDF exists in R2.

    Returns True if the object exists in R2, False otherwise.
    """
    from db import get_paper_r2_key

    r2_key = get_paper_r2_key(paper_id)
    if not r2_key:
        return False

    try:
        s3_client = get_r2_client()
        s3_client.head_object(Bucket=R2_BUCKET_NAME, Key=r2_key)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Mirror PDFs to Cloudflare R2")
    parser.add_argument("--limit", type=int, help="Limit number of PDFs to upload")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without uploading")
    parser.add_argument("--stats", action="store_true", help="Show R2 mirror statistics")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument("--verify", type=int, metavar="PAPER_ID", help="Verify a specific paper's R2 upload")
    parser.add_argument("--stream", action="store_true", help="Stream PDFs directly from CMU (no local storage)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between uploads in seconds (streaming mode)")

    args = parser.parse_args()

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
