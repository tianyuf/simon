"""Generate summaries for archive boxes and folders using DeepSeek API."""

import os
import sys
import time
from pathlib import Path
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / '.env')

from db import (
    init_db, get_folders_for_summarization, get_boxes_for_summarization,
    get_folder_documents, get_box_documents, save_archive_summary,
    get_archive_summaries
)

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

DEEPSEEK_MODEL = "deepseek-chat"

FOLDER_SUMMARY_PROMPT = """Create a very short topic label for this folder from Herbert Simon's papers archive.

Folder: Box {box_number}, Folder {folder_number}
Number of documents: {doc_count}

Documents in this folder:
{documents}

Create a brief topic label (5-15 words max) that captures what this folder is about. Format examples:
- "1980 China trip correspondence"
- "NSF grant proposals, cognitive science"
- "Allen Newell collaboration, 1975-1982"
- "Carnegie Mellon administrative files"
- "Artificial intelligence lectures and notes"

Focus on: topic, key people/institutions, and dates if apparent.

Respond with ONLY the short topic label, nothing else."""

BOX_SUMMARY_PROMPT = """Create a very short topic label for this box from Herbert Simon's papers archive.

Box {box_number}
Number of folders: {folder_count}
Number of documents: {doc_count}

Folder contents:
{folders}

Create a brief topic label (5-15 words max) that captures the overall theme of this box. Format examples:
- "Professional correspondence, 1970s"
- "Cognitive science research materials"
- "Carnegie Mellon administration, 1965-1975"
- "Publications and manuscripts"
- "Conference papers and lectures"

Focus on: main themes, key institutions/people, and time periods if apparent.

Respond with ONLY the short topic label, nothing else."""


def summarize_with_deepseek(client: OpenAI, prompt: str) -> str | None:
    """Generate summary using DeepSeek API."""
    try:
        response = client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"\nDeepSeek API error: {e}")
        return None


def summarize_folder(client: OpenAI, box_number: int, folder_number: int) -> str | None:
    """Generate summary for a single folder."""
    documents = get_folder_documents(box_number, folder_number, limit=50)

    if not documents:
        return None

    # Format documents for the prompt
    doc_lines = []
    for doc in documents:
        line = f"- {doc['title']}"
        if doc.get('date'):
            line += f" ({doc['date']})"
        if doc.get('summary'):
            line += f": {doc['summary'][:100]}..."
        doc_lines.append(line)

    prompt = FOLDER_SUMMARY_PROMPT.format(
        box_number=box_number,
        folder_number=folder_number,
        doc_count=len(documents),
        documents="\n".join(doc_lines[:30])  # Limit to avoid token limits
    )

    return summarize_with_deepseek(client, prompt)


def summarize_box(client: OpenAI, box_number: int) -> str | None:
    """Generate summary for a single box."""
    documents = get_box_documents(box_number, limit=150)

    if not documents:
        return None

    # Group by folder
    folders = {}
    for doc in documents:
        fn = doc.get('folder_number')
        if fn not in folders:
            folders[fn] = []
        folders[fn].append(doc)

    # Format folders for the prompt
    folder_lines = []
    for fn in sorted(folders.keys()):
        docs = folders[fn]
        titles = [d['title'] for d in docs[:5]]  # First 5 titles per folder
        folder_lines.append(f"Folder {fn} ({len(docs)} docs): {', '.join(titles)}")

    prompt = BOX_SUMMARY_PROMPT.format(
        box_number=box_number,
        folder_count=len(folders),
        doc_count=len(documents),
        folders="\n".join(folder_lines[:20])  # Limit folders shown
    )

    return summarize_with_deepseek(client, prompt)


def summarize_folders(client: OpenAI, limit: int = None, delay: float = 0.5):
    """Summarize all folders that need summarization."""
    folders = get_folders_for_summarization()

    if limit:
        folders = folders[:limit]

    if not folders:
        print("No folders need summarization")
        return

    print(f"Found {len(folders)} folders to summarize")

    success = 0
    failed = 0

    for folder in tqdm(folders, desc="Summarizing folders"):
        box = folder['box_number']
        fld = folder['folder_number']

        summary = summarize_folder(client, box, fld)

        if summary:
            save_archive_summary('folder', box, fld, summary, model='deepseek')
            success += 1
        else:
            failed += 1

        time.sleep(delay)

    print(f"\nFolder summarization complete:")
    print(f"  Success: {success}")
    print(f"  Failed: {failed}")


def summarize_boxes(client: OpenAI, limit: int = None, delay: float = 0.5):
    """Summarize all boxes that need summarization."""
    boxes = get_boxes_for_summarization()

    if limit:
        boxes = boxes[:limit]

    if not boxes:
        print("No boxes need summarization")
        return

    print(f"Found {len(boxes)} boxes to summarize")

    success = 0
    failed = 0

    for box in tqdm(boxes, desc="Summarizing boxes"):
        box_number = box['box_number']

        summary = summarize_box(client, box_number)

        if summary:
            save_archive_summary('box', box_number, None, summary, model='deepseek')
            success += 1
        else:
            failed += 1

        time.sleep(delay)

    print(f"\nBox summarization complete:")
    print(f"  Success: {success}")
    print(f"  Failed: {failed}")


def show_stats():
    """Show summarization statistics."""
    summaries = get_archive_summaries()

    print("Archive Summarization Statistics:")
    print(f"  Box summaries: {len(summaries['boxes'])}")
    print(f"  Folder summaries: {len(summaries['folders'])}")

    # Show sample summaries
    if summaries['boxes']:
        print("\nSample box summaries:")
        for box_num, data in list(summaries['boxes'].items())[:3]:
            print(f"  Box {box_num}: {data['summary'][:100]}...")

    if summaries['folders']:
        print("\nSample folder summaries:")
        for (box, folder), data in list(summaries['folders'].items())[:3]:
            print(f"  Box {box}, Folder {folder}: {data['summary'][:100]}...")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate archive summaries using DeepSeek")
    parser.add_argument("--folders", action="store_true", help="Summarize folders")
    parser.add_argument("--boxes", action="store_true", help="Summarize boxes")
    parser.add_argument("--all", action="store_true", help="Summarize both folders and boxes")
    parser.add_argument("--limit", type=int, help="Limit number to summarize")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between API calls")
    parser.add_argument("--stats", action="store_true", help="Show summarization statistics")

    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if not any([args.folders, args.boxes, args.all]):
        parser.print_help()
        print("\nSpecify --folders, --boxes, or --all to generate summaries")
        return

    if not OPENAI_AVAILABLE:
        print("Error: openai library not available. Install with: pip install openai")
        return

    deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
    if not deepseek_key:
        print("Error: DEEPSEEK_API_KEY not set in environment")
        return

    # Initialize
    init_db()
    client = OpenAI(api_key=deepseek_key, base_url="https://api.deepseek.com")
    print(f"Using DeepSeek ({DEEPSEEK_MODEL})")

    if args.folders or args.all:
        summarize_folders(client, limit=args.limit, delay=args.delay)

    if args.boxes or args.all:
        summarize_boxes(client, limit=args.limit, delay=args.delay)


if __name__ == "__main__":
    main()
