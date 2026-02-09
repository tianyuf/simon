"""Analyze OCR'd papers using DeepSeek API (with Anthropic fallback) to extract summaries, tags, and language."""

import os
import sys
import json
import time
from pathlib import Path
from tqdm import tqdm

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / '.env')

from db import init_db, get_papers_for_analysis, update_paper_analysis, update_analysis_status

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

DEEPSEEK_MODEL = "deepseek-chat"
ANTHROPIC_MODEL = "claude-3-haiku-20240307"


ANALYSIS_PROMPT = """Analyze this document from Herbert Simon's papers archive and provide:

1. **Summary**: A concise 1-2 sentence summary. Be direct and go straight to the point. Do NOT start with phrases like "This document...", "This is a...", "This letter...", etc. Instead, start with the actual content (e.g., "Discusses the role of..." or "Requests funding for..." or "Thank you note for...").

2. **Tags**: A JSON array of relevant tags including:
   - Topic tags (e.g., "artificial intelligence", "decision making", "economics", "cognitive science")
   - People mentioned (e.g., "Allen Newell", "John McCarthy")
   - Organizations mentioned (e.g., "Carnegie Mellon University", "RAND Corporation", "NSF")
   - Locations mentioned (e.g., "Pittsburgh", "Washington D.C.")

3. **Language**: The primary language of the document (e.g., "English", "Chinese", "German", "French")

Document metadata:
- Title: {title}
- Series: {series}
- Type: {item_type}
- Date: {date}

Document text (may be OCR'd, so some errors possible):
---
{text}
---

Respond in this exact JSON format:
{{
    "summary": "Your 1-2 sentence summary here",
    "tags": ["tag1", "tag2", "tag3", ...],
    "language": "English"
}}

Only respond with valid JSON, no other text."""


ANTHROPIC_ANALYSIS_PROMPT = """Analyze this document from Herbert Simon's papers archive.

CRITICAL INSTRUCTION FOR SUMMARY: Your summary MUST be direct. NEVER start with:
- "This document..."
- "This is a..."
- "This letter..."
- "This paper..."
- "The document..."

Instead, start with action verbs or the actual subject matter:
- "Discusses the role of..."
- "Proposes a new method for..."
- "Requests funding for..."
- "Examines the relationship between..."
- "Thank you note for..."

Provide:
1. **Summary**: 1-2 sentences, direct style as described above.
2. **Tags**: JSON array of topics, people, organizations, and locations mentioned.
3. **Language**: Primary language of the document.

Document metadata:
- Title: {title}
- Series: {series}
- Type: {item_type}
- Date: {date}

Document text (may be OCR'd):
---
{text}
---

Respond ONLY with valid JSON in this exact format:
{{
    "summary": "Direct summary without 'This document...'",
    "tags": ["tag1", "tag2", "tag3"],
    "language": "English"
}}"""


def parse_json_response(result_text: str) -> dict | None:
    """Parse JSON from API response, handling markdown code blocks."""
    import re
    try:
        return json.loads(result_text)
    except json.JSONDecodeError:
        json_match = re.search(r'\{[\s\S]*\}', result_text)
        if json_match:
            return json.loads(json_match.group())
        return None


def analyze_with_deepseek(client: OpenAI, prompt: str) -> tuple[dict | None, bool]:
    """
    Analyze using DeepSeek API.
    Returns (result, content_filtered) - content_filtered is True if content filter triggered.
    """
    try:
        response = client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        result_text = response.choices[0].message.content.strip()
        return parse_json_response(result_text), False
    except Exception as e:
        error_str = str(e)
        if "Content Exists Risk" in error_str or "content" in error_str.lower() and "risk" in error_str.lower():
            return None, True  # Content filter triggered
        print(f"\nDeepSeek API error: {e}")
        return None, False


def analyze_with_anthropic(client: anthropic.Anthropic, prompt: str) -> dict | None:
    """Analyze using Anthropic API as fallback."""
    try:
        response = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        result_text = response.content[0].text.strip()
        return parse_json_response(result_text)
    except Exception as e:
        print(f"\nAnthropic API error: {e}")
        return None


def analyze_paper(deepseek_client: OpenAI, anthropic_client: anthropic.Anthropic | None, paper: dict) -> tuple[dict | None, str | None]:
    """
    Analyze a single paper using DeepSeek API, falling back to Anthropic if content filtered.
    Returns (result, model_used) where model_used is 'deepseek' or 'anthropic'.
    """
    text = paper.get('text_content', '')[:8000]

    if len(text.strip()) < 20:
        return None, None

    prompt_kwargs = {
        'title': paper.get('title', 'Unknown'),
        'series': paper.get('series', 'Unknown'),
        'item_type': paper.get('item_type', 'Unknown'),
        'date': paper.get('date', 'Unknown'),
        'text': text
    }

    # Try DeepSeek first
    deepseek_prompt = ANALYSIS_PROMPT.format(**prompt_kwargs)
    result, content_filtered = analyze_with_deepseek(deepseek_client, deepseek_prompt)
    if result:
        return result, 'deepseek'

    # If content filtered and Anthropic available, try fallback with Anthropic-specific prompt
    if content_filtered and anthropic_client:
        print(f"\n  [Content filter triggered, falling back to Anthropic...]")
        anthropic_prompt = ANTHROPIC_ANALYSIS_PROMPT.format(**prompt_kwargs)
        result = analyze_with_anthropic(anthropic_client, anthropic_prompt)
        if result:
            return result, 'anthropic'

    return None, None


def analyze_all_papers(
    limit: int = None,
    delay: float = 0.5,
    verbose: bool = False
):
    """Analyze all papers that have OCR text but haven't been analyzed."""
    if not OPENAI_AVAILABLE:
        print("Error: openai library not available. Install with: pip install openai")
        return

    deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
    if not deepseek_key:
        print("Error: DEEPSEEK_API_KEY not set")
        return

    # Initialize database
    init_db()

    # Initialize DeepSeek client
    deepseek_client = OpenAI(api_key=deepseek_key, base_url="https://api.deepseek.com")

    # Initialize Anthropic client if available (for fallback)
    anthropic_client = None
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if ANTHROPIC_AVAILABLE and anthropic_key:
        anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
        print(f"Using DeepSeek ({DEEPSEEK_MODEL}) with Anthropic fallback ({ANTHROPIC_MODEL})")
    else:
        print(f"Using DeepSeek ({DEEPSEEK_MODEL}) only (no Anthropic fallback)")

    # Get papers to analyze
    papers = get_papers_for_analysis(limit=limit)

    if not papers:
        print("No papers to analyze (all already analyzed or no OCR text)")
        return

    print(f"Found {len(papers)} papers to analyze")

    analyzed = 0
    failed = 0
    deepseek_count = 0
    anthropic_count = 0

    for paper in tqdm(papers, desc="Analyzing"):
        result, model_used = analyze_paper(deepseek_client, anthropic_client, paper)

        if result and model_used:
            summary = result.get('summary', '')
            tags = json.dumps(result.get('tags', []))
            language = result.get('language', 'Unknown')

            update_paper_analysis(paper['id'], summary, tags, language, model=model_used)
            analyzed += 1

            if model_used == 'deepseek':
                deepseek_count += 1
            else:
                anthropic_count += 1

            if verbose:
                print(f"\n  [{model_used}] {paper['title'][:50]}...")
                print(f"    Summary: {summary[:80]}...")
                print(f"    Tags: {tags[:60]}...")
                print(f"    Language: {language}")
        else:
            update_analysis_status(paper['id'], 'failed')
            failed += 1
            if verbose:
                print(f"\n  Failed: {paper['title'][:50]}...")

        # Rate limiting
        time.sleep(delay)

    print(f"\nAnalysis complete:")
    print(f"  Analyzed: {analyzed} (DeepSeek: {deepseek_count}, Anthropic: {anthropic_count})")
    print(f"  Failed: {failed}")


def get_analysis_stats():
    """Get statistics about paper analysis."""
    from db import get_connection

    conn = get_connection()
    cursor = conn.cursor()

    # Total papers with OCR text
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
    """)
    total_with_text = cursor.fetchone()[0]

    # Analysis status breakdown
    cursor.execute("""
        SELECT analysis_status, COUNT(*) as count
        FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
        GROUP BY analysis_status
    """)
    status_counts = {row['analysis_status'] or 'pending': row['count'] for row in cursor.fetchall()}

    # Language breakdown
    cursor.execute("""
        SELECT language, COUNT(*) as count
        FROM papers
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY count DESC
        LIMIT 10
    """)
    languages = [(row['language'], row['count']) for row in cursor.fetchall()]

    # Most common tags
    cursor.execute("""
        SELECT tags FROM papers
        WHERE tags IS NOT NULL AND tags != '[]'
    """)
    tag_counts = {}
    for row in cursor.fetchall():
        try:
            tags = json.loads(row['tags'])
            for tag in tags:
                tag_lower = tag.lower()
                tag_counts[tag_lower] = tag_counts.get(tag_lower, 0) + 1
        except:
            pass

    top_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    conn.close()

    print(f"Analysis Statistics:")
    print(f"  Papers with OCR text: {total_with_text}")
    print(f"  Status breakdown:")
    for status, count in sorted(status_counts.items()):
        print(f"    {status}: {count}")
    print(f"  Remaining to analyze: {status_counts.get('pending', 0)}")

    if languages:
        print(f"\n  Languages detected:")
        for lang, count in languages:
            print(f"    {lang}: {count}")

    if top_tags:
        print(f"\n  Top tags:")
        for tag, count in top_tags[:15]:
            print(f"    {tag}: {count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyze OCR'd papers using DeepSeek API")
    parser.add_argument("--limit", type=int, help="Limit number of papers to analyze")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between API calls (seconds)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument("--stats", action="store_true", help="Show analysis statistics")

    args = parser.parse_args()

    if args.stats:
        get_analysis_stats()
    else:
        analyze_all_papers(
            limit=args.limit,
            delay=args.delay,
            verbose=args.verbose
        )
