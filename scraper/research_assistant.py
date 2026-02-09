"""Research assistant chatbot using Claude Sonnet to answer questions about Herbert Simon's papers."""

import os
import sys
import json
import re
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / '.env')

from db import get_connection, init_db, search_papers

try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

CLAUDE_MODEL = "claude-sonnet-4-20250514"

# How much text content to include per paper (reduced to fit more papers)
TEXT_CONTENT_LIMIT = 1200


def get_papers_for_context(
    query: str = None,
    year: str = None,
    limit: int = 50
) -> list[dict]:
    """Get papers relevant to a query or time period for context."""
    conn = get_connection()
    cursor = conn.cursor()

    # Build query based on parameters
    if year:
        # Get papers from a specific year
        cursor.execute("""
            SELECT id, title, summary, tags, series, item_type, date, date_sort,
                   box_number, folder_number, text_content
            FROM papers
            WHERE date_sort LIKE ? AND text_content IS NOT NULL AND text_content != ''
            ORDER BY date_sort
            LIMIT ?
        """, (f"{year}%", limit))
    elif query:
        # Use FTS to find relevant papers
        results, _ = search_papers(query=query, limit=limit, fuzzy=True)
        if results:
            paper_ids = [p['id'] for p in results]
            placeholders = ','.join('?' * len(paper_ids))
            cursor.execute(f"""
                SELECT id, title, summary, tags, series, item_type, date, date_sort,
                       box_number, folder_number, text_content
                FROM papers
                WHERE id IN ({placeholders})
            """, paper_ids)
        else:
            return []
    else:
        # Get recent papers with content
        cursor.execute("""
            SELECT id, title, summary, tags, series, item_type, date, date_sort,
                   box_number, folder_number, text_content
            FROM papers
            WHERE text_content IS NOT NULL AND text_content != ''
            ORDER BY date_sort DESC
            LIMIT ?
        """, (limit,))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_all_analyzed_papers(limit: int = 300) -> list[dict]:
    """Get papers with summaries for broad questions."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, summary, tags, series, item_type, date, date_sort,
               box_number, folder_number, text_content
        FROM papers
        WHERE summary IS NOT NULL AND summary != ''
        ORDER BY date_sort DESC
        LIMIT ?
    """, (limit,))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_all_summaries() -> list[dict]:
    """Get all papers with summaries (lightweight - no text_content)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, summary, tags, series, item_type, date, date_sort
        FROM papers
        WHERE summary IS NOT NULL AND summary != ''
        ORDER BY date_sort DESC
    """)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_papers_full_text(paper_ids: list[int]) -> list[dict]:
    """Get full paper details including text_content for specific IDs."""
    if not paper_ids:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ','.join('?' * len(paper_ids))
    cursor.execute(f"""
        SELECT id, title, summary, tags, series, item_type, date, date_sort,
               box_number, folder_number, text_content
        FROM papers
        WHERE id IN ({placeholders})
    """, paper_ids)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    # Preserve order
    id_to_paper = {p['id']: p for p in results}
    return [id_to_paper[pid] for pid in paper_ids if pid in id_to_paper]


def extract_keywords_and_year(question: str) -> tuple[list[str], Optional[str]]:
    """Extract potential search keywords and year from a question."""
    # Extract year if mentioned
    year_match = re.search(r'\b(19[0-9]{2}|20[0-2][0-9])\b', question)
    year = year_match.group(1) if year_match else None

    # Extract potential keywords (simplified)
    # Remove common question words
    stop_words = {'what', 'when', 'where', 'who', 'why', 'how', 'did', 'was', 'were', 'is',
                  'are', 'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or',
                  'his', 'her', 'their', 'about', 'simon', 'herbert', "simon's", 'tell', 'me',
                  'can', 'you', 'please', 'i', 'want', 'know', 'would', 'like', 'could'}

    words = re.findall(r'\b[a-zA-Z]+\b', question.lower())
    keywords = [w for w in words if w not in stop_words and len(w) > 2]

    return keywords, year


def build_discovery_prompt(question: str, papers: list[dict]) -> str:
    """Build prompt for Stage 1: discovering relevant papers from summaries."""
    papers_text = []

    for p in papers:
        tags = p.get('tags', '[]')
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except:
                tags = []
        tags_str = ', '.join(tags[:5]) if tags else ''

        entry = f"[{p['id']}] {p['title']} ({p.get('date', 'Unknown')}) - {p.get('summary', 'No summary')}"
        if tags_str:
            entry += f" [Tags: {tags_str}]"
        papers_text.append(entry)

    context = "\n".join(papers_text)

    return f"""You are analyzing Herbert Simon's papers archive to find documents relevant to a research question.

QUESTION: {question}

Below are {len(papers)} papers with their summaries. Identify ALL papers that might be relevant to answering this question. Be inclusive - if a paper might contain useful information, include it.

PAPERS:
{context}

Return a JSON object with:
- "relevant_ids": Array of paper IDs (integers) that are relevant to the question. Include all potentially useful papers (up to 50).
- "search_strategy": Brief note on what types of papers you looked for.

Return ONLY valid JSON."""


def build_research_prompt(question: str, papers: list[dict]) -> str:
    """Build the prompt for answering research questions."""
    papers_context = []

    for p in papers:
        # Parse tags
        tags = p.get('tags', '[]')
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except:
                tags = []
        tags_str = ', '.join(tags[:8]) if tags else ''

        # Build paper entry with content
        entry_parts = [
            f"[Paper ID:{p['id']}] \"{p['title']}\"",
            f"Date: {p.get('date', 'Unknown')} | Type: {p.get('item_type', 'Unknown')} | Series: {p.get('series', 'Unknown')}"
        ]

        if tags_str:
            entry_parts.append(f"Topics/People: {tags_str}")

        if p.get('summary'):
            entry_parts.append(f"Summary: {p['summary']}")

        if p.get('text_content'):
            text = p['text_content'][:TEXT_CONTENT_LIMIT]
            text = ' '.join(text.split())  # Normalize whitespace
            if len(p['text_content']) > TEXT_CONTENT_LIMIT:
                text += "..."
            entry_parts.append(f"Content excerpt:\n{text}")

        papers_context.append('\n'.join(entry_parts))

    context = "\n\n---\n\n".join(papers_context)

    return f"""You are a research assistant with deep knowledge of Herbert A. Simon's life and work. Herbert Simon (1916-2001) was a polymath who won the Nobel Prize in Economics (1978) and the Turing Award (1975). He made foundational contributions to artificial intelligence, cognitive psychology, organizational behavior, economics, and computer science.

You have access to {len(papers)} documents from his personal papers archive at Carnegie Mellon University. Use these documents to answer the user's question. Your answer should be:

1. **Comprehensive**: Draw on multiple sources when relevant
2. **Well-cited**: Reference specific papers by their ID when making claims (e.g., "In a 1965 letter [Paper ID:123], Simon wrote...")
3. **Accurate**: Only state what is supported by the documents. If the documents don't contain enough information, say so.
4. **Contextual**: Provide historical context when helpful

USER'S QUESTION: {question}

DOCUMENTS FROM THE ARCHIVE:

{context}

---

Please provide a thorough answer to the question. Structure your response clearly, cite specific documents when relevant, and acknowledge any limitations in the available evidence. If appropriate, suggest which documents the user might want to explore further.

At the end of your response, list the most relevant papers under "Key Sources:" with their IDs and titles."""


def ask_research_question(
    question: str,
    max_detailed_papers: int = 50,
    verbose: bool = False
) -> dict:
    """
    Answer a research question about Herbert Simon using a two-stage approach:
    1. Scan ALL summaries to find relevant papers
    2. Load full text of relevant papers for detailed analysis

    Args:
        question: The user's question
        max_detailed_papers: Maximum papers to analyze in detail (with full text)
        verbose: Print progress

    Returns:
        Dict with 'answer', 'sources', and 'reasoning' (if available)
    """
    if not ANTHROPIC_AVAILABLE:
        raise RuntimeError("anthropic library not available. Install with: pip install anthropic")

    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    if not anthropic_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment")

    init_db()
    client = anthropic.Anthropic(api_key=anthropic_key)

    # Extract keywords and year from question
    keywords, year = extract_keywords_and_year(question)

    if verbose:
        print(f"Extracted keywords: {keywords}")
        if year:
            print(f"Detected year: {year}")

    # ============ STAGE 1: Discovery - scan all summaries ============
    if verbose:
        print("\n[Stage 1] Loading all paper summaries...")

    all_summaries = get_all_summaries()

    if verbose:
        print(f"Loaded {len(all_summaries)} papers with summaries")

    # If year specified, filter to that year first
    if year:
        year_filtered = [p for p in all_summaries if p.get('date_sort', '').startswith(year)]
        if verbose:
            print(f"Filtered to {len(year_filtered)} papers from {year}")
        # Combine year papers with others, year papers first
        other_papers = [p for p in all_summaries if not p.get('date_sort', '').startswith(year)]
        all_summaries = year_filtered + other_papers

    if not all_summaries:
        return {
            'answer': "No papers with summaries found. Please run analysis first.",
            'sources': [],
            'reasoning': None
        }

    if verbose:
        print(f"[Stage 1] Asking Claude to identify relevant papers from {len(all_summaries)} summaries...")

    # Build discovery prompt and find relevant papers
    discovery_prompt = build_discovery_prompt(question, all_summaries)

    if verbose:
        est_tokens = len(discovery_prompt) // 4
        print(f"Discovery prompt size: ~{est_tokens:,} tokens")

    try:
        discovery_response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2048,
            messages=[{"role": "user", "content": discovery_prompt}]
        )

        discovery_text = discovery_response.content[0].text.strip()

        # Parse the response
        try:
            discovery_result = json.loads(discovery_text)
        except json.JSONDecodeError:
            json_match = re.search(r'\{[\s\S]*\}', discovery_text)
            if json_match:
                discovery_result = json.loads(json_match.group())
            else:
                # Fallback: extract IDs directly
                found_ids = list(map(int, re.findall(r'\b(\d+)\b', discovery_text)))[:max_detailed_papers]
                discovery_result = {'relevant_ids': found_ids, 'search_strategy': 'Fallback extraction'}

        relevant_ids = discovery_result.get('relevant_ids', [])[:max_detailed_papers]

        if verbose:
            strategy = discovery_result.get('search_strategy', 'N/A')
            print(f"[Stage 1] Found {len(relevant_ids)} relevant papers")
            print(f"Search strategy: {strategy}")

    except Exception as e:
        if verbose:
            print(f"[Stage 1] Error: {e}")
        # Fallback: use keyword search
        relevant_ids = []
        if keywords:
            keyword_results = get_papers_for_context(query=' '.join(keywords[:5]), limit=max_detailed_papers)
            relevant_ids = [p['id'] for p in keyword_results]

    if not relevant_ids:
        return {
            'answer': "I couldn't identify any relevant papers for your question. Try rephrasing or asking about a different topic.",
            'sources': [],
            'reasoning': None
        }

    # ============ STAGE 2: Deep analysis with full text ============
    if verbose:
        print(f"\n[Stage 2] Loading full text for {len(relevant_ids)} papers...")

    papers = get_papers_full_text(relevant_ids)

    if verbose:
        print(f"[Stage 2] Analyzing {len(papers)} papers in detail...")

    # Build research prompt with full text
    research_prompt = build_research_prompt(question, papers)

    if verbose:
        est_tokens = len(research_prompt) // 4
        print(f"Research prompt size: ~{est_tokens:,} tokens")

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": research_prompt}]
        )

        answer = response.content[0].text.strip()

        # Extract cited paper IDs from the answer
        cited_ids = set(map(int, re.findall(r'\[Paper ID:(\d+)\]', answer)))

        # Get source papers that were cited
        sources = [
            {'id': p['id'], 'title': p['title'], 'date': p.get('date'), 'type': p.get('item_type')}
            for p in papers if p['id'] in cited_ids
        ]

        return {
            'answer': answer,
            'sources': sources,
            'reasoning': None
        }

    except Exception as e:
        return {
            'answer': f"I encountered an error while processing your question: {str(e)}",
            'sources': [],
            'reasoning': None
        }


def chat_session():
    """Run an interactive chat session."""
    print("\n" + "=" * 70)
    print("HERBERT SIMON RESEARCH ASSISTANT")
    print(f"Powered by Claude Sonnet ({CLAUDE_MODEL})")
    print("=" * 70)
    print("\nExamples:")
    print("  - What were Simon's views on artificial intelligence?")
    print("  - Summarize Simon's activities in 1975")
    print("  - What was Simon's relationship with Allen Newell?")
    print("  - How did Simon contribute to cognitive science?")
    print("\nType 'quit' to exit.\n")

    while True:
        try:
            question = input("You: ").strip()
            if not question:
                continue
            if question.lower() in ('quit', 'exit', 'q'):
                print("Goodbye!")
                break

            print("\nThinking...\n")
            result = ask_research_question(question, verbose=True)

            print("\n" + "-" * 70)
            print("Assistant:\n")
            print(result['answer'])

            if result['sources']:
                print("\n" + "-" * 40)
                print(f"Sources cited: {len(result['sources'])} papers")

            print("\n")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Research assistant for Herbert Simon papers")
    parser.add_argument("question", nargs="?", help="Question to ask (omit for interactive mode)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed progress")
    parser.add_argument("--max-detailed", type=int, default=50, help="Max papers to analyze in detail")

    args = parser.parse_args()

    if args.question:
        result = ask_research_question(args.question, max_detailed_papers=args.max_detailed, verbose=args.verbose)
        print(result['answer'])
    else:
        chat_session()
