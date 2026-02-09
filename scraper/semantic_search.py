"""Advanced semantic search using DeepSeek Reasoner for intelligent paper retrieval."""

import os
import sys
import json
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / '.env')

from db import get_connection, init_db, search_papers

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

DEEPSEEK_REASONER_MODEL = "deepseek-reasoner"

# How much text content to include per paper (characters)
TEXT_CONTENT_LIMIT = 1500


def get_papers_with_text(limit: int = 200) -> list[dict]:
    """Get papers with text content for semantic search."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, summary, tags, series, item_type, date,
               box_number, folder_number, text_content
        FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
        ORDER BY date_sort DESC
        LIMIT ?
    """, (limit,))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def prefilter_by_keywords(keywords: str, limit: int = 100) -> list[dict]:
    """Use FTS5 to pre-filter papers by keywords, returning papers with full text."""
    # Use existing search_papers with fuzzy matching
    results, _ = search_papers(query=keywords, limit=limit, fuzzy=True)

    if not results:
        return []

    # Get full text content for matching papers
    paper_ids = [p['id'] for p in results]
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ','.join('?' * len(paper_ids))
    cursor.execute(f"""
        SELECT id, title, summary, tags, series, item_type, date,
               box_number, folder_number, text_content
        FROM papers
        WHERE id IN ({placeholders})
    """, paper_ids)
    papers = [dict(row) for row in cursor.fetchall()]
    conn.close()

    # Preserve original order from search results
    id_to_paper = {p['id']: p for p in papers}
    return [id_to_paper[pid] for pid in paper_ids if pid in id_to_paper]


def get_papers_by_ids(paper_ids: list[int]) -> list[dict]:
    """Get full paper details by IDs."""
    if not paper_ids:
        return []
    conn = get_connection()
    cursor = conn.cursor()
    placeholders = ','.join('?' * len(paper_ids))
    cursor.execute(f"""
        SELECT * FROM papers WHERE id IN ({placeholders})
    """, paper_ids)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    # Preserve order based on input IDs
    id_to_paper = {p['id']: p for p in results}
    return [id_to_paper[pid] for pid in paper_ids if pid in id_to_paper]


def build_search_prompt(query: str, papers: list[dict], include_text: bool = True) -> str:
    """Build the prompt for DeepSeek Reasoner with rich paper content."""
    papers_text = []

    for p in papers:
        # Parse tags
        tags = p.get('tags', '[]')
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except:
                tags = []
        tags_str = ', '.join(tags[:10]) if tags else 'none'

        # Build paper entry
        entry = [
            f"[ID:{p['id']}] {p['title']}",
            f"  Type: {p.get('item_type', 'N/A')} | Series: {p.get('series', 'N/A')} | Date: {p.get('date', 'N/A')}",
        ]

        if tags_str != 'none':
            entry.append(f"  Tags: {tags_str}")

        if p.get('summary'):
            entry.append(f"  Summary: {p['summary']}")

        # Include truncated text content for deeper analysis
        if include_text and p.get('text_content'):
            text = p['text_content'][:TEXT_CONTENT_LIMIT]
            if len(p['text_content']) > TEXT_CONTENT_LIMIT:
                text += "..."
            # Clean up the text a bit
            text = ' '.join(text.split())  # Normalize whitespace
            entry.append(f"  Content excerpt: {text}")

        papers_text.append('\n'.join(entry))

    papers_context = "\n\n".join(papers_text)

    return f"""You are a research assistant helping find relevant documents from Herbert Simon's papers archive. Herbert Simon was a Nobel laureate known for work in artificial intelligence, cognitive science, economics, organizational behavior, and decision-making theory.

USER QUERY: {query}

Below is a list of {len(papers)} papers from the archive. Each entry includes:
- ID (in brackets) and Title
- Type, Series, and Date
- Tags (topics, people, organizations mentioned)
- AI-generated summary (if available)
- Content excerpt from the document text (OCR'd, may have minor errors)

PAPERS:
{papers_context}

TASK: Carefully analyze each paper and identify those most relevant to the user's query. Consider:
1. Direct topic matches in content and summary
2. Related concepts, themes, and terminology
3. Mentioned people, organizations, or locations
4. Historical context and temporal relevance
5. Implicit connections (e.g., "decision making" relates to "bounded rationality")

Return a JSON object with:
- "relevant_ids": Array of paper IDs (integers) most relevant to the query, ordered by relevance (most relevant first). Include up to 25 papers if relevant, fewer if the query is specific.
- "reasoning": 2-3 sentences explaining why these papers were selected and how they relate to the query.

Return ONLY valid JSON, no other text. Example format:
{{"relevant_ids": [123, 456, 789], "reasoning": "These papers are relevant because..."}}"""


def semantic_search(
    query: str,
    max_candidates: int = 150,
    prefilter: Optional[str] = None,
    include_text: bool = True,
    verbose: bool = False
) -> tuple[list[dict], str]:
    """
    Perform semantic search using DeepSeek Reasoner.

    Args:
        query: Natural language search query
        max_candidates: Maximum number of papers to send to the model
        prefilter: Optional keywords to pre-filter candidates using FTS5 (reduces candidates)
        include_text: Include text content excerpts in the prompt (more context but more tokens)
        verbose: Print progress and reasoning

    Returns:
        Tuple of (list of relevant papers, reasoning explanation)
    """
    if not OPENAI_AVAILABLE:
        raise RuntimeError("openai library not available. Install with: pip install openai")

    deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
    if not deepseek_key:
        raise RuntimeError("DEEPSEEK_API_KEY not set in environment")

    init_db()

    # Get candidate papers
    if prefilter:
        if verbose:
            print(f"Pre-filtering with keywords: '{prefilter}'")
        papers = prefilter_by_keywords(prefilter, limit=max_candidates)
        if verbose:
            print(f"Found {len(papers)} papers matching pre-filter")
    else:
        if verbose:
            print(f"Fetching up to {max_candidates} papers for semantic analysis...")
        papers = get_papers_with_text(limit=max_candidates)

    if not papers:
        if verbose:
            print("No papers found. Make sure papers have been OCR'd.")
        return [], "No papers available for search."

    if verbose:
        print(f"Analyzing {len(papers)} papers with DeepSeek Reasoner...")
        if include_text:
            print(f"Including up to {TEXT_CONTENT_LIMIT} chars of text content per paper")

    # Build prompt and call API
    client = OpenAI(api_key=deepseek_key, base_url="https://api.deepseek.com")
    prompt = build_search_prompt(query, papers, include_text=include_text)

    if verbose:
        # Estimate token count (rough: ~4 chars per token)
        est_tokens = len(prompt) // 4
        print(f"Estimated prompt size: ~{est_tokens:,} tokens")

    try:
        response = client.chat.completions.create(
            model=DEEPSEEK_REASONER_MODEL,
            messages=[{"role": "user", "content": prompt}]
        )

        result_text = response.choices[0].message.content.strip()

        # Parse reasoning content if available (R1 model returns reasoning_content)
        reasoning_content = ""
        if hasattr(response.choices[0].message, 'reasoning_content') and response.choices[0].message.reasoning_content:
            reasoning_content = response.choices[0].message.reasoning_content
            if verbose:
                print(f"\n--- Model's Internal Reasoning ---")
                # Show truncated reasoning
                if len(reasoning_content) > 2000:
                    print(reasoning_content[:2000] + "\n[...truncated...]")
                else:
                    print(reasoning_content)
                print("--- End Reasoning ---\n")

        # Parse JSON response
        try:
            result = json.loads(result_text)
        except json.JSONDecodeError:
            # Try to extract JSON from response
            import re
            json_match = re.search(r'\{[\s\S]*\}', result_text)
            if json_match:
                result = json.loads(json_match.group())
            else:
                if verbose:
                    print(f"Failed to parse response: {result_text[:500]}")
                return [], "Failed to parse search results"

        relevant_ids = result.get('relevant_ids', [])
        reasoning = result.get('reasoning', 'No reasoning provided')

        if verbose:
            print(f"Found {len(relevant_ids)} relevant papers")

        # Get full paper details
        relevant_papers = get_papers_by_ids(relevant_ids)

        return relevant_papers, reasoning

    except Exception as e:
        error_msg = str(e)
        if verbose:
            print(f"API Error: {error_msg}")
        return [], f"Search error: {error_msg}"


def print_search_results(papers: list[dict], reasoning: str, query: str):
    """Pretty print search results."""
    print(f"\n{'=' * 70}")
    print(f"SEMANTIC SEARCH RESULTS")
    print(f"Query: {query}")
    print(f"{'=' * 70}")

    if reasoning:
        print(f"\n{reasoning}")

    if not papers:
        print("\nNo relevant papers found.")
        return

    print(f"\n{len(papers)} relevant papers:\n")

    for i, paper in enumerate(papers, 1):
        print(f"{i}. [{paper['id']}] {paper['title']}")

        meta_parts = []
        if paper.get('item_type'):
            meta_parts.append(paper['item_type'])
        if paper.get('date'):
            meta_parts.append(paper['date'])
        if paper.get('series'):
            meta_parts.append(paper['series'])
        if meta_parts:
            print(f"   {' | '.join(meta_parts)}")

        if paper.get('summary'):
            summary = paper['summary']
            if len(summary) > 200:
                summary = summary[:200] + "..."
            print(f"   {summary}")

        if paper.get('tags'):
            try:
                tags = json.loads(paper['tags']) if isinstance(paper['tags'], str) else paper['tags']
                if tags:
                    print(f"   Tags: {', '.join(tags[:6])}")
            except:
                pass
        print()


def interactive_search():
    """Run interactive semantic search session."""
    print("\n" + "=" * 70)
    print("HERBERT SIMON PAPERS - SEMANTIC SEARCH")
    print("Powered by DeepSeek Reasoner")
    print("=" * 70)
    print("\nEnter natural language queries to search the archive.")
    print("\nTips:")
    print("  - Use natural language: 'papers about decision making in organizations'")
    print("  - Ask for specific topics: 'correspondence with Allen Newell about GPS'")
    print("  - Prefix with 'filter:keyword' to pre-filter: 'filter:economics papers on rationality'")
    print("\nType 'quit' to exit.\n")

    while True:
        try:
            query = input("Search: ").strip()
            if not query:
                continue
            if query.lower() in ('quit', 'exit', 'q'):
                print("Goodbye!")
                break

            # Check for pre-filter prefix
            prefilter = None
            if query.lower().startswith('filter:'):
                parts = query[7:].split(' ', 1)
                if len(parts) == 2:
                    prefilter = parts[0]
                    query = parts[1]
                else:
                    print("Usage: filter:keyword your search query")
                    continue

            papers, reasoning = semantic_search(query, prefilter=prefilter, verbose=True)
            print_search_results(papers, reasoning, query)

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Semantic search using DeepSeek Reasoner")
    parser.add_argument("query", nargs="?", help="Search query (or omit for interactive mode)")
    parser.add_argument("--max-candidates", type=int, default=150,
                        help="Max papers to analyze (default: 150)")
    parser.add_argument("--prefilter", "-p", type=str,
                        help="Pre-filter candidates with keyword search first")
    parser.add_argument("--no-text", action="store_true",
                        help="Exclude text content (use only titles/summaries/tags)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show detailed progress and model reasoning")
    parser.add_argument("--json", action="store_true",
                        help="Output results as JSON")

    args = parser.parse_args()

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
            print(json.dumps(output, indent=2, default=str))
        else:
            print_search_results(papers, reasoning, args.query)
    else:
        interactive_search()
