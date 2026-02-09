"""Database module for Herbert Simon papers catalog."""

import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "simon_papers.db"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database schema."""
    conn = get_connection()
    cursor = conn.cursor()

    # Main papers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS papers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id INTEGER UNIQUE NOT NULL,
            title TEXT NOT NULL,
            date TEXT,
            date_sort TEXT,
            series TEXT,
            item_type TEXT,
            url TEXT,
            thumbnail_url TEXT,
            box_number INTEGER,
            folder_number INTEGER,
            bundle_number INTEGER,
            document_number INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Add columns if they don't exist (for migration)
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN box_number INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN folder_number INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN bundle_number INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN document_number INTEGER")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN local_pdf_path TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN text_content TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN ocr_status TEXT")  # 'pending', 'completed', 'failed', 'no_pdf'
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN starred INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN starred_at TIMESTAMP")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN summary TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN tags TEXT")  # JSON array of tags
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN language TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN analysis_status TEXT")  # 'pending', 'completed', 'failed'
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE papers ADD COLUMN analysis_model TEXT")  # 'deepseek' or 'anthropic'
    except sqlite3.OperationalError:
        pass

    # Full-text search virtual table (includes text_content for OCR search)
    cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS papers_fts USING fts5(
            title,
            series,
            item_type,
            text_content,
            content='papers',
            content_rowid='id'
        )
    """)

    # Triggers to keep FTS in sync
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS papers_ai AFTER INSERT ON papers BEGIN
            INSERT INTO papers_fts(rowid, title, series, item_type, text_content)
            VALUES (new.id, new.title, new.series, new.item_type, new.text_content);
        END
    """)

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS papers_ad AFTER DELETE ON papers BEGIN
            INSERT INTO papers_fts(papers_fts, rowid, title, series, item_type, text_content)
            VALUES('delete', old.id, old.title, old.series, old.item_type, old.text_content);
        END
    """)

    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS papers_au AFTER UPDATE ON papers BEGIN
            INSERT INTO papers_fts(papers_fts, rowid, title, series, item_type, text_content)
            VALUES('delete', old.id, old.title, old.series, old.item_type, old.text_content);
            INSERT INTO papers_fts(rowid, title, series, item_type, text_content)
            VALUES (new.id, new.title, new.series, new.item_type, new.text_content);
        END
    """)

    # Indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_date_sort ON papers(date_sort)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_series ON papers(series)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_item_type ON papers(item_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_box ON papers(box_number)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_folder ON papers(folder_number)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_box_folder ON papers(box_number, folder_number)")

    # Archive summaries table (for box and folder summaries)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS archive_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            summary_type TEXT NOT NULL,  -- 'box' or 'folder'
            box_number INTEGER NOT NULL,
            folder_number INTEGER,  -- NULL for box summaries
            summary TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model TEXT,  -- which model generated the summary
            UNIQUE(summary_type, box_number, folder_number)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_archive_summaries_box ON archive_summaries(box_number)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_archive_summaries_type ON archive_summaries(summary_type)")

    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")


def insert_paper(paper: dict) -> bool:
    """Insert a paper record, returns True if inserted, False if already exists."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO papers (node_id, title, date, date_sort, series, item_type, url, thumbnail_url,
                               box_number, folder_number, bundle_number, document_number)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            paper['node_id'],
            paper['title'],
            paper.get('date'),
            paper.get('date_sort'),
            paper.get('series'),
            paper.get('item_type'),
            paper.get('url'),
            paper.get('thumbnail_url'),
            paper.get('box_number'),
            paper.get('folder_number'),
            paper.get('bundle_number'),
            paper.get('document_number')
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def insert_papers_batch(papers: list) -> int:
    """Insert multiple papers in a batch. Returns count of newly inserted."""
    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0

    for paper in papers:
        try:
            cursor.execute("""
                INSERT INTO papers (node_id, title, date, date_sort, series, item_type, url, thumbnail_url,
                                   box_number, folder_number, bundle_number, document_number)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                paper['node_id'],
                paper['title'],
                paper.get('date'),
                paper.get('date_sort'),
                paper.get('series'),
                paper.get('item_type'),
                paper.get('url'),
                paper.get('thumbnail_url'),
                paper.get('box_number'),
                paper.get('folder_number'),
                paper.get('bundle_number'),
                paper.get('document_number')
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return inserted


import re as regex_module


def _build_fts_query(query: str) -> str:
    """
    Build an FTS5 query string supporting boolean operators.

    Supports:
    - AND: both terms must match (default between words)
    - OR: either term matches
    - NOT: exclude term
    - "quoted phrases": exact phrase match
    - Parentheses for grouping: (term1 OR term2) AND term3

    Examples:
    - 'simon carnegie' -> '"simon" AND "carnegie"'
    - 'simon OR newell' -> '"simon" OR "newell"'
    - 'simon NOT chess' -> '"simon" NOT "chess"'
    - '"bounded rationality"' -> '"bounded rationality"'
    - '(simon OR newell) AND AI' -> '("simon" OR "newell") AND "AI"'
    """
    if not query or not query.strip():
        return ''

    query = query.strip()

    # Tokenize: extract quoted phrases, operators, parentheses, and words
    tokens = []
    i = 0
    while i < len(query):
        # Skip whitespace
        if query[i].isspace():
            i += 1
            continue

        # Quoted phrase
        if query[i] == '"':
            end = query.find('"', i + 1)
            if end == -1:
                end = len(query)
            phrase = query[i+1:end].strip()
            if phrase:
                tokens.append(('PHRASE', phrase))
            i = end + 1
            continue

        # Parentheses
        if query[i] == '(':
            tokens.append(('LPAREN', '('))
            i += 1
            continue
        if query[i] == ')':
            tokens.append(('RPAREN', ')'))
            i += 1
            continue

        # Word or operator
        j = i
        while j < len(query) and not query[j].isspace() and query[j] not in '"()':
            j += 1
        word = query[i:j]
        i = j

        # Check if it's an operator
        word_upper = word.upper()
        if word_upper == 'AND':
            tokens.append(('AND', 'AND'))
        elif word_upper == 'OR':
            tokens.append(('OR', 'OR'))
        elif word_upper == 'NOT':
            tokens.append(('NOT', 'NOT'))
        elif word:
            # Clean the word - remove special chars that could break FTS5
            clean = ''.join(c for c in word if c.isalnum() or c in '-_')
            if clean:
                tokens.append(('WORD', clean))

    if not tokens:
        return ''

    # Build FTS5 query from tokens
    # Insert implicit AND between adjacent terms (WORD/PHRASE) without operators
    result_tokens = []
    prev_type = None

    for token_type, token_value in tokens:
        # Insert implicit AND between terms
        if token_type in ('WORD', 'PHRASE', 'LPAREN'):
            if prev_type in ('WORD', 'PHRASE', 'RPAREN'):
                result_tokens.append('AND')

        if token_type == 'WORD':
            result_tokens.append(f'"{token_value}"')
        elif token_type == 'PHRASE':
            result_tokens.append(f'"{token_value}"')
        elif token_type in ('AND', 'OR', 'NOT'):
            result_tokens.append(token_value)
        elif token_type == 'LPAREN':
            result_tokens.append('(')
        elif token_type == 'RPAREN':
            result_tokens.append(')')

        prev_type = token_type

    return ' '.join(result_tokens)


def search_papers(
    query: Optional[str] = None,
    series: Optional[str] = None,
    item_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    box_number: Optional[int] = None,
    folder_number: Optional[int] = None,
    sort_by: str = 'date_sort',
    sort_order: str = 'DESC',
    limit: int = 50,
    offset: int = 0,
    fuzzy: bool = False,
    use_regex: bool = False,
    analysis_model: Optional[str] = None,
    language: Optional[str] = None,
    tags: Optional[list[str]] = None
) -> tuple[list[dict], int]:
    """
    Search papers with filters and full-text search.
    Supports fuzzy search, regex patterns, and exact tag filtering.
    Returns (results, total_count).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Register regex function for regex searches
    if use_regex:
        def regexp(pattern, string):
            if string is None:
                return False
            try:
                return bool(regex_module.search(pattern, string, regex_module.IGNORECASE))
            except:
                return False
        conn.create_function("REGEXP", 2, regexp)

    params = []
    where_clauses = []

    # Search based on mode
    if query:
        if use_regex:
            # Regex search - search in title and text_content
            where_clauses.append("(title REGEXP ? OR text_content REGEXP ?)")
            params.append(query)
            params.append(query)
        elif fuzzy:
            # Fuzzy search - use LIKE with wildcards for each word
            # Also searches partial matches and handles typos by matching substrings
            words = query.split()
            fuzzy_conditions = []
            for word in words:
                if len(word) >= 2:
                    # Create pattern that matches word with possible characters between
                    # e.g., "simon" matches "simons", "simeon", etc.
                    like_pattern = f'%{word}%'
                    fuzzy_conditions.append("(title LIKE ? OR text_content LIKE ?)")
                    params.append(like_pattern)
                    params.append(like_pattern)
            if fuzzy_conditions:
                # Any word match counts (OR)
                where_clauses.append(f"({' OR '.join(fuzzy_conditions)})")
        else:
            # Standard FTS5 search with boolean operator support
            # Supports: AND, OR, NOT, quoted phrases, parentheses
            fts_query = _build_fts_query(query)
            if fts_query:
                where_clauses.append("papers.id IN (SELECT rowid FROM papers_fts WHERE papers_fts MATCH ?)")
                params.append(fts_query)

    # Filter by series
    if series:
        where_clauses.append("papers.series = ?")
        params.append(series)

    # Filter by item type
    if item_type:
        where_clauses.append("papers.item_type = ?")
        params.append(item_type)

    # Date range
    if date_from:
        where_clauses.append("papers.date_sort >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("papers.date_sort <= ?")
        params.append(date_to)

    # Box and folder filters
    if box_number is not None:
        where_clauses.append("papers.box_number = ?")
        params.append(box_number)
    if folder_number is not None:
        where_clauses.append("papers.folder_number = ?")
        params.append(folder_number)

    # Analysis model filter
    if analysis_model:
        where_clauses.append("papers.analysis_model = ?")
        params.append(analysis_model)

    # Language filter
    if language:
        where_clauses.append("papers.language = ?")
        params.append(language)

    # Exact tag filtering (all specified tags must be present)
    if tags:
        for tag in tags:
            # Use JSON to check if tag exists in the tags array
            # Match exact tag (case-insensitive) within the JSON array
            where_clauses.append("papers.tags LIKE ?")
            # Escape special characters in tag for LIKE pattern
            escaped_tag = tag.replace('%', '\\%').replace('_', '\\_')
            params.append(f'%"{escaped_tag}"%')

    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

    # Get total count
    count_sql = f"SELECT COUNT(*) FROM papers WHERE {where_sql}"
    cursor.execute(count_sql, params)
    total_count = cursor.fetchone()[0]

    # Get results with pagination
    valid_sort_columns = {'date_sort', 'title', 'series', 'item_type', 'id',
                          'box_number', 'folder_number', 'archive_order'}

    # Special handling for archive order (box, folder, bundle, document)
    if sort_by == 'archive_order':
        order_sql = "box_number, folder_number, bundle_number, document_number"
        if sort_order.upper() == 'DESC':
            order_sql = "box_number DESC, folder_number DESC, bundle_number DESC, document_number DESC"
    elif sort_by not in valid_sort_columns:
        sort_by = 'date_sort'
        order_sql = f"{sort_by} {'DESC' if sort_order.upper() == 'DESC' else 'ASC'}"
    else:
        order_sql = f"{sort_by} {'DESC' if sort_order.upper() == 'DESC' else 'ASC'}"

    results_sql = f"""
        SELECT * FROM papers
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    cursor.execute(results_sql, params)
    results = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return results, total_count


def get_facets() -> dict:
    """Get counts for faceted search."""
    conn = get_connection()
    cursor = conn.cursor()

    # Series counts
    cursor.execute("""
        SELECT series, COUNT(*) as count
        FROM papers
        WHERE series IS NOT NULL
        GROUP BY series
        ORDER BY count DESC
    """)
    series = [(row['series'], row['count']) for row in cursor.fetchall()]

    # Item type counts
    cursor.execute("""
        SELECT item_type, COUNT(*) as count
        FROM papers
        WHERE item_type IS NOT NULL
        GROUP BY item_type
        ORDER BY count DESC
    """)
    item_types = [(row['item_type'], row['count']) for row in cursor.fetchall()]

    # Year distribution
    cursor.execute("""
        SELECT substr(date_sort, 1, 4) as year, COUNT(*) as count
        FROM papers
        WHERE date_sort IS NOT NULL AND length(date_sort) >= 4
        GROUP BY year
        ORDER BY year
    """)
    years = [(row['year'], row['count']) for row in cursor.fetchall()]

    # Box counts
    cursor.execute("""
        SELECT box_number, COUNT(*) as count
        FROM papers
        WHERE box_number IS NOT NULL
        GROUP BY box_number
        ORDER BY box_number
    """)
    boxes = [(row['box_number'], row['count']) for row in cursor.fetchall()]

    # Analysis model counts
    cursor.execute("""
        SELECT analysis_model, COUNT(*) as count
        FROM papers
        WHERE analysis_model IS NOT NULL
        GROUP BY analysis_model
        ORDER BY count DESC
    """)
    models = [(row['analysis_model'], row['count']) for row in cursor.fetchall()]

    # Language counts
    cursor.execute("""
        SELECT language, COUNT(*) as count
        FROM papers
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY count DESC
    """)
    languages = [(row['language'], row['count']) for row in cursor.fetchall()]

    # Total count
    cursor.execute("SELECT COUNT(*) FROM papers")
    total = cursor.fetchone()[0]

    conn.close()
    return {
        'series': series,
        'item_types': item_types,
        'years': years,
        'boxes': boxes,
        'models': models,
        'languages': languages,
        'total': total
    }


def get_paper_by_id(paper_id: int) -> Optional[dict]:
    """Get a single paper by ID."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM papers WHERE id = ?", (paper_id,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_folders_for_box(box_number: int) -> list[tuple[int, int]]:
    """Get folders and their counts for a given box."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT folder_number, COUNT(*) as count
        FROM papers
        WHERE box_number = ? AND folder_number IS NOT NULL
        GROUP BY folder_number
        ORDER BY folder_number
    """, (box_number,))
    folders = [(row['folder_number'], row['count']) for row in cursor.fetchall()]
    conn.close()
    return folders


def get_archive_structure() -> dict:
    """Get the complete archive box/folder structure."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get box/folder structure
    cursor.execute("""
        SELECT box_number, folder_number, COUNT(*) as count
        FROM papers
        WHERE box_number IS NOT NULL
        GROUP BY box_number, folder_number
        ORDER BY box_number, folder_number
    """)

    structure = {}
    for row in cursor.fetchall():
        box = row['box_number']
        folder = row['folder_number']
        count = row['count']

        if box not in structure:
            structure[box] = {'folders': {}, 'total': 0}
        structure[box]['folders'][folder] = count
        structure[box]['total'] += count

    conn.close()
    return structure


def update_local_pdf_path(paper_id: int, local_path: str) -> bool:
    """Update the local PDF path for a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE papers SET local_pdf_path = ? WHERE id = ?", (local_path, paper_id))
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def get_papers_for_download(limit: int = None) -> list[dict]:
    """Get papers that have archive info but no local PDF yet."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        SELECT id, box_number, folder_number, bundle_number, document_number
        FROM papers
        WHERE box_number IS NOT NULL
          AND folder_number IS NOT NULL
          AND bundle_number IS NOT NULL
          AND document_number IS NOT NULL
          AND (local_pdf_path IS NULL OR local_pdf_path = '')
        ORDER BY box_number, folder_number, bundle_number, document_number
    """
    if limit:
        sql += f" LIMIT {limit}"
    cursor.execute(sql)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_papers_for_ocr(limit: int = None) -> list[dict]:
    """Get papers that have local PDFs but haven't been OCR'd yet."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        SELECT id, local_pdf_path, title
        FROM papers
        WHERE local_pdf_path IS NOT NULL
          AND local_pdf_path != ''
          AND (ocr_status IS NULL OR ocr_status = 'pending')
        ORDER BY id
    """
    if limit:
        sql += f" LIMIT {limit}"
    cursor.execute(sql)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def update_text_content(paper_id: int, text_content: str, ocr_status: str = 'completed') -> bool:
    """Update the OCR text content for a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE papers SET text_content = ?, ocr_status = ? WHERE id = ?",
        (text_content, ocr_status, paper_id)
    )
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def update_ocr_status(paper_id: int, status: str) -> bool:
    """Update just the OCR status for a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE papers SET ocr_status = ? WHERE id = ?", (status, paper_id))
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def get_papers_for_streaming_ocr(limit: int = None) -> list[dict]:
    """Get papers that have archive info but haven't been OCR'd yet (for streaming OCR)."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        SELECT id, title, box_number, folder_number, bundle_number, document_number
        FROM papers
        WHERE box_number IS NOT NULL
          AND folder_number IS NOT NULL
          AND bundle_number IS NOT NULL
          AND document_number IS NOT NULL
          AND (ocr_status IS NULL OR ocr_status = 'pending')
        ORDER BY box_number, folder_number, bundle_number, document_number
    """
    if limit:
        sql += f" LIMIT {limit}"
    cursor.execute(sql)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def star_paper(paper_id: int) -> bool:
    """Star a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE papers SET starred = 1, starred_at = CURRENT_TIMESTAMP WHERE id = ?",
        (paper_id,)
    )
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def unstar_paper(paper_id: int) -> bool:
    """Unstar a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE papers SET starred = 0, starred_at = NULL WHERE id = ?",
        (paper_id,)
    )
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def get_starred_papers() -> list[dict]:
    """Get all starred papers."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM papers
        WHERE starred = 1
        ORDER BY starred_at DESC
    """)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_starred_count() -> int:
    """Get count of starred papers."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM papers WHERE starred = 1")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_papers_for_analysis(limit: int = None) -> list[dict]:
    """Get papers that have OCR text but haven't been analyzed yet."""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        SELECT id, title, text_content, series, item_type, date
        FROM papers
        WHERE text_content IS NOT NULL
          AND text_content != ''
          AND (analysis_status IS NULL OR analysis_status = 'pending')
        ORDER BY id
    """
    if limit:
        sql += f" LIMIT {limit}"
    cursor.execute(sql)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def update_paper_analysis(paper_id: int, summary: str, tags: str, language: str, status: str = 'completed', model: str = None) -> bool:
    """Update the analysis fields for a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE papers SET summary = ?, tags = ?, language = ?, analysis_status = ?, analysis_model = ? WHERE id = ?",
        (summary, tags, language, status, model, paper_id)
    )
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def update_analysis_status(paper_id: int, status: str) -> bool:
    """Update just the analysis status for a paper."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE papers SET analysis_status = ? WHERE id = ?", (status, paper_id))
    conn.commit()
    updated = cursor.rowcount > 0
    conn.close()
    return updated


def save_archive_summary(summary_type: str, box_number: int, folder_number: Optional[int],
                         summary: str, model: str = None) -> bool:
    """Save or update an archive summary (box or folder)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO archive_summaries (summary_type, box_number, folder_number, summary, model, generated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(summary_type, box_number, folder_number) DO UPDATE SET
                summary = excluded.summary,
                model = excluded.model,
                generated_at = CURRENT_TIMESTAMP
        """, (summary_type, box_number, folder_number, summary, model))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error saving archive summary: {e}")
        return False
    finally:
        conn.close()


def get_archive_summaries() -> dict:
    """Get all archive summaries organized by box and folder."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT summary_type, box_number, folder_number, summary, model, generated_at
        FROM archive_summaries
        ORDER BY box_number, folder_number
    """)

    summaries = {'boxes': {}, 'folders': {}}
    for row in cursor.fetchall():
        if row['summary_type'] == 'box':
            summaries['boxes'][row['box_number']] = {
                'summary': row['summary'],
                'model': row['model'],
                'generated_at': row['generated_at']
            }
        else:
            key = (row['box_number'], row['folder_number'])
            summaries['folders'][key] = {
                'summary': row['summary'],
                'model': row['model'],
                'generated_at': row['generated_at']
            }

    conn.close()
    return summaries


def get_boxes_for_summarization() -> list[dict]:
    """Get boxes that need summarization (have documents but no summary)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT p.box_number, COUNT(*) as doc_count
        FROM papers p
        LEFT JOIN archive_summaries s ON s.summary_type = 'box' AND s.box_number = p.box_number
        WHERE p.box_number IS NOT NULL
          AND s.id IS NULL
        GROUP BY p.box_number
        ORDER BY p.box_number
    """)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_folders_for_summarization() -> list[dict]:
    """Get folders that need summarization (have documents but no summary)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT p.box_number, p.folder_number, COUNT(*) as doc_count
        FROM papers p
        LEFT JOIN archive_summaries s ON s.summary_type = 'folder'
            AND s.box_number = p.box_number AND s.folder_number = p.folder_number
        WHERE p.box_number IS NOT NULL AND p.folder_number IS NOT NULL
          AND s.id IS NULL
        GROUP BY p.box_number, p.folder_number
        ORDER BY p.box_number, p.folder_number
    """)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_folder_documents(box_number: int, folder_number: int, limit: int = 50) -> list[dict]:
    """Get documents from a specific folder for summarization."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, summary, text_content, date
        FROM papers
        WHERE box_number = ? AND folder_number = ?
        ORDER BY bundle_number, document_number
        LIMIT ?
    """, (box_number, folder_number, limit))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_box_documents(box_number: int, limit: int = 100) -> list[dict]:
    """Get documents from a specific box for summarization."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, summary, folder_number, date
        FROM papers
        WHERE box_number = ?
        ORDER BY folder_number, bundle_number, document_number
        LIMIT ?
    """, (box_number, limit))
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_related_papers(paper_id: int, limit: int = 10) -> dict:
    """Get related papers grouped by relationship type."""
    paper = get_paper_by_id(paper_id)
    if not paper:
        return {}

    result = {
        'same_folder': [],
        'shared_tags': []
    }

    conn = get_connection()
    cursor = conn.cursor()

    # Track IDs we've already included to avoid duplicates
    seen_ids = {paper_id}

    # 1. Same folder (excluding self)
    if paper.get('box_number') and paper.get('folder_number'):
        cursor.execute("""
            SELECT id, title, date, series, item_type, box_number, folder_number,
                   bundle_number, document_number, summary, tags
            FROM papers
            WHERE box_number = ? AND folder_number = ? AND id != ?
            ORDER BY bundle_number, document_number
            LIMIT ?
        """, (paper['box_number'], paper['folder_number'], paper_id, limit))
        for row in cursor.fetchall():
            result['same_folder'].append(dict(row))
            seen_ids.add(row['id'])

    # 2. Shared tags (excluding above)
    if paper.get('tags'):
        try:
            import json
            paper_tags = json.loads(paper['tags'])
            if paper_tags:
                # Find papers with overlapping tags
                placeholders = ','.join('?' * len(seen_ids))
                cursor.execute(f"""
                    SELECT id, title, date, series, item_type, box_number, folder_number,
                           bundle_number, document_number, summary, tags
                    FROM papers
                    WHERE tags IS NOT NULL AND tags != '[]' AND id NOT IN ({placeholders})
                """, tuple(seen_ids))

                # Score papers by number of shared tags
                candidates = []
                paper_tags_lower = {t.lower() for t in paper_tags}
                for row in cursor.fetchall():
                    try:
                        other_tags = json.loads(row['tags'])
                        other_tags_lower = {t.lower() for t in other_tags}
                        shared = paper_tags_lower & other_tags_lower
                        if shared:
                            paper_dict = dict(row)
                            paper_dict['shared_tag_count'] = len(shared)
                            paper_dict['shared_tags'] = list(shared)
                            candidates.append(paper_dict)
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Sort by number of shared tags and take top N
                candidates.sort(key=lambda x: x['shared_tag_count'], reverse=True)
                result['shared_tags'] = candidates[:limit]
        except (json.JSONDecodeError, TypeError):
            pass

    conn.close()
    return result


if __name__ == "__main__":
    init_db()
