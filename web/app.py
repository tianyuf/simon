"""Flask web application for searching Herbert Simon papers."""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / '.env')

import json
import re
from markupsafe import Markup, escape
from flask import Flask, render_template, request, jsonify, send_from_directory, abort, redirect
from db import search_papers, get_facets, get_paper_by_id, init_db, get_archive_structure, get_folders_for_box, get_connection, star_paper, unstar_paper, get_starred_papers, get_starred_count, get_archive_summaries, get_related_papers, get_finding_aid_box_titles, get_finding_aid_folder_descriptions, get_missing_from_collection

# Import OCR functions
try:
    from scraper.ocr_pdfs import extract_text_from_pdf, PDF_DIR, PYMUPDF_AVAILABLE, TESSERACT_AVAILABLE
    OCR_AVAILABLE = PYMUPDF_AVAILABLE or TESSERACT_AVAILABLE
except ImportError:
    OCR_AVAILABLE = False
    PDF_DIR = None

app = Flask(__name__)


@app.context_processor
def inject_url_prefix():
    """Inject URL prefix into all templates for building URLs."""
    import os
    prefix = os.environ.get('URL_PREFIX', '')
    return dict(url_prefix=prefix)


@app.template_filter('folder_label')
def folder_label_filter(description):
    """Strip 'Simon, Herbert A. -- Series -- ' prefix from finding aid descriptions."""
    if not description:
        return ''
    if description.startswith('Simon'):
        parts = description.split(' -- ')
        if len(parts) > 2:
            return ' -- '.join(parts[2:])
    return description


@app.template_filter('fromjson')
def fromjson_filter(value):
    """Parse JSON string to Python object."""
    if not value:
        return []
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []


@app.template_filter('highlight_snippet')
def highlight_snippet_filter(text, query, snippet_length=300):
    """
    Extract a snippet around the search term and highlight matches.
    If no query or no match, returns the first snippet_length characters.
    """
    if not text:
        return ''

    text = str(text)

    if not query:
        # No search query, just return first part
        snippet = text[:snippet_length]
        if len(text) > snippet_length:
            snippet += '...'
        return Markup(escape(snippet))

    # Find the first occurrence of any search term (case-insensitive)
    query_lower = query.lower()
    text_lower = text.lower()

    # Split query into words for multi-word searches
    search_terms = query_lower.split()

    # Find the first match position
    first_match_pos = -1
    for term in search_terms:
        pos = text_lower.find(term)
        if pos != -1 and (first_match_pos == -1 or pos < first_match_pos):
            first_match_pos = pos

    if first_match_pos == -1:
        # No match found, return first part
        snippet = text[:snippet_length]
        if len(text) > snippet_length:
            snippet += '...'
        return Markup(escape(snippet))

    # Extract snippet centered around the match
    context_before = 100
    start = max(0, first_match_pos - context_before)
    end = start + snippet_length

    snippet = text[start:end]
    if start > 0:
        snippet = '...' + snippet
    if end < len(text):
        snippet += '...'

    # Highlight all occurrences of search terms
    escaped_snippet = str(escape(snippet))
    for term in search_terms:
        if len(term) >= 2:  # Only highlight terms with 2+ chars
            pattern = re.compile(re.escape(term), re.IGNORECASE)
            escaped_snippet = pattern.sub(
                lambda m: f'<mark>{escape(m.group(0))}</mark>',
                escaped_snippet
            )

    return Markup(escaped_snippet)


@app.route('/')
def index():
    """Main search page."""
    # Get search parameters
    query = request.args.get('q', '').strip()
    series = request.args.get('series', '').strip()
    item_type = request.args.get('type', '').strip()
    date_from = request.args.get('from', '').strip()
    date_to = request.args.get('to', '').strip()
    box_str = request.args.get('box', '').strip()
    folder_str = request.args.get('folder', '').strip()
    sort_by = request.args.get('sort', 'date_sort')
    sort_order = request.args.get('order', 'DESC')
    page = max(1, int(request.args.get('page', 1)))
    per_page = min(100, max(10, int(request.args.get('per_page', 25))))

    # New filters
    analysis_model = request.args.get('model', '').strip()
    language = request.args.get('lang', '').strip()
    tags_param = request.args.getlist('tag')  # Multiple tags supported

    # Search mode options
    search_mode = request.args.get('mode', 'normal')  # normal, fuzzy, regex
    fuzzy = search_mode == 'fuzzy'
    use_regex = search_mode == 'regex'

    # Coverage filter: digitized, missing, all
    include_coverage = request.args.get('coverage', 'all')

    # Parse box/folder as integers
    box_number = int(box_str) if box_str.isdigit() else None
    folder_number = int(folder_str) if folder_str.isdigit() else None

    # Perform search
    offset = (page - 1) * per_page
    results, total = search_papers(
        query=query if query else None,
        series=series if series else None,
        item_type=item_type if item_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        box_number=box_number,
        folder_number=folder_number,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=per_page,
        offset=offset,
        fuzzy=fuzzy,
        use_regex=use_regex,
        analysis_model=analysis_model if analysis_model else None,
        language=language if language else None,
        tags=tags_param if tags_param else None,
        include_coverage=include_coverage
    )

    # Calculate pagination
    total_pages = (total + per_page - 1) // per_page

    # Get facets for sidebar
    facets = get_facets()

    # Get folders for selected box (for dynamic dropdown)
    folders_for_box = []
    if box_number:
        folders_for_box = get_folders_for_box(box_number)

    return render_template(
        'index.html',
        results=results,
        total=total,
        query=query,
        series=series,
        item_type=item_type,
        date_from=date_from,
        date_to=date_to,
        box_number=box_number,
        box_str=box_str,
        folder_number=folder_number,
        folder_str=folder_str,
        folders_for_box=folders_for_box,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        facets=facets,
        search_mode=search_mode,
        analysis_model=analysis_model,
        language=language,
        tags=tags_param,
        include_coverage=include_coverage
    )


@app.route('/paper/<int:paper_id>')
def paper_detail(paper_id):
    """Paper detail page."""
    paper = get_paper_by_id(paper_id)
    if not paper:
        return "Paper not found", 404
    related = get_related_papers(paper_id)
    return render_template('paper.html', paper=paper, related=related)


@app.route('/api/search')
def api_search():
    """API endpoint for search (JSON response)."""
    query = request.args.get('q', '').strip()
    series = request.args.get('series', '').strip()
    item_type = request.args.get('type', '').strip()
    date_from = request.args.get('from', '').strip()
    date_to = request.args.get('to', '').strip()
    sort_by = request.args.get('sort', 'date_sort')
    sort_order = request.args.get('order', 'DESC')
    page = max(1, int(request.args.get('page', 1)))
    per_page = min(100, max(10, int(request.args.get('per_page', 25))))

    offset = (page - 1) * per_page
    results, total = search_papers(
        query=query if query else None,
        series=series if series else None,
        item_type=item_type if item_type else None,
        date_from=date_from if date_from else None,
        date_to=date_to if date_to else None,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=per_page,
        offset=offset
    )

    return jsonify({
        'results': results,
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page
    })


@app.route('/api/facets')
def api_facets():
    """API endpoint for facets."""
    return jsonify(get_facets())


@app.route('/archive')
def archive_browser():
    """Browse papers by physical archive structure (box/folder)."""
    structure = get_archive_structure()
    box_titles = get_finding_aid_box_titles()
    folder_descriptions = get_finding_aid_folder_descriptions()
    return render_template('archive.html', structure=structure,
                           box_titles=box_titles, folder_descriptions=folder_descriptions)


@app.route('/missing')
def missing_items():
    """Show items from the finding aid not in the digital collection."""
    data = get_missing_from_collection()
    return render_template('missing.html', data=data)


@app.route('/api/folders/<int:box_number>')
def api_folders(box_number):
    """Get folders for a given box."""
    folders = get_folders_for_box(box_number)
    return jsonify(folders)


@app.route('/stats')
def analysis_stats():
    """View analysis statistics."""
    conn = get_connection()
    cursor = conn.cursor()

    stats = {}

    # Total papers
    cursor.execute("SELECT COUNT(*) FROM papers")
    stats['total_papers'] = cursor.fetchone()[0]

    # Papers with OCR text
    cursor.execute("SELECT COUNT(*) FROM papers WHERE text_content IS NOT NULL AND text_content != ''")
    stats['with_ocr'] = cursor.fetchone()[0]

    # Analyzed papers
    cursor.execute("SELECT COUNT(*) FROM papers WHERE analysis_status = 'completed'")
    stats['analyzed'] = cursor.fetchone()[0]

    # Pending analysis
    cursor.execute("""
        SELECT COUNT(*) FROM papers
        WHERE text_content IS NOT NULL AND text_content != ''
        AND (analysis_status IS NULL OR analysis_status = 'pending')
    """)
    stats['pending'] = cursor.fetchone()[0]

    # Model usage
    cursor.execute("""
        SELECT analysis_model, COUNT(*) as count
        FROM papers
        WHERE analysis_model IS NOT NULL
        GROUP BY analysis_model
        ORDER BY count DESC
    """)
    stats['models'] = [(row['analysis_model'], row['count']) for row in cursor.fetchall()]

    # Language breakdown
    cursor.execute("""
        SELECT language, COUNT(*) as count
        FROM papers
        WHERE language IS NOT NULL
        GROUP BY language
        ORDER BY count DESC
        LIMIT 15
    """)
    stats['languages'] = [(row['language'], row['count']) for row in cursor.fetchall()]

    # Top tags
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

    stats['top_tags'] = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:50]
    stats['max_tag_count'] = stats['top_tags'][0][1] if stats['top_tags'] else 1

    # Recently analyzed
    cursor.execute("""
        SELECT id, title, analysis_model, language
        FROM papers
        WHERE analysis_status = 'completed'
        ORDER BY id DESC
        LIMIT 10
    """)
    stats['recent'] = [dict(row) for row in cursor.fetchall()]

    conn.close()

    return render_template('stats.html', stats=stats)


# Legacy route redirect
@app.route('/processed')
def processed_redirect():
    """Redirect old OCR page to stats."""
    return redirect('/stats')


# PDF directory
PDF_DIR = Path(__file__).parent.parent / "pdfs"


@app.route('/api/star/<int:paper_id>', methods=['POST'])
def api_star(paper_id):
    """Star a paper."""
    if star_paper(paper_id):
        return jsonify({'success': True, 'starred': True})
    return jsonify({'success': False, 'error': 'Paper not found'}), 404


@app.route('/api/unstar/<int:paper_id>', methods=['POST'])
def api_unstar(paper_id):
    """Unstar a paper."""
    if unstar_paper(paper_id):
        return jsonify({'success': True, 'starred': False})
    return jsonify({'success': False, 'error': 'Paper not found'}), 404


@app.route('/api/starred')
def api_starred():
    """Get all starred papers."""
    papers = get_starred_papers()
    return jsonify({'papers': papers, 'count': len(papers)})


@app.route('/api/related/<int:paper_id>')
def api_related(paper_id):
    """Get related papers for a given paper."""
    paper = get_paper_by_id(paper_id)
    if not paper:
        return jsonify({'error': 'Paper not found'}), 404
    related = get_related_papers(paper_id)
    return jsonify({
        'paper_id': paper_id,
        'paper_title': paper.get('title'),
        'related': related
    })


@app.route('/api/reocr/<int:paper_id>', methods=['POST'])
def api_reocr(paper_id):
    """Re-OCR a specific paper."""
    if not OCR_AVAILABLE:
        return jsonify({'success': False, 'error': 'OCR not available. Install PyMuPDF or Tesseract.'}), 503

    # Get paper details
    paper = get_paper_by_id(paper_id)
    if not paper:
        return jsonify({'success': False, 'error': 'Paper not found'}), 404

    if not paper.get('local_pdf_path'):
        return jsonify({'success': False, 'error': 'No local PDF for this paper'}), 400

    # Import here to avoid circular imports
    from scraper.ocr_pdfs import extract_text_from_pdf, PDF_DIR
    from db import update_text_content, update_ocr_status

    pdf_path = PDF_DIR / paper['local_pdf_path']
    if not pdf_path.exists():
        return jsonify({'success': False, 'error': 'PDF file not found on disk'}), 404

    try:
        # Force OCR to re-extract
        text, method = extract_text_from_pdf(pdf_path, force_ocr=True)

        if text:
            update_text_content(paper_id, text, 'completed')
            # Also clear analysis so it can be re-analyzed
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE papers
                SET summary = NULL, tags = NULL, language = NULL,
                    analysis_status = NULL, analysis_model = NULL
                WHERE id = ?
            """, (paper_id,))
            conn.commit()
            conn.close()

            return jsonify({
                'success': True,
                'method': method,
                'text_length': len(text),
                'preview': text[:200] + '...' if len(text) > 200 else text
            })
        else:
            update_ocr_status(paper_id, 'failed')
            return jsonify({'success': False, 'error': 'OCR failed to extract text'}), 500

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/starred')
def starred_page():
    """View starred papers."""
    papers = get_starred_papers()
    return render_template('starred.html', papers=papers)


@app.route('/pdf/<path:filename>')
def serve_pdf(filename):
    """Serve local PDF files."""
    pdf_path = PDF_DIR / filename
    if not pdf_path.exists():
        abort(404)
    # Get the directory and file name parts
    directory = pdf_path.parent
    file_name = pdf_path.name
    return send_from_directory(directory, file_name, mimetype='application/pdf')


if __name__ == '__main__':
    # Initialize database if needed
    init_db()

    app.run(debug=True, port=5000)
