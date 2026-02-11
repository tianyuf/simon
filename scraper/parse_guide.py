"""Parser for the Herbert A. Simon Collection Finding Aid."""

import re
from pathlib import Path

GUIDE_PATH = Path(__file__).parent.parent / "guide"

# Series header pattern in the container list section (after line ~335)
# Matches: "Series I.  Personal Papers -- (1909) 1929-1979"
# Also matches: "Series IX: Correspondence 1940-2001" (colon variant)
SERIES_RE = re.compile(r'^Series\s+([IVX]+)[.:]\s+(.+?)(?:\s+--\s*.+|\s+\d{4}.+)?$')

# Box patterns
BOX_RE = re.compile(r'^(?:Over-Size\s+)?Box\s+(\d+)$')
BOX_CONTINUED_RE = re.compile(r'^(?:Over-Size\s+)?Box\s+\d+\s*-\s*Continued')

# Folder pattern: FF followed by digits, then tab, then description
FF_RE = re.compile(r'^FF(\d+)\t(.+)$')


def parse_guide(guide_path=None):
    """
    Parse the finding aid guide file.

    Returns:
        boxes: dict mapping box_number -> {title, series, series_number, is_oversize}
        folders: dict mapping folder_number -> {box_number, description, series, series_number}
    """
    path = Path(guide_path) if guide_path else GUIDE_PATH
    text = path.read_text(encoding='utf-8')
    lines = text.split('\n')

    boxes = {}
    folders = {}

    # State tracking
    current_series = None
    current_series_number = None
    current_box = None
    in_container_list = False

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        # Detect start of container list section (first Series header after front matter)
        if not in_container_list:
            if line.startswith('Series I.') and 'Personal Papers' in line and '--' in line:
                in_container_list = True
                m = SERIES_RE.match(line)
                if m:
                    current_series_number = m.group(1)
                    current_series = m.group(2).strip()
                i += 1
                continue
            i += 1
            continue

        # Check for Series header
        m = SERIES_RE.match(line)
        if m:
            current_series_number = m.group(1)
            current_series = m.group(2).strip()
            i += 1
            continue

        # Check for "Box N - Continued" (skip, don't update box)
        if BOX_CONTINUED_RE.match(line):
            i += 1
            continue

        # Check for Box header
        m = BOX_RE.match(line)
        if m:
            box_num = int(m.group(1))
            is_oversize = line.startswith('Over-Size')
            current_box = box_num

            # Get the box title from the next non-blank line
            title = None
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line:
                    # The title line is the descriptive text (not an FF entry or another Box)
                    if not FF_RE.match(next_line) and not BOX_RE.match(next_line):
                        title = next_line
                    break
                j += 1

            if box_num not in boxes:
                boxes[box_num] = {
                    'title': title,
                    'series': current_series,
                    'series_number': current_series_number,
                    'is_oversize': is_oversize,
                }
            i += 1
            continue

        # Check for folder entry
        m = FF_RE.match(line)
        if m:
            ff_num = int(m.group(1))
            description = m.group(2).strip()
            folders[ff_num] = {
                'box_number': current_box,
                'description': description,
                'series': current_series,
                'series_number': current_series_number,
            }
            i += 1
            continue

        i += 1

    return boxes, folders


def print_summary(boxes, folders):
    """Print a summary of parsed finding aid data."""
    print(f"\nFinding Aid Summary")
    print(f"{'=' * 50}")
    print(f"Total boxes: {len(boxes)}")
    print(f"Total folders (FF entries): {len(folders)}")

    if folders:
        ff_nums = sorted(folders.keys())
        print(f"FF range: FF{ff_nums[0]} - FF{ff_nums[-1]}")

    if boxes:
        box_nums = sorted(boxes.keys())
        print(f"Box range: {box_nums[0]} - {box_nums[-1]}")

    # Count by series
    series_counts = {}
    for f in folders.values():
        s = f.get('series', 'Unknown')
        series_counts[s] = series_counts.get(s, 0) + 1

    print(f"\nFolders by Series:")
    for series, count in sorted(series_counts.items(), key=lambda x: -x[1]):
        print(f"  {series}: {count}")


if __name__ == '__main__':
    boxes, folders = parse_guide()
    print_summary(boxes, folders)
