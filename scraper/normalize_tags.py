#!/usr/bin/env python3
"""
Tag normalization script for Herbert Simon papers.

This script helps identify and merge similar tags, such as:
- "Herbert Simon" and "Herbert A Simon"
- "Edward Feigenbaum" and "Feigenbaum"
- "Carnegie Mellon University" and "Carnegie-Mellon University"

Usage:
    python -m scraper.normalize_tags --find-similar    # Find similar tags
    python -m scraper.normalize_tags --apply rules.json # Apply normalization rules
    python -m scraper.normalize_tags --interactive     # Interactive mode
"""

import json
import re
import sys
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db import get_connection


def get_all_tags():
    """Get all unique tags with their counts."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT tags FROM papers WHERE tags IS NOT NULL AND tags != '[]'")

    tag_counts = defaultdict(int)
    for row in cursor.fetchall():
        try:
            tags = json.loads(row['tags'])
            for tag in tags:
                tag_counts[tag] += 1
        except (json.JSONDecodeError, TypeError):
            pass

    conn.close()
    return tag_counts


def normalize_tag(tag):
    """Normalize a tag for comparison (lowercase, remove punctuation, etc.)"""
    # Lowercase
    normalized = tag.lower()
    # Remove common prefixes/suffixes
    normalized = re.sub(r'^(dr\.?|prof\.?|mr\.?|mrs\.?|ms\.?)\s+', '', normalized)
    # Remove middle initials for names
    normalized = re.sub(r'\s+[a-z]\.?\s+', ' ', normalized)
    # Remove punctuation
    normalized = re.sub(r'[^\w\s]', '', normalized)
    # Collapse whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def similarity(s1, s2):
    """Calculate similarity ratio between two strings."""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def find_similar_tags(tag_counts, threshold=0.8):
    """Find groups of similar tags."""
    tags = list(tag_counts.keys())
    normalized = {tag: normalize_tag(tag) for tag in tags}

    # Group by normalized form
    groups = defaultdict(list)
    for tag, norm in normalized.items():
        groups[norm].append(tag)

    # Find exact normalized matches
    exact_matches = {k: v for k, v in groups.items() if len(v) > 1}

    # Find fuzzy matches (for tags that didn't match exactly)
    fuzzy_matches = []
    single_tags = [tags[0] for norm, tags in groups.items() if len(tags) == 1]

    checked = set()
    for i, tag1 in enumerate(single_tags):
        if tag1 in checked:
            continue
        similar = [tag1]
        for tag2 in single_tags[i+1:]:
            if tag2 in checked:
                continue
            # Check if one is substring of other
            if tag1.lower() in tag2.lower() or tag2.lower() in tag1.lower():
                similar.append(tag2)
                checked.add(tag2)
            # Check similarity ratio
            elif similarity(tag1, tag2) >= threshold:
                similar.append(tag2)
                checked.add(tag2)

        if len(similar) > 1:
            fuzzy_matches.append(similar)
        checked.add(tag1)

    return exact_matches, fuzzy_matches


def print_similar_tags(tag_counts, threshold=0.8):
    """Print groups of similar tags for review."""
    exact_matches, fuzzy_matches = find_similar_tags(tag_counts, threshold)

    print("=" * 60)
    print("EXACT NORMALIZED MATCHES")
    print("(These tags normalize to the same string)")
    print("=" * 60)

    for norm, tags in sorted(exact_matches.items(), key=lambda x: -sum(tag_counts[t] for t in x[1])):
        total_count = sum(tag_counts[t] for t in tags)
        print(f"\nNormalized: '{norm}' (total: {total_count})")
        for tag in sorted(tags, key=lambda t: -tag_counts[t]):
            print(f"  - '{tag}' ({tag_counts[tag]})")

    print("\n" + "=" * 60)
    print("FUZZY MATCHES")
    print("(These tags are similar but not exact matches)")
    print("=" * 60)

    for group in sorted(fuzzy_matches, key=lambda g: -sum(tag_counts[t] for t in g)):
        total_count = sum(tag_counts[t] for t in group)
        print(f"\nSimilar group (total: {total_count}):")
        for tag in sorted(group, key=lambda t: -tag_counts[t]):
            print(f"  - '{tag}' ({tag_counts[tag]})")


def generate_rules(tag_counts, threshold=0.8):
    """Generate normalization rules (maps variant -> canonical)."""
    exact_matches, fuzzy_matches = find_similar_tags(tag_counts, threshold)

    rules = {}

    # For exact matches, use the most common variant as canonical
    for norm, tags in exact_matches.items():
        canonical = max(tags, key=lambda t: tag_counts[t])
        for tag in tags:
            if tag != canonical:
                rules[tag] = canonical

    # For fuzzy matches, use the most common variant as canonical
    for group in fuzzy_matches:
        canonical = max(group, key=lambda t: tag_counts[t])
        for tag in group:
            if tag != canonical:
                rules[tag] = canonical

    return rules


def apply_rules(rules):
    """Apply normalization rules to the database."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, tags FROM papers WHERE tags IS NOT NULL AND tags != '[]'")
    rows = cursor.fetchall()

    updated = 0
    for row in rows:
        try:
            tags = json.loads(row['tags'])
            new_tags = []
            changed = False

            for tag in tags:
                if tag in rules:
                    new_tags.append(rules[tag])
                    changed = True
                else:
                    new_tags.append(tag)

            # Remove duplicates while preserving order
            seen = set()
            unique_tags = []
            for tag in new_tags:
                if tag.lower() not in seen:
                    seen.add(tag.lower())
                    unique_tags.append(tag)

            if changed or len(unique_tags) != len(new_tags):
                cursor.execute(
                    "UPDATE papers SET tags = ? WHERE id = ?",
                    (json.dumps(unique_tags), row['id'])
                )
                updated += 1

        except (json.JSONDecodeError, TypeError):
            pass

    conn.commit()
    conn.close()

    print(f"Updated {updated} papers")


def interactive_mode(tag_counts):
    """Interactive mode for reviewing and merging tags."""
    exact_matches, fuzzy_matches = find_similar_tags(tag_counts)
    all_groups = list(exact_matches.values()) + fuzzy_matches

    rules = {}

    print("\nInteractive tag normalization")
    print("For each group, enter the number of the canonical tag, or 's' to skip, 'q' to quit\n")

    for group in sorted(all_groups, key=lambda g: -sum(tag_counts[t] for t in g)):
        print(f"\nGroup (total: {sum(tag_counts[t] for t in group)}):")
        sorted_tags = sorted(group, key=lambda t: -tag_counts[t])
        for i, tag in enumerate(sorted_tags, 1):
            print(f"  {i}. '{tag}' ({tag_counts[tag]})")

        while True:
            choice = input("Choose canonical [1] or s/q: ").strip().lower()
            if choice == 'q':
                break
            if choice == 's' or choice == '':
                if choice == '':
                    # Default to first (most common)
                    canonical = sorted_tags[0]
                    for tag in sorted_tags[1:]:
                        rules[tag] = canonical
                    print(f"  -> Using '{canonical}' as canonical")
                break
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(sorted_tags):
                    canonical = sorted_tags[idx]
                    for tag in sorted_tags:
                        if tag != canonical:
                            rules[tag] = canonical
                    print(f"  -> Using '{canonical}' as canonical")
                    break
            except ValueError:
                pass
            print("Invalid choice, try again")

        if choice == 'q':
            break

    if rules:
        print(f"\n{len(rules)} normalization rules created")
        save = input("Save rules to file? [y/N]: ").strip().lower()
        if save == 'y':
            filename = input("Filename [tag_rules.json]: ").strip() or "tag_rules.json"
            with open(filename, 'w') as f:
                json.dump(rules, f, indent=2)
            print(f"Saved to {filename}")

        apply_now = input("Apply rules now? [y/N]: ").strip().lower()
        if apply_now == 'y':
            apply_rules(rules)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Tag normalization for Simon papers")
    parser.add_argument('--find-similar', action='store_true', help='Find similar tags')
    parser.add_argument('--generate-rules', type=str, metavar='FILE', help='Generate rules file')
    parser.add_argument('--apply', type=str, metavar='FILE', help='Apply rules from file')
    parser.add_argument('--interactive', action='store_true', help='Interactive mode')
    parser.add_argument('--threshold', type=float, default=0.8, help='Similarity threshold (0-1)')

    args = parser.parse_args()

    tag_counts = get_all_tags()
    print(f"Found {len(tag_counts)} unique tags\n")

    if args.find_similar:
        print_similar_tags(tag_counts, args.threshold)
    elif args.generate_rules:
        rules = generate_rules(tag_counts, args.threshold)
        with open(args.generate_rules, 'w') as f:
            json.dump(rules, f, indent=2)
        print(f"Generated {len(rules)} rules, saved to {args.generate_rules}")
    elif args.apply:
        with open(args.apply) as f:
            rules = json.load(f)
        print(f"Loaded {len(rules)} rules from {args.apply}")
        apply_rules(rules)
    elif args.interactive:
        interactive_mode(tag_counts)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
