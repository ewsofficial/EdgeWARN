#!/usr/bin/env python3
"""
find_duplicates.py - Find duplicate code blocks in a project
Usage: python find_duplicates.py /path/to/project
"""

import os
import sys
import hashlib
from collections import defaultdict

def get_code_blocks(filepath, min_lines=5):
    """Extract code blocks from a file."""
    blocks = []
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except:
        return blocks
    
    # Sliding window to find duplicate blocks
    for start in range(len(lines) - min_lines + 1):
        block = ''.join(lines[start:start + min_lines])
        # Normalize whitespace
        normalized = ' '.join(block.split())
        if len(normalized) > 50:  # Skip very short blocks
            block_hash = hashlib.md5(normalized.encode()).hexdigest()
            blocks.append({
                'hash': block_hash,
                'start': start + 1,
                'end': start + min_lines,
                'content': block.strip()[:100]
            })
    
    return blocks

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} /path/to/project")
        sys.exit(1)
    
    project_dir = sys.argv[1]
    all_blocks = defaultdict(list)
    
    for root, dirs, files in os.walk(project_dir):
        dirs[:] = [d for d in dirs if d not in ['node_modules', '.git', '__pycache__']]
        
        for file in files:
            if file.endswith(('.py', '.js', '.ts')):
                filepath = os.path.join(root, file)
                blocks = get_code_blocks(filepath)
                rel_path = os.path.relpath(filepath, project_dir)
                
                for block in blocks:
                    all_blocks[block['hash']].append({
                        'file': rel_path,
                        'lines': f"{block['start']}-{block['end']}",
                        'preview': block['content']
                    })
    
    # Find duplicates
    duplicates = {h: locs for h, locs in all_blocks.items() if len(locs) > 1}
    
    print("# Duplicate Code Analysis\n")
    
    if not duplicates:
        print("**No duplicates found!**")
        return
    
    print(f"**Found {len(duplicates)} duplicate code blocks**\n")
    
    for i, (hash_val, locations) in enumerate(list(duplicates.items())[:20], 1):
        print(f"## Duplicate #{i}")
        print(f"**Found in {len(locations)} locations:**\n")
        for loc in locations:
            print(f"- `{loc['file']}` (lines {loc['lines']})")
        print(f"\n**Preview:**")
        print(f"```\n{locations[0]['preview']}...\n```\n")

if __name__ == "__main__":
    main()
