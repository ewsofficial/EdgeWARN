#!/usr/bin/env python3
"""
map_dependencies.py - Map import/require dependencies in a project
Usage: python map_dependencies.py /path/to/project
"""

import os
import sys
import re
from collections import defaultdict

def find_python_imports(filepath):
    """Extract imports from a Python file."""
    imports = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                # Match: import x, from x import y
                match = re.match(r'^(?:from\s+(\S+)|import\s+(\S+))', line.strip())
                if match:
                    module = match.group(1) or match.group(2)
                    module = module.split('.')[0]  # Get root module
                    imports.append(module)
    except:
        pass
    return imports

def find_js_imports(filepath):
    """Extract imports from a JavaScript file."""
    imports = []
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # Match: require('x'), import x from 'x'
            patterns = [
                r"require\(['\"]([^'\"]+)['\"]\)",
                r"from\s+['\"]([^'\"]+)['\"]",
                r"import\s+['\"]([^'\"]+)['\"]"
            ]
            for pattern in patterns:
                matches = re.findall(pattern, content)
                imports.extend(matches)
    except:
        pass
    return imports

def analyze_project(project_dir):
    """Analyze all files in a project."""
    dependencies = defaultdict(list)
    
    for root, dirs, files in os.walk(project_dir):
        # Skip common non-source directories
        dirs[:] = [d for d in dirs if d not in ['node_modules', '.git', '__pycache__', '.next', 'dist']]
        
        for file in files:
            filepath = os.path.join(root, file)
            rel_path = os.path.relpath(filepath, project_dir)
            
            if file.endswith('.py'):
                imports = find_python_imports(filepath)
                dependencies[rel_path] = imports
            elif file.endswith(('.js', '.ts', '.jsx', '.tsx')):
                imports = find_js_imports(filepath)
                dependencies[rel_path] = imports
    
    return dependencies

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} /path/to/project")
        sys.exit(1)
    
    project_dir = sys.argv[1]
    deps = analyze_project(project_dir)
    
    print("# Dependency Map\n")
    
    # Group by directory
    by_dir = defaultdict(list)
    for filepath, imports in sorted(deps.items()):
        dirname = os.path.dirname(filepath) or '.'
        by_dir[dirname].append((filepath, imports))
    
    for dirname, files in sorted(by_dir.items()):
        print(f"## {dirname}/\n")
        for filepath, imports in files:
            if imports:
                print(f"### {os.path.basename(filepath)}")
                for imp in sorted(set(imports)):
                    print(f"- {imp}")
                print()

if __name__ == "__main__":
    main()
