#!/usr/bin/env python3
"""
generate_docs.py - Generate documentation from source code
Usage: python generate_docs.py /path/to/source
"""

import os
import sys
import ast
import re

def extract_python_docs(filepath):
    """Extract documentation from Python file."""
    docs = {'module': '', 'classes': [], 'functions': []}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
    except:
        return docs
    
    docs['module'] = ast.get_docstring(tree) or ''
    
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            docs['classes'].append({
                'name': node.name,
                'docstring': ast.get_docstring(node) or '',
                'methods': [m.name for m in node.body if isinstance(m, ast.FunctionDef)]
            })
        elif isinstance(node, ast.FunctionDef):
            # Only top-level functions
            if not any(isinstance(n, ast.ClassDef) for n in ast.walk(tree) if hasattr(n, 'body') and node in getattr(n, 'body', [])):
                args = [arg.arg for arg in node.args.args]
                docs['functions'].append({
                    'name': node.name,
                    'args': args,
                    'docstring': ast.get_docstring(node) or ''
                })
    
    return docs

def generate_markdown(filepath, docs):
    """Generate markdown documentation."""
    filename = os.path.basename(filepath)
    
    output = [f"# {filename}\n"]
    
    if docs['module']:
        output.append(f"{docs['module']}\n")
    
    if docs['functions']:
        output.append("## Functions\n")
        for func in docs['functions']:
            if not func['name'].startswith('_'):
                args_str = ', '.join(func['args'])
                output.append(f"### `{func['name']}({args_str})`\n")
                if func['docstring']:
                    output.append(f"{func['docstring']}\n")
                output.append("")
    
    if docs['classes']:
        output.append("## Classes\n")
        for cls in docs['classes']:
            output.append(f"### `{cls['name']}`\n")
            if cls['docstring']:
                output.append(f"{cls['docstring']}\n")
            if cls['methods']:
                output.append("**Methods:**\n")
                for method in cls['methods']:
                    if not method.startswith('_'):
                        output.append(f"- `{method}()`")
                output.append("")
    
    return '\n'.join(output)

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} /path/to/source")
        sys.exit(1)
    
    source_path = sys.argv[1]
    
    if os.path.isfile(source_path):
        files = [source_path]
    else:
        files = []
        for root, dirs, filenames in os.walk(source_path):
            dirs[:] = [d for d in dirs if d not in ['node_modules', '.git', '__pycache__']]
            for f in filenames:
                if f.endswith('.py'):
                    files.append(os.path.join(root, f))
    
    print("# API Documentation\n")
    print(f"Generated from: {source_path}\n")
    
    for filepath in sorted(files):
        docs = extract_python_docs(filepath)
        if docs['functions'] or docs['classes']:
            print(generate_markdown(filepath, docs))
            print("---\n")

if __name__ == "__main__":
    main()
