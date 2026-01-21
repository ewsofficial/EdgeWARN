#!/usr/bin/env python3
"""
generate_pytest.py - Generate pytest test stubs for Python functions
Usage: python generate_pytest.py <source_file.py>
"""

import sys
import ast
import os

def extract_functions(filepath):
    """Extract function names and signatures from a Python file."""
    with open(filepath, 'r') as f:
        tree = ast.parse(f.read())
    
    functions = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            args = [arg.arg for arg in node.args.args]
            functions.append({
                'name': node.name,
                'args': args,
                'docstring': ast.get_docstring(node) or ''
            })
    return functions

def generate_test(func):
    """Generate a test stub for a function."""
    name = func['name']
    args = func['args']
    
    test_code = f'''
def test_{name}_success():
    """Test {name} with valid input."""
    # Arrange
    {chr(10).join(f"    {arg} = None  # TODO: set test value" for arg in args if arg != 'self')}
    
    # Act
    result = {name}({", ".join(arg for arg in args if arg != 'self')})
    
    # Assert
    assert result is not None  # TODO: add proper assertion


def test_{name}_edge_case():
    """Test {name} with edge case input."""
    # TODO: implement edge case test
    pass


def test_{name}_error():
    """Test {name} error handling."""
    import pytest
    with pytest.raises(Exception):  # TODO: specify exception type
        {name}(None)
'''
    return test_code

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <source_file.py>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        sys.exit(1)
    
    functions = extract_functions(filepath)
    module_name = os.path.basename(filepath).replace('.py', '')
    
    print(f'"""Tests for {module_name} module."""')
    print(f'\nimport pytest')
    print(f'from {module_name} import *')
    print()
    
    for func in functions:
        if not func['name'].startswith('_'):
            print(generate_test(func))

if __name__ == "__main__":
    main()
