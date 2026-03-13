"""
Config-driven RAP integration.
Optimized: Zero-grid-memory extraction via eccodes RAPPointExtractor.
"""
from __future__ import annotations

import ast
import operator

from .config import get_rap_products
from util.grib_loader import RAPPointExtractor

# Transformation functions
TRANSFORMS = {
    "kelvin_to_celsius": lambda x: x - 273.15,
}

_ALLOWED_BINARY_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}

_ALLOWED_UNARY_OPS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


def _safe_eval_formula(formula: str, variables: dict[str, object]) -> float:
    """Safely evaluate simple arithmetic formulas using AST.

    Supported syntax:
    - Numeric constants
    - Variable names (from ``variables``)
    - Binary ops: +, -, *, /, **
    - Unary ops: +, -
    """

    def _eval(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return _eval(node.body)

        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return float(node.value)

        if isinstance(node, ast.Name):
            value = variables.get(node.id)
            if not isinstance(value, (int, float)):
                raise ValueError(f"Unsupported value for '{node.id}': {value!r}")
            return float(value)

        if isinstance(node, ast.BinOp):
            op_fn = _ALLOWED_BINARY_OPS.get(type(node.op))
            if op_fn is None:
                raise ValueError(f"Unsupported operator: {type(node.op).__name__}")
            return op_fn(_eval(node.left), _eval(node.right))

        if isinstance(node, ast.UnaryOp):
            op_fn = _ALLOWED_UNARY_OPS.get(type(node.op))
            if op_fn is None:
                raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
            return op_fn(_eval(node.operand))

        raise ValueError(f"Unsupported formula node: {type(node).__name__}")

    parsed = ast.parse(formula, mode="eval")
    return _eval(parsed)


def integrate_rap(storm_cells, rap_file_path, io_manager):
    """
    Integrate RAP data into storm cells.
    Uses RAPPointExtractor for efficient point-based data extraction.
    """
    if not storm_cells:
        return storm_cells

    if not rap_file_path:
        io_manager.write_warning("No RAP file path provided")
        return storm_cells

    config = get_rap_products()
    products = config.get("products", [])
    derived = config.get("derived", [])

    # Prepare cell coordinates
    cell_coords = {}
    for cell in storm_cells:
        cid = cell.get("id")
        centroid = cell.get("centroid", [0, 0])
        cell_coords[cid] = (centroid[0], centroid[1])

    # Run batch extraction
    try:
        extractor = RAPPointExtractor(rap_file_path)
        extracted_data = extractor.extract_batch(products, cell_coords)
        io_manager.write_debug(f"Extracted {len(extracted_data)} keys from RAP")
    except Exception as e:
        io_manager.write_error(f"Failed to extract RAP data: {e}")
        return storm_cells

    # Apply extracted data to cells
    for product in products:
        transform_fn = TRANSFORMS.get(product.get("transform"), lambda x: x)
        
        if "levels" in product:
            levels = product["levels"]
            key_template = product["key_template"]
            
            for level in levels:
                key = key_template.format(level=level)
                if key in extracted_data:
                    _apply_to_cells(storm_cells, extracted_data[key], key, transform_fn)
        else:
            key = product["key"]
            if key in extracted_data:
                _apply_to_cells(storm_cells, extracted_data[key], key, transform_fn)
                
    # Calculate derived fields
    for d in derived:
        _calculate_derived(storm_cells, d["formula"], d["key"])

    return storm_cells


def _set_nested(root, key, value):
    """Set value in a nested dictionary using dot notation."""
    parts = key.split(".")
    curr = root
    for part in parts[:-1]:
        if part not in curr:
            curr[part] = {}
        elif not isinstance(curr[part], dict):
            # Overwrite if it exists but isn't a dict (shouldn't happen with clean data)
            curr[part] = {}
        curr = curr[part]
    curr[parts[-1]] = value


def _apply_to_cells(storm_cells, cell_values, key, transform_fn):
    """Apply extracted values to cells."""
    for cell in storm_cells:
        if "properties" not in cell:
            cell["properties"] = {}
            
        cid = cell.get("id")
        val = cell_values.get(cid)
        
        final_val = None
        if val is not None:
            try:
                final_val = round(transform_fn(float(val)), 2)
            except Exception:
                final_val = None
                
        _set_nested(cell["properties"], key, final_val)


def _calculate_derived(storm_cells, formula, key):
    """Calculate derived field from existing properties."""
    for cell in storm_cells:
        props = cell.get("properties", {})
        try:
            props[key] = round(_safe_eval_formula(formula, props), 2)
        except Exception:
            props[key] = None
