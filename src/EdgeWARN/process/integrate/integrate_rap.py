"""
Config-driven RAP integration.
Optimized: Zero-grid-memory extraction via eccodes RAPPointExtractor.
"""
import ast
import operator

from .config import get_rap_products
from util.grib_loader import RAPPointExtractor

# Transformation functions
TRANSFORMS = {
    "kelvin_to_celsius": lambda x: x - 273.15,
}

_BINARY_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}

_UNARY_OPERATORS = {
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
}


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
                
    # Calculate derived fields (compile formula AST once per field)
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
    try:
        compiled_expression = ast.parse(formula, mode="eval").body
    except Exception:
        for cell in storm_cells:
            props = cell.get("properties", {})
            props[key] = None
        return

    for cell in storm_cells:
        props = cell.get("properties", {})
        try:
            value = _safe_eval_formula(compiled_expression, props)
            props[key] = round(value, 2)
        except Exception:
            props[key] = None


def _safe_eval_formula(expression, variables):
    """Evaluate arithmetic formulas using a restricted AST parser."""
    return _evaluate_node(expression, variables)


def _evaluate_node(node, variables):
    """Recursively evaluate an expression node with strict operator support."""
    if isinstance(node, ast.BinOp):
        operator_fn = _BINARY_OPERATORS.get(type(node.op))
        if operator_fn is None:
            raise ValueError("Unsupported binary operator")

        left = _evaluate_node(node.left, variables)
        right = _evaluate_node(node.right, variables)
        if left is None or right is None:
            raise ValueError("Cannot evaluate formula with missing values")

        return operator_fn(left, right)

    if isinstance(node, ast.UnaryOp):
        operator_fn = _UNARY_OPERATORS.get(type(node.op))
        if operator_fn is None:
            raise ValueError("Unsupported unary operator")

        operand = _evaluate_node(node.operand, variables)
        if operand is None:
            raise ValueError("Cannot evaluate formula with missing values")

        return operator_fn(operand)

    if isinstance(node, ast.Name):
        value = variables.get(node.id)
        if isinstance(value, (int, float)):
            return float(value)
        raise ValueError(f"Invalid variable value for '{node.id}'")

    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return float(node.value)

    raise ValueError("Unsupported expression in derived formula")
