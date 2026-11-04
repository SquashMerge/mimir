"""Keep injected runtime signatures typed across core and optional plugins."""

from __future__ import annotations

import ast
from pathlib import Path


def _bare_tool_runtime(annotation: ast.expr) -> bool:
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            annotation = ast.parse(annotation.value, mode="eval").body
        except SyntaxError:
            return False
    if isinstance(annotation, ast.Subscript) and isinstance(annotation.value, ast.Name):
        if annotation.value.id == "ToolRuntime":
            return False
    if isinstance(annotation, ast.Name):
        return annotation.id == "ToolRuntime"
    return any(_bare_tool_runtime(child) for child in ast.iter_child_nodes(annotation)
               if isinstance(child, ast.expr))


def test_injected_tool_runtimes_have_bound_context_types() -> None:
    """Catch both postponed function annotations and raw injection bindings."""
    root = Path(__file__).resolve().parents[1]
    sources = (*sorted((root / "mimir").rglob("*.py")),
               *sorted((root / "plugins").glob("*/**/*.py")))
    bare: list[str] = []
    for source in sources:
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for arg in (*node.args.posonlyargs, *node.args.args,
                            *node.args.kwonlyargs):
                    if arg.arg == "runtime" and arg.annotation is not None:
                        if _bare_tool_runtime(arg.annotation):
                            bare.append(f"{source.relative_to(root)}:{arg.lineno}")
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if (isinstance(target, ast.Subscript)
                            and isinstance(target.value, ast.Attribute)
                            and target.value.attr == "__annotations__"
                            and isinstance(target.slice, ast.Constant)
                            and target.slice.value == "runtime"
                            and _bare_tool_runtime(node.value)):
                        bare.append(f"{source.relative_to(root)}:{node.lineno}")
    assert not bare, f"Unbound ToolRuntime context annotations: {', '.join(bare)}"
