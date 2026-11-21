"""Explicit opt-in source mutation experiment for #2189.

Run through heavy_run with ``uv run python <this file>``. Each fresh child
compiles one permissive source variant through an import loader. The compiled
source digest proves the mutation landed; checkout files are never overwritten.
"""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

TEST = "plugins/cultivate/tests/test_package_compare.py"
SOURCES = {
    "mimir.access_control": Path("mimir/access_control.py"),
    "mimir.package_evidence": Path("mimir/package_evidence.py"),
    "mimir_cultivate.package_compare": Path("plugins/cultivate/mimir_cultivate/package_compare.py"),
}
mutants = []


def add(module, label, before, after, selection=""):
    source = SOURCES[module].read_text()
    assert source.count(before) == 1, (label, source.count(before))
    changed = source.replace(before, after)
    record(module, label, changed, selection)


def record(module, label, changed, selection=""):
    assert changed != SOURCES[module].read_text()
    compile(changed, str(SOURCES[module]), "exec")
    mutants.append({"module": module, "label": label, "source": changed,
                    "path": str(SOURCES[module].resolve()), "selection": selection})


def replace_node(module, label, node, replacement, selection=""):
    source = SOURCES[module].read_text()
    lines = source.splitlines(keepends=True)
    start = sum(map(len, lines[:node.lineno - 1])) + node.col_offset
    end = sum(map(len, lines[:node.end_lineno - 1])) + node.end_col_offset
    record(module, label, source[:start] + replacement + source[end:], selection)


module = "mimir_cultivate.package_compare"
for function in ast.parse(SOURCES[module].read_text()).body:
    if not isinstance(function, ast.FunctionDef):
        continue
    for node in ast.walk(function):
        if isinstance(node, ast.If) and node.body and isinstance(node.body[0], ast.Raise):
            if function.name == "_extract" and ast.unparse(node.test) == "source is None":
                # extractfile returns None only for non-files, already excluded.
                continue
            replace_node(module, f"{function.name}: deny line {node.lineno}", node.test, "False")
add(module, "registry integrity default trusted", 'integrity = _verify(data, distribution.get("integrity"))',
    'integrity = distribution.get("integrity")', "test_integrity_failure")
add(module, "disable proxy isolation", "trust_env=False", "trust_env=True", "http_client_safety")
add(module, "enable redirects", "follow_redirects=False", "follow_redirects=True", "http_client_safety")
add(module, "return hostile paths instead of omitting",
    'if not member.name.startswith("package/") or not safe_path(path):', "if False:",
    "changed_and_only_in_one_and_hostile_paths")
add(module, "return file bytes instead of hashes", "inventory[path] = digest.hexdigest()",
    'inventory[path] = (root / str(index)).read_bytes().decode("utf-8", errors="replace")',
    "test_no_package_or_manifest_text")
add(module, "remove bound execution scope", "_scope(runtime, repository, pull_request)", "None",
    "test_direct_tool_requires_bound_scope")

module = "mimir.package_evidence"
tree = ast.parse(SOURCES[module].read_text())
for function in tree.body:
    if not isinstance(function, ast.FunctionDef):
        continue
    for node in ast.walk(function):
        if isinstance(node, ast.If) and node.body and isinstance(node.body[0], ast.Raise):
            replace_node(module, f"{function.name}: deny line {node.lineno}", node.test, "False")
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Constant) and node.value.value is False:
            replace_node(module, f"{function.name}: return False line {node.lineno}", node, "return True")
add(module, "closed schema permits extra content", 'extra="forbid"', 'extra="ignore"', "contract_rejects_file_bytes")
add(module, "schema coerces external types", "strict=True", "strict=False", "contract_fails_closed")
add(module, "path whitelist removed", "PATH.fullmatch(path) is not None", "True", "path_filter")
add(module, "path component confinement removed",
    'all(part not in {"", ".", ".."} for part in path.split("/"))', "True", "path_filter")
add(module, "exact specs become prefix matching", "SPEC.fullmatch(spec)", "SPEC.match(spec)", "only_exact_npm_specs")
add(module, "refusal reason becomes arbitrary text",
    'type(result["reason"]) is str and result["reason"] in REASONS', "True", "contract_fails_closed")
add(module, "refusal may carry content", 'set(result) == {"status", "reason"}', "True", "contract_fails_closed")
file_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "FileEvidence")
guard = next(n for n in ast.walk(file_class) if isinstance(n, ast.If))
replace_node(module, "file evidence consistency removed", guard.test, "False", "contract_fails_closed")

module = "mimir.access_control"
function = next(n for n in ast.parse(SOURCES[module].read_text()).body
                if isinstance(n, ast.FunctionDef) and n.name == "_bound_package_compare_result")
boundary = next(
    node for node in ast.walk(function) if isinstance(node, ast.UnaryOp)
    and isinstance(node.op, ast.Not) and isinstance(node.operand, ast.BoolOp)
)
for index, clause in enumerate(boundary.operand.values):
    replace_node(module, f"bound result clause {index}: {ast.unparse(clause)}", clause, "True",
                 "package_result_binding_fails_closed or package_boundary_clauses")
for node in ast.walk(function):
    if isinstance(node, ast.Return) and isinstance(node.value, ast.Constant) and node.value.value is False:
        replace_node(module, f"bound result deny line {node.lineno}", node, "return True",
                     "package_result_binding_fails_closed")
add(module, "remove content validator",
    'return content_free_result(result, arguments.get("specs"), arguments.get("pairs"))',
    "return True", "package_result_binding_fails_closed")

CHILD = r'''
import hashlib, importlib.abc, importlib.machinery, json, os, sys
payload = json.loads(sys.stdin.read())
original_stdout = os.dup(1)
class Loader(importlib.machinery.SourceFileLoader):
    def get_code(self, fullname):
        code = compile(payload["source"], payload["path"], "exec")
        # pytest's fd capture redirects even sys.__stdout__; retain the original
        # pipe so a compiled-source witness cannot disappear in collection output.
        os.write(original_stdout, ("MUTATION_LANDED " + hashlib.sha256(
            payload["source"].encode()).hexdigest() + "\n").encode())
        return code
class Finder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname != payload["module"]:
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        spec.loader = Loader(fullname, payload["path"])
        return spec
sys.meta_path.insert(0, Finder())
import pytest
args = ["-q", "-n", "0", "-x", "--tb=short", "--basetemp", payload["basetemp"],
        "plugins/cultivate/tests/test_package_compare.py"]
if payload["selection"]:
    args.extend(["-k", payload["selection"]])
raise SystemExit(pytest.main(args))
'''

if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="npm-mutation-control-") as scratch:
        baseline = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-n", "0", TEST, "--tb=short", "--basetemp", scratch],
            capture_output=True, text=True,
        )
    if baseline.returncode:
        print(baseline.stdout, baseline.stderr)
        raise SystemExit("baseline failed")
    print("BASELINE PASS", flush=True)
    records = []
    for payload in mutants:
        with tempfile.TemporaryDirectory(prefix="npm-mutation-") as scratch:
            payload["basetemp"] = scratch
            result = subprocess.run([sys.executable, "-c", CHILD], input=json.dumps(payload),
                                    capture_output=True, text=True)
        output = result.stdout + result.stderr
        digest = hashlib.sha256(payload["source"].encode()).hexdigest()
        landed = "MUTATION_LANDED " + digest in output
        killed = landed and result.returncode == 1 and " failed" in output and "ERROR collecting" not in output
        print(("KILLED " if killed else "SURVIVED ") + payload["label"] + " " + digest[:12], flush=True)
        if not killed:
            print(output, flush=True)
        records.append({"label": payload["label"], "landed": landed, "killed": killed, "sha256": digest})
    print(json.dumps(records, indent=2))
    print(f"{sum(r['killed'] for r in records)}/{len(records)} killed; checkout source untouched")
    raise SystemExit(0 if all(r["killed"] for r in records) else 1)
