"""Compare Worklink operations, including transitively referenced schemas."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OPERATIONS = {
    "/api/info": ["get"], "/api/event": ["get"],
    "/api/session": ["get", "post"], "/api/session/{sessionID}": ["get"],
    "/api/session/{sessionID}/prompt": ["post"],
    "/api/session/{sessionID}/command": ["post"],
    "/api/session/{sessionID}/message": ["get"],
    "/api/session/{sessionID}/inbox": ["get"],
    "/api/session/{sessionID}/inbox/{inboxID}": ["delete"],
    "/api/session/{sessionID}/interrupt": ["post"],
    "/api/session/{sessionID}/environment": ["put"],
    "/api/experimental/session/{sessionID}/log": ["get"],
    "/api/credential": ["get", "post"],
    "/api/credential/{credentialID}": ["delete"],
    "/api/shell": ["get"], "/api/shell/{id}": ["delete"],
}


def contract(document):
    schemas = {}

    def visit(value):
        if isinstance(value, dict):
            if "$ref" in value:
                name = value["$ref"].removeprefix("#/components/schemas/")
                if name not in schemas:
                    schemas[name] = document["components"]["schemas"][name]
                    visit(schemas[name])
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    operations = {f"{method.upper()} {route}": document["paths"][route][method]
                  for route, methods in OPERATIONS.items() for method in methods}
    visit(operations)
    return {"operations": operations, "schemas": schemas}


def differences(before, after, path=""):
    if before == after:
        return []
    if isinstance(before, dict) and isinstance(after, dict):
        return [diff for key in sorted(before.keys() | after.keys()) for diff in (
            differences(before[key], after[key], path + "/" + key)
            if key in before and key in after else [{"path": path + "/" + key,
                                                     "before": before.get(key), "after": after.get(key)}])]
    return [{"path": path, "before": before, "after": after}]


if __name__ == "__main__":
    old = contract(json.loads((ROOT.parent / "opencode_2_2_7/openapi.json").read_text()))
    new = contract(json.loads((ROOT / "openapi.json").read_text()))
    result = {"operations_checked": sorted(new["operations"]),
              "schemas_checked": len(new["schemas"]), "differences": differences(old, new)}
    (ROOT / "contract-diff.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({**result, "differences": [d["path"] for d in result["differences"]]}, indent=2))
