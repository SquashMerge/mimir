"""Extract byte-exact, hash-identified native source for compatibility claims."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
MARKERS = {
    "oauth_stale": (b'uZ=(s,g,m=vc(5))=>', 100),
    "oauth_resolve": (b'if(!x||!uZ(P.value,yield*Tt))', 130),
    "permission_last_match": (b'function UN(e,o,...a)', 210),
    "permission_agent_then_session": (b'let x=yield*a.resolve(C??P.agent)', 225),
    "permission_final_deny": (b'let H=u(function*(Y){let C=yield*O(Y.sessionID,Y.agent)', 220),
    "agent_editor_replacement": (b'update:(g,A)=>{let y=T.agents.get(g)??gS.default(g)', 145),
    "question_permission_and_form": (b'execute:(d,y)=>o.assert({action:"question"', 405),
    "question_field_text": (b'function i_(e,T){return{key:`q${T}`', 242),
    "session_parent_lookup": (b'let pe=H.parentID?yield*C.get(H.parentID)', 292),
    "session_permission_inheritance": (b'title:H.title,agent:H.agent,metadata:H.metadata??pe?.metadata', 131),
    "subagent_session_create": (b'de=ee??(yield*a.create({parentID:O.sessionID', 207),
    "command_subagent_session_create": (b'_e=yield*x.create({parentID:he.id', 133),
}


def inspect(binary):
    raw = binary.read_bytes()
    excerpts = {}
    for name, (marker, length) in MARKERS.items():
        assert raw.count(marker) == 1, (name, "artifact-specific marker no longer unique")
        offset = raw.index(marker)
        excerpts[name] = {"offset": offset, "source": raw[offset:offset + length].decode()}
    result = {"package": "@opencode/cli-linux-arm64@2.3.0", "sha256": hashlib.sha256(raw).hexdigest(),
              "excerpts": excerpts}
    (ROOT / "source-inspection.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    inspect(Path(sys.argv[1]))
