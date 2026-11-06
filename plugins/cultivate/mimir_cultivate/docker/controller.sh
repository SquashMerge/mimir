#!/bin/sh
# Install a matching core/Cultivate pair: verified PyPI wheels or one verified
# source commit. Runs as the controller venv owner. PyPI pairs are immutable;
# upgrade them by rerunning setup with an explicit version, not a core update.
set -eu
if [ "${MIMIR_INSTALL_MODE:-}" = pypi ]; then
    python3 /opt/cultivate-docker/verify_wheels.py /opt/mimir-worklink/wheels \
        "$MIMIR_VERSION" "$MIMIR_AGENT_WHEEL" "$MIMIR_AGENT_SHA256" \
        "$MIMIR_CULTIVATE_WHEEL" "$MIMIR_CULTIVATE_SHA256" \
        > "$VIRTUAL_ENV/controller-install-identity.json"
    pip install --no-cache-dir --no-compile \
        "/opt/mimir-worklink/wheels/$MIMIR_AGENT_WHEEL[${MIMIR_EXTRAS}]" \
        "/opt/mimir-worklink/wheels/$MIMIR_CULTIVATE_WHEEL"
    # Root snapshots startup hooks and verifies after all optional installs.
    exit 0
fi
git check-ref-format "$MIMIR_GIT_REF"
work="$(mktemp -d)"
source="$work/mimir"
git init -q "$source"
git -C "$source" remote add origin "$MIMIR_GIT_URL"
git -C "$source" fetch --no-tags --depth=1 origin "$MIMIR_CONTROLLER_COMMIT" \
    || git -C "$source" fetch --no-tags --depth=1 origin "$MIMIR_GIT_REF"
test "$(git -C "$source" rev-parse FETCH_HEAD)" = "$MIMIR_CONTROLLER_COMMIT"
git -C "$source" checkout -q --detach FETCH_HEAD
test "$(git -C "$source" rev-parse HEAD)" = "$MIMIR_CONTROLLER_COMMIT"
test -z "$(git -C "$source" status --porcelain=v1)"
# The React console is a gitignored build artifact; the core wheel ships it only
# when it exists at build time (see [tool.hatch.build] artifacts).
(cd "$source" && npm ci --include=dev --no-audit --no-fund && npm run build)
test -f "$source/mimir/react_app/dist/index.html"
# One resolution for both: the plugin's exact mimir-agent pin must be satisfied
# by this commit's core, or the install fails here instead of at boot.
core="$source"
if [ -n "${MIMIR_EXTRAS:-}" ]; then core="$source[$MIMIR_EXTRAS]"; fi
case ",${MIMIR_EXTRAS:-}," in
    *,local-embeddings,*) pip install --no-cache-dir 'torch==2.15.0+cpu' --extra-index-url https://download.pytorch.org/whl/cpu ;;
esac
pip install --no-cache-dir "$core" "$source/plugins/cultivate"
python - "$source" <<'PY'
import importlib.metadata as metadata
import json
import pathlib
import sys
import urllib.parse

source = pathlib.Path(sys.argv[1]).resolve()
for name, path in (("mimir-agent", source), ("mimir-cultivate", source / "plugins/cultivate")):
    origin = json.loads(metadata.distribution(name).read_text("direct_url.json") or "{}")
    url = urllib.parse.urlparse(origin.get("url", ""))
    if url.scheme != "file" or pathlib.Path(urllib.parse.unquote(url.path)).resolve() != path:
        raise SystemExit(f"{name} is not installed from {path}: {origin}")
PY
printf '%s\n' "$MIMIR_CONTROLLER_COMMIT" > "$VIRTUAL_ENV/controller-source-commit"
rm -rf "$work"
