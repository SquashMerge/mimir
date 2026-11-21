#!/bin/sh
# factory-version: 0.14.1
set -eu
case "$MIMIR_ENABLE_OPENCODE" in 0|1) ;; *) exit 1 ;; esac
case "${MIMIR_WORKLINK_HOST:-opencode}:${MIMIR_ENABLE_CLAUDE_CODE:-0}:$MIMIR_ENABLE_OPENCODE" in
    opencode:0:0|opencode:0:1|claude_code:1:0) ;;
    *) echo "inconsistent Worklink host and coding tool switches" >&2; exit 1 ;;
esac
install -d -o root -g root -m 0755 /opt/cultivate-docker
printf '%s\n' "$MIMIR_ENABLE_OPENCODE" > /opt/cultivate-docker/opencode-enabled
chmod 0644 /opt/cultivate-docker/opencode-enabled
printf '%s\n' "${MIMIR_WORKLINK_HOST:-opencode}" > /opt/cultivate-docker/worklink-host
chmod 0644 /opt/cultivate-docker/worklink-host
apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg git jq procps ripgrep xz-utils \
    lsof file unzip less make netcat-openbsd iproute2 psmisc \
    poppler-utils tesseract-ocr tesseract-ocr-eng libjemalloc2
curl -fsSL "https://deb.nodesource.com/setup_${NODE_VERSION}.x" | bash -
apt-get install -y --no-install-recommends nodejs
if [ "$MIMIR_ENABLE_OPENCODE" = "1" ]; then
    npm install --global --prefix /opt/mimir-opencode \
        @opencode/cli@2.3.0 \
        @openai/codex@0.139.0 \
        "feature-factory@${FACTORY_VERSION}" \
        "opencode2-feature-factory@${FACTORY_VERSION}" \
        @devtheops/opencode-plugin-otel@2.1.0
fi
if [ "${MIMIR_WORKLINK_HOST:-opencode}" = claude_code ]; then
    npm install --global --prefix /opt/mimir-claude \
        @anthropic-ai/claude-code@2.1.326 \
        "claude-code-feature-factory@${FACTORY_VERSION}"
    /opt/mimir-claude/bin/claude --version | grep -F '2.1.326'
    node -e '
      const fs = require("node:fs");
      const root = "/opt/mimir-claude/lib/node_modules/";
      for (const [path, name, version] of [
        ["@anthropic-ai/claude-code", "@anthropic-ai/claude-code", "2.1.326"],
        ["claude-code-feature-factory", "claude-code-feature-factory", "0.14.1"],
        ["claude-code-feature-factory/vendor/feature-factory", "feature-factory", "0.14.1"]
      ]) {
        const pkg = JSON.parse(fs.readFileSync(root + path + "/package.json", "utf8"));
        if (pkg.name !== name || pkg.version !== version) process.exit(1);
      }
      fs.accessSync(root + "claude-code-feature-factory/vendor/feature-factory/bin/factory.js", fs.constants.R_OK);
    '
fi
apt-get clean
rm -rf /var/lib/apt/lists/*
