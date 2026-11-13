#!/bin/sh
# factory-version: 0.14.0
set -eu
apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg git jq procps ripgrep xz-utils \
    lsof file unzip less make netcat-openbsd iproute2 psmisc \
    poppler-utils tesseract-ocr tesseract-ocr-eng libjemalloc2
curl -fsSL "https://deb.nodesource.com/setup_${NODE_VERSION}.x" | bash -
apt-get install -y --no-install-recommends nodejs
if [ "$MIMIR_ENABLE_OPENCODE" = "1" ]; then
    npm install --global --prefix /opt/mimir-opencode \
        @opencode/cli@2.2.7 \
        @openai/codex@0.139.0 \
        "feature-factory@${FACTORY_VERSION}" \
        "opencode2-feature-factory@${FACTORY_VERSION}" \
        @devtheops/opencode-plugin-otel@2.1.0
fi
apt-get clean
rm -rf /var/lib/apt/lists/*
