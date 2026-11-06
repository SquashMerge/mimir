# syntax=docker/dockerfile:1
# Explicit MIMIR_INSTALL_MODE=source retains matching Git commit installs for
# mimirbot; MIMIR_INSTALL_MODE=pypi requires two verified wheel archives and
# refuses Git provenance. Cultivate requires MIMIR_PLUGIN_SELECTION=cultivate
# and CHAINLINK_ARTIFACT_STAGE=chainlink-build. Without the
# selection the image is core-only: no worker venv, no ``worklink`` user, no
# ``worklink-execd`` service. Enable it at runtime with ``MIMIR_PLUGINS=cultivate``
# in the container environment (compose ``environment:``/``env_file``).
#
# ── Auth: pick ONE of these at runtime via -e or compose env: ─────────
#   A. Max plan (subscription, requires the ``claude-code`` extra and
#      an operator-provided Claude Code CLI):
#        Run ``claude setup-token`` ON THE HOST first, mount the
#        credential file into the container at runtime. macOS hosts:
#        keychain isn't portable; copy the token blob via
#        ``security find-generic-password`` and pass through
#        ``CLAUDE_CODE_OAUTH_TOKEN``. Linux hosts: mount
#        ``~/.claude/credentials`` into the container at
#        ``/home/mimir/.claude/``.
#   B. Anthropic API key (default build path — uses the ``anthropic``
#      extra):
#        -e ANTHROPIC_API_KEY=sk-ant-...
#   C. Gateway (LiteLLM, OpenRouter, internal proxy):
#        -e ANTHROPIC_BASE_URL=https://your-gateway/
#        -e ANTHROPIC_AUTH_TOKEN=...
#        -e ANTHROPIC_MODEL=claude-haiku-4-5  (or gateway-equivalent name)
# ─────────────────────────────────────────────────────────────────────

# ECR Public mirrors the same Docker Official Image; switch back with
# --build-arg BASE_IMAGE=python:3.11-slim if needed.
ARG BASE_IMAGE=public.ecr.aws/docker/library/python:3.11-slim
# BuildKit resolves stage dependencies before RUN shell conditions. Keep the
# Rust builder unreachable unless the caller explicitly selects its artifact.
ARG CHAINLINK_ARTIFACT_STAGE=chainlink-none
ARG MIMIR_INSTALL_MODE=invalid
FROM ${BASE_IMAGE} AS provenance-validation

# Every canonical image must identify one remote ref and the exact commit it is
# expected to resolve. Keep this validation in its own early stage so an
# argument-less build fails quickly and with an actionable diagnostic.
ARG MIMIR_GIT_REF=""
ARG MIMIR_CONTROLLER_COMMIT=""
ARG MIMIR_EXECUTOR_COMMIT=""
ARG MIMIR_GIT_URL=""
ARG MIMIR_INSTALL_MODE
ARG MIMIR_VERSION=""
ARG MIMIR_AGENT_WHEEL=""
ARG MIMIR_AGENT_SHA256=""
ARG MIMIR_CULTIVATE_WHEEL=""
ARG MIMIR_CULTIVATE_SHA256=""
RUN set -eu; \
    case "$MIMIR_INSTALL_MODE" in \
      source) if [ -n "$MIMIR_VERSION$MIMIR_AGENT_WHEEL$MIMIR_AGENT_SHA256$MIMIR_CULTIVATE_WHEEL$MIMIR_CULTIVATE_SHA256" ]; then echo >&2 "source mode refuses wheel arguments"; exit 2; fi ;; \
      pypi) if [ -n "$MIMIR_GIT_REF$MIMIR_CONTROLLER_COMMIT$MIMIR_EXECUTOR_COMMIT$MIMIR_GIT_URL" ]; then echo >&2 "pypi mode refuses Mimir source arguments"; exit 2; fi; \
        case "$MIMIR_VERSION" in ''|*[!a-zA-Z0-9.!+_-]*) echo >&2 "MIMIR_VERSION must be exact"; exit 2;; esac; \
        case "$MIMIR_AGENT_WHEEL" in *.whl) ;; *) echo >&2 "MIMIR_AGENT wheel required"; exit 2;; esac; \
        case "$MIMIR_CULTIVATE_WHEEL" in *.whl) ;; *) echo >&2 "MIMIR_CULTIVATE wheel required"; exit 2;; esac; \
        if ! printf '%s\n' "$MIMIR_AGENT_SHA256" | grep -Eq '^[0-9a-f]{64}$'; then echo >&2 "MIMIR_AGENT SHA256 required"; exit 2; fi; \
        if ! printf '%s\n' "$MIMIR_CULTIVATE_SHA256" | grep -Eq '^[0-9a-f]{64}$'; then echo >&2 "MIMIR_CULTIVATE SHA256 required"; exit 2; fi; \
        exit 0 ;; \
      *) echo >&2 "MIMIR_INSTALL_MODE must be source or pypi"; exit 2 ;; \
    esac; \
    if [ -z "$MIMIR_GIT_REF" ]; then echo >&2 "MIMIR_GIT_REF is required"; exit 2; fi; \
    case "$MIMIR_GIT_REF" in refs/*) ;; *) \
        echo >&2 "MIMIR_GIT_REF must be a fully qualified refs/* name"; exit 2;; \
    esac; \
    if ! printf '%s\n' "$MIMIR_CONTROLLER_COMMIT" | grep -Eq '^[0-9a-f]{40}$'; then \
        echo >&2 "MIMIR_CONTROLLER_COMMIT must be a full lowercase Git SHA"; exit 2; \
    fi; \
    if ! printf '%s\n' "$MIMIR_EXECUTOR_COMMIT" | grep -Eq '^[0-9a-f]{40}$'; then \
        echo >&2 "MIMIR_EXECUTOR_COMMIT must be a full lowercase Git SHA"; exit 2; \
    fi; \
    if ! test "$MIMIR_EXECUTOR_COMMIT" = "$MIMIR_CONTROLLER_COMMIT"; then \
        echo >&2 "MIMIR_EXECUTOR_COMMIT must equal MIMIR_CONTROLLER_COMMIT"; exit 2; \
    fi

# Build the Worklink CLI away from the runtime filesystem. The bind mount below
# exposes only the finished binary to the selected image; no Rust or Cargo layer
# is inherited by either runtime variant.
FROM provenance-validation AS chainlink-build
ARG CHAINLINK_RUST_TOOLCHAIN=1.96.0
ARG CHAINLINK_COMMIT=9909d7e0d5c95251f3555e0dd9d71f894cfc4883
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl git build-essential pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:${PATH}"
RUN set -eu; \
    if ! printf '%s\n' "$CHAINLINK_COMMIT" | grep -Eq '^[0-9a-f]{40}$'; then \
        echo >&2 "CHAINLINK_COMMIT must be a full lowercase Git SHA"; exit 2; \
    fi; \
    if ! printf '%s\n' "$CHAINLINK_RUST_TOOLCHAIN" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then \
        echo >&2 "CHAINLINK_RUST_TOOLCHAIN must be a pinned Rust release"; exit 2; \
    fi; \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh; \
    sh /tmp/rustup.sh -y --profile minimal --default-toolchain "$CHAINLINK_RUST_TOOLCHAIN"; \
    rm /tmp/rustup.sh; \
    CARGO_BUILD_JOBS=1 cargo install --locked \
        --git https://github.com/dollspace-gay/chainlink.git \
        --rev "$CHAINLINK_COMMIT" --root /usr/local chainlink-tracker

# A bind-mount source must exist even when core skips installing the artifact.
# The empty placeholder is rejected if Cultivate is selected without its builder.
FROM provenance-validation AS chainlink-none
RUN touch /usr/local/bin/chainlink

FROM ${CHAINLINK_ARTIFACT_STAGE} AS chainlink-artifact

FROM provenance-validation AS base
ENV MIMIR_INSTALL_MODE=${MIMIR_INSTALL_MODE}
# First-party bytecode has no immutable wheel reference: forbid it at runtime
# and pass --no-compile explicitly to every PyPI-mode pip install.
ENV PYTHONDONTWRITEBYTECODE=1
COPY plugins/cultivate/mimir_cultivate/docker/ /opt/cultivate-docker/
ARG MIMIR_INSTALL_MODE
ARG MIMIR_VERSION
ARG MIMIR_AGENT_WHEEL
ARG MIMIR_AGENT_SHA256
ARG MIMIR_CULTIVATE_WHEEL
ARG MIMIR_CULTIVATE_SHA256
RUN set -eu; if [ "$MIMIR_INSTALL_MODE" = pypi ]; then \
        python3 /opt/cultivate-docker/verify_wheels.py /opt/cultivate-docker/wheels \
          "$MIMIR_VERSION" "$MIMIR_AGENT_WHEEL" "$MIMIR_AGENT_SHA256" \
          "$MIMIR_CULTIVATE_WHEEL" "$MIMIR_CULTIVATE_SHA256"; \
        install -d -m 0755 /opt/mimir-worklink/wheels; \
        cp /opt/cultivate-docker/wheels/*.whl /opt/mimir-worklink/wheels/; \
        chmod 0644 /opt/mimir-worklink/wheels/*.whl; \
    fi

# OS deps:
#   - ca-certificates curl gnupg: prereqs for adding NodeSource +
#     fetching uv installer
#   - git: source-control tooling for skills and operator workflows.
#   - jq: JSON/JSONL parsing relied on by pollers, skill bodies, and
#     operational/debugging shell workflows. Kept in parity with the
#     scaffold-generated image (scaffold_docker.py), which already ships
#     jq, so clean rebuilds of this image keep the same capability (#560).
#   - nodejs + npm: Node runtime/tooling for optional coding tools.
#   - poppler-utils, tesseract-ocr, tesseract-ocr-eng: PDF-ingest
#     toolchain used by mimir's reading-queue pipeline. Tesseract's
#     control file declares ``Depends: tesseract-ocr-eng |
#     tesseract-ocr-osd`` (an OR-relation APT can satisfy with osd
#     alone — orientation detection only). Pinning ``eng`` explicitly
#     removes that ambiguity.
#   - libjemalloc2: allocator preloaded below (see the jemalloc block).
ENV NODE_VERSION=22
ENV MIMIR_FACTORY_ENTRYPOINT=/opt/mimir-opencode/lib/node_modules/feature-factory/bin/factory.js
ENV PATH="/opt/mimir-opencode/bin:${PATH}"
ARG MIMIR_ENABLE_OPENCODE=0
ARG FACTORY_VERSION=0.12.1
RUN NODE_VERSION="$NODE_VERSION" MIMIR_ENABLE_OPENCODE="$MIMIR_ENABLE_OPENCODE" FACTORY_VERSION="$FACTORY_VERSION" \
    sh /opt/cultivate-docker/base.sh

# jemalloc: under glibc malloc the long-running agent's RSS crept to ~2 GB, and
# ~500 MB of that was freed-but-retained fragmentation (one malloc_trim(0)
# released 452 MB). With jemalloc preloaded the same agent held ~530-575 MB
# steady over 6h, returning its hourly scheduled-job spikes within 10-20 min
# (muninn, 2026-10-06). The library path is arch-specific (x86_64-linux-gnu vs
# aarch64-linux-gnu), so resolve it at build time behind a fixed symlink. Opt
# out at runtime with an empty LD_PRELOAD (e.g. `-e LD_PRELOAD=`).
RUN set -eu; \
    lib="$(dpkg -L libjemalloc2 | grep '/libjemalloc\.so\.2$' | head -n 1)"; \
    test -n "$lib"; \
    ln -sf "$lib" /usr/local/lib/libjemalloc.so.2
ENV LD_PRELOAD=/usr/local/lib/libjemalloc.so.2

# s6-overlay (PID 1 + process supervisor). Supersedes tini — it does the same
# zombie-reaping + signal-forwarding AND supervises multiple services: here the
# agent plus an in-container liveness watcher (deploy/s6-overlay/,
# docs/watchdog.md). If the agent crashes or the watcher SIGKILLs a wedge, s6
# restarts that service in place — no full-container restart. TARGETARCH is set
# by BuildKit (amd64 / arm64 → s6's x86_64 / aarch64 tarball names).
ARG S6_OVERLAY_VERSION=3.2.0.2
ARG TARGETARCH
RUN set -eu; \
    case "${TARGETARCH:-amd64}" in \
        amd64) S6_ARCH=x86_64 ;; \
        arm64) S6_ARCH=aarch64 ;; \
        *)     S6_ARCH=x86_64 ;; \
    esac; \
    base="https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}"; \
    curl_args="--fail --silent --show-error --location --retry 5 --retry-all-errors --retry-delay 2"; \
    curl ${curl_args} "${base}/s6-overlay-noarch.tar.xz"      -o /tmp/s6-noarch.tar.xz; \
    curl ${curl_args} "${base}/s6-overlay-${S6_ARCH}.tar.xz"  -o /tmp/s6-arch.tar.xz; \
    tar -C / -Jxpf /tmp/s6-noarch.tar.xz; \
    tar -C / -Jxpf /tmp/s6-arch.tar.xz; \
    rm -f /tmp/s6-noarch.tar.xz /tmp/s6-arch.tar.xz

# Non-root controller user. Source/core images retain the user-owned venv
# update flow. Verified PyPI Cultivate pairs must instead be upgraded together
# through setup and rebuilt; in-place core-only updates are refused.
# /home/mimir also holds the Claude Code CLI OAuth credential under .claude/.
#
# MIMIR_PLUGIN_SELECTION is the build-time MIMIR_PLUGINS selection. ``cultivate``
# (or ``all``) selects the first-party provider. MIMIR_PLUGIN_PACKAGES names only pinned third-party
# distributions from an index, so it refuses both first-party packages.
ARG MIMIR_PLUGIN_PACKAGES=""
ARG MIMIR_PLUGIN_SELECTION=""
ARG MIMIR_PLUGIN_ALL_INSTALLED=0
RUN set -eu; \
    selected=0; \
    selection="$(printf '%s' "$MIMIR_PLUGIN_SELECTION" | tr -d ' ')"; \
    case ",${selection}," in *,cultivate,*|,all,) selected=1 ;; esac; \
    old_ifs="$IFS"; IFS=,; \
    for package in $MIMIR_PLUGIN_PACKAGES; do \
        name="$(printf '%s' "${package%%==*}" | tr 'A-Z_.' 'a-z--' | tr -d ' ')"; \
        if [ "$name" = mimir-cultivate ] || [ "$name" = mimir-agent ]; then \
            echo >&2 "mimir-agent and mimir-cultivate are first-party packages; remove it from MIMIR_PLUGIN_PACKAGES and set MIMIR_PLUGIN_SELECTION=cultivate"; \
            exit 2; \
        fi; \
    done; IFS="$old_ifs"; \
    if [ "$MIMIR_INSTALL_MODE" = pypi ] && [ "$selected" != 1 ]; then \
        echo >&2 "pypi wheel-pair mode requires MIMIR_PLUGIN_SELECTION=cultivate"; exit 2; fi; \
    printf '%s\n' "$selected" > /opt/cultivate-docker/selected
# Core binds only the placeholder, so BuildKit never schedules chainlink-build.
# World-executable mode lets both mimir and worklink invoke the selected binary.
RUN --mount=type=bind,from=chainlink-artifact,source=/usr/local/bin/chainlink,target=/tmp/chainlink,ro \
    if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        if ! test -s /tmp/chainlink; then \
            echo >&2 "Cultivate requires --build-arg CHAINLINK_ARTIFACT_STAGE=chainlink-build"; exit 2; \
        fi; \
        install -m 0755 /tmp/chainlink /usr/local/bin/chainlink; \
    fi
# gh api --allow-escape-sequences is required by github-ci-watch for job logs;
# pin 2.103.2, which supports that flag and includes the 2.102.0 security fixes.
# SHA-256 values are from the official v2.103.2 linux amd64/arm64 release
# assets; override the version and both digests together to bump.
ARG GH_VERSION=2.103.2
ARG GH_AMD64_SHA256=62ccf747115ddc61237efffdde66a3c8f70ba249433dc64f70801089b59c59c8
ARG GH_ARM64_SHA256=4c3e99a50bd8b181a3fe9d69b25dd978bac768148d88f01507c6b4d1ef61a443
RUN set -eu; \
    if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        case "$(dpkg --print-architecture)" in \
            amd64) arch=amd64; digest="$GH_AMD64_SHA256" ;; \
            arm64) arch=arm64; digest="$GH_ARM64_SHA256" ;; \
            *) echo >&2 "Unsupported architecture for gh"; exit 2 ;; \
        esac; \
        case "$GH_VERSION" in *[!0-9.]*|.*|*..*|*.) echo >&2 "GH_VERSION must be pinned"; exit 2 ;; esac; \
        test -n "$GH_VERSION"; \
        archive="gh_${GH_VERSION}_linux_${arch}.tar.gz"; \
        curl --fail --silent --show-error --location --retry 5 \
            "https://github.com/cli/cli/releases/download/v${GH_VERSION}/${archive}" -o /tmp/gh.tar.gz; \
        printf '%s  %s\n' "$digest" /tmp/gh.tar.gz | sha256sum -c -; \
        tar -xzf /tmp/gh.tar.gz -C /tmp "gh_${GH_VERSION}_linux_${arch}/bin/gh"; \
        install -m 0755 "/tmp/gh_${GH_VERSION}_linux_${arch}/bin/gh" /usr/local/bin/gh; \
        rm -rf /tmp/gh.tar.gz "/tmp/gh_${GH_VERSION}_linux_${arch}"; \
    fi
# Core-only images create just the controller user; the worklink identity and
# its state directories belong to the selected Cultivate worker.
RUN if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        sh /opt/cultivate-docker/users.sh; \
    else \
        groupadd --gid 1001 mimir \
        && useradd --create-home --uid 1001 --gid mimir --shell /bin/bash mimir \
        && chmod 0700 /home/mimir; \
    fi
USER mimir
# Land ``docker exec -it <ctn> bash`` at a predictable home dir.
# Docker's default of ``/`` is technically fine but operators
# dropping into the container expect to be near the state. Per
# mimir-carreira review note on PR #331.
WORKDIR /home/mimir

# Install the controller into /home/mimir/venv. Source/core deployments retain
# their user-owned pending-update behavior. PyPI Cultivate installs a verified
# pair here and refuses in-place core updates; use setup --mimir-version to
# rebuild both controller and executor together.
ENV VIRTUAL_ENV=/home/mimir/venv
RUN python3 -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Extras to install with the package. Default targets the common
# multi-bridge deployment (Anthropic API, Discord, Slack, MCP).
# Override at build time:
#
#   docker build \
#     --build-arg MIMIR_INSTALL_MODE=source \
#     --build-arg MIMIR_GIT_REF="$MIMIR_GIT_REF" \
#     --build-arg MIMIR_CONTROLLER_COMMIT="$MIMIR_COMMIT" \
#     --build-arg MIMIR_EXECUTOR_COMMIT="$MIMIR_COMMIT" \
#     --build-arg MIMIR_EXTRAS=anthropic,discord .
#
# Available extras (see pyproject.toml):
#   anthropic, claude-code, openai, codex-plus  (model providers)
#   discord, slack                              (bridges)
#   mcp, acp                                    (Model Context Protocol)
#   local-embeddings                            (Voyage 4 nano, CPU)
#
ARG MIMIR_EXTRAS="anthropic,discord,slack,mcp"
RUN set -eu; case ",${MIMIR_EXTRAS}," in *,local-embeddings,*) \
        pip install --no-cache-dir 'torch==2.15.0+cpu' --extra-index-url https://download.pytorch.org/whl/cpu ;; \
    esac
RUN pip install --no-cache-dir --upgrade pip \
    && if [ "$MIMIR_INSTALL_MODE" = source ]; then pip install --no-cache-dir "mimir-agent[${MIMIR_EXTRAS}]"; fi

# Optional pinned third-party provider packages must be supplied at image build
# time. Private wheels require an operator-supplied pip index or build context.
RUN set -eu; old_ifs="$IFS"; IFS=,; \
    for package in $MIMIR_PLUGIN_PACKAGES; do \
        case "$package" in *==*) ;; *) echo >&2 "plugin package must be pinned: $package"; exit 2 ;; esac; \
        if [ "$MIMIR_INSTALL_MODE" = pypi ]; then \
            pip install --no-cache-dir --no-compile "$package" \
                "/opt/mimir-worklink/wheels/$MIMIR_AGENT_WHEEL[${MIMIR_EXTRAS}]" \
                "/opt/mimir-worklink/wheels/$MIMIR_CULTIVATE_WHEEL"; \
        else pip install --no-cache-dir "$package"; fi; \
    done; IFS="$old_ifs"

USER root
ARG MIMIR_GIT_URL
LABEL org.opencontainers.image.revision="${MIMIR_EXECUTOR_COMMIT}"
# The root executor may run unreleased code, but only from an immutable full SHA.
# Fetching the caller-named ref makes GitHub's synthetic refs/pull/*/merge commit
# reachable without broadening the clone or trusting the mutable ref as identity.
# The full executor SHA remains authoritative and must match FETCH_HEAD exactly.
# Fetch the immutable SHA FIRST. Fetching only the caller's ref races every merge: the
# ref moves between build start and fetch, FETCH_HEAD resolves to a newer commit, and the
# assertion below fails on a commit whose own checks were green (run 33010517969 -- main
# went red on 19d7c517 because cab16d11 landed ~1 minute later). The ref remains as a
# fallback for commits a bare-SHA fetch cannot reach, and the SHA assertion below is
# unchanged either way, so provenance is identical: a mismatch still fails the build.
#
# In source mode the worker venv takes its dependency set from the published mimir-agent, then
# overlays BOTH this commit's core and its plugins/cultivate member with --no-deps,
# so every line of Worklink and worker_exec code it runs is from the commit in the
# label, and no wheel's exact ``mimir-agent==`` pin can replace the overlaid core.
# A runtime dependency newer than the published wheel must be installed
# explicitly in executor.sh. Core-only images have no worker venv.
RUN if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        MIMIR_INSTALL_MODE="$MIMIR_INSTALL_MODE" MIMIR_VERSION="$MIMIR_VERSION" \
        MIMIR_AGENT_WHEEL="$MIMIR_AGENT_WHEEL" MIMIR_AGENT_SHA256="$MIMIR_AGENT_SHA256" \
        MIMIR_CULTIVATE_WHEEL="$MIMIR_CULTIVATE_WHEEL" MIMIR_CULTIVATE_SHA256="$MIMIR_CULTIVATE_SHA256" \
        MIMIR_GIT_REF="$MIMIR_GIT_REF" MIMIR_EXECUTOR_COMMIT="$MIMIR_EXECUTOR_COMMIT" \
        MIMIR_GIT_URL="${MIMIR_GIT_URL:-https://github.com/SquashMerge/mimir.git}" MIMIR_EXTRAS="$MIMIR_EXTRAS" sh /opt/cultivate-docker/executor.sh; \
    fi
USER mimir
# The source controller gets core and Cultivate from the same pinned commit.
# PyPI installs the verified wheel pair; upgrade it only by rebuilding both
# runtimes together, never through the core-only pending-update flow.
RUN if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        MIMIR_INSTALL_MODE="$MIMIR_INSTALL_MODE" MIMIR_VERSION="$MIMIR_VERSION" \
        MIMIR_AGENT_WHEEL="$MIMIR_AGENT_WHEEL" MIMIR_AGENT_SHA256="$MIMIR_AGENT_SHA256" \
        MIMIR_CULTIVATE_WHEEL="$MIMIR_CULTIVATE_WHEEL" MIMIR_CULTIVATE_SHA256="$MIMIR_CULTIVATE_SHA256" \
        MIMIR_GIT_REF="$MIMIR_GIT_REF" MIMIR_CONTROLLER_COMMIT="$MIMIR_CONTROLLER_COMMIT" \
        MIMIR_GIT_URL="${MIMIR_GIT_URL:-https://github.com/SquashMerge/mimir.git}" MIMIR_EXTRAS="$MIMIR_EXTRAS" sh /opt/cultivate-docker/controller.sh; \
    fi

# Optional: install the Claude Code model-provider adapter. The CLI remains an
# operator-provided runtime dependency and is not bundled in this image.
ARG MIMIR_ENABLE_CLAUDE_CODE=0
RUN if [ "$MIMIR_ENABLE_CLAUDE_CODE" = "1" ]; then \
        if [ "$MIMIR_INSTALL_MODE" = pypi ]; then \
            pip install --no-cache-dir --no-compile "/opt/mimir-worklink/wheels/$MIMIR_AGENT_WHEEL[claude-code]" \
                "/opt/mimir-worklink/wheels/$MIMIR_CULTIVATE_WHEEL"; \
        else pip install --no-cache-dir "mimir-agent[claude-code]"; fi; \
    fi
USER root
RUN set -eu; if [ "$MIMIR_INSTALL_MODE" = pypi ]; then \
        /home/mimir/venv/bin/python -c 'from mimir_cultivate.install_identity import write_startup_hooks, CONTROLLER_HOOKS, controller_identity; write_startup_hooks(CONTROLLER_HOOKS); controller_identity()'; \
        chown root:root /opt/mimir-worklink/controller-startup-hooks.json; \
        chmod 0444 /opt/mimir-worklink/controller-startup-hooks.json; \
        chown root:root /home/mimir/venv/controller-install-identity.json; \
        chmod 0444 /home/mimir/venv/controller-install-identity.json; \
    fi
USER mimir

# Pre-warm the fastembed cache so the first request doesn't pay the
# ~80MB download. Skipped silently if offline at build time.
RUN python -c "from fastembed import TextEmbedding; TextEmbedding(model_name='BAAI/bge-small-en-v1.5')" || true

# Default agent home — overridden by volume mount typically.
# ``mimir setup`` seeds the directory structure (skills, memory
# scaffolds, scheduler.yaml). Idempotent — re-runs on container
# recreate are no-ops.
ENV MIMIR_HOME=/home/mimir/agent
RUN mkdir -p /home/mimir/agent && mimir setup --home /home/mimir/agent || true
# OpenCode reads this XDG-global config before walking project-local
# opencode.json files, so the registrations apply in outer repos and nested
# Worklink worktrees alike. The bootstrap is a preserving, idempotent merge.
RUN if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        MIMIR_PLUGINS=cultivate MIMIR_ENABLE_OPENCODE="$MIMIR_ENABLE_OPENCODE" \
        sh /opt/cultivate-docker/bootstrap.sh; \
    fi
# Check the installed runtime binaries as the non-root controller user.
RUN if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        chainlink --version; \
    fi
RUN set -eu; \
    if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        export HOME=/home/mimir XDG_CONFIG_HOME=/home/mimir/.config GH_CONFIG_DIR=/home/mimir/.config/gh; \
        gh --version; \
        gh api --help | grep -q -- --allow-escape-sequences; \
    fi

# Update behavior: verified PyPI Cultivate deployments refuse core-only
# in-place updates. Rerun setup with --mimir-version and recreate the pair.
# Source/core-only images retain the following pending-update behavior:
#
#   - Daily PyPI poll runs automatically at 08:00 UTC inside the
#     container — emits ``mimir_update_available`` algedonic when
#     a newer mimir-agent version is on PyPI. Surfaces in the
#     per-turn feedback block + the /ops dashboard.
#   - Operator approves an update → agent calls
#     ``request_mimir_update`` tool → flag written to
#     ``/home/mimir/agent/.mimir/pending-update.flag``.
#   - On next ``docker compose restart``, the pre-flight in
#     ``server.main`` runs ``pip install --upgrade`` against the
#     user-owned venv above, then ``os.execv``'s onto the new code.
#   - The daily PyPI poll cron is hardcoded to ``0 8 * * *`` (08:00
#     UTC). To suppress notifications without code changes, override
#     the ``update-check`` entry in ``<MIMIR_HOME>/scheduler.yaml``
#     (the operator-managed schedule file ``mimir setup`` seeds).
#     The auto-install path stays available regardless — only fires
#     when the flag is present, which only the agent writes after
#     operator approval.
#
# See ``mimir/update_on_start.py`` for the full flow rationale.

# Web UI + /event endpoint.
ENV MIMIR_WEB_PORT=8080
EXPOSE 8080

# Persistent volumes:
#   /home/mimir/agent   — agent home (memory/, state/, logs/, .mimir/saga.db,
#                         .mimir/pending-update.flag when set)
#   /home/mimir/.claude — Claude Code session credential (Max plan path)
#   /home/mimir/.cache  — fastembed and Hugging Face model caches
VOLUME ["/home/mimir/agent", "/home/mimir/.claude", "/home/mimir/.cache"]

# Container liveness probe → the in-process /health endpoint, which returns
# {"ok": true} only while the event loop is responsive — so a *timeout* here
# catches a wedged loop, not just a dead process. /health is auth-exempt, so
# no API key is needed. start-period covers cold boot + fastembed warm-up.
#
# NOTE: `restart: unless-stopped` does NOT act on health status — Docker only
# restarts on process *exit*, never on `unhealthy`. To turn an `unhealthy`
# result into a restart, run an autoheal sidecar (e.g. willfarrell/autoheal)
# or a Swarm / k8s liveness probe. See docs/watchdog.md.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -fsS "http://127.0.0.1:${MIMIR_WEB_PORT:-8080}/health" || exit 1

# s6 service definitions: the agent + the in-container liveness watcher, both
# supervised by s6. COPY/chmod run as root (the prior mimir-user build steps are
# done); /init must start as root to set up /run, then each service drops to the
# `mimir` user via s6-setuidgid (see the run scripts). No CMD — the s6 ``user``
# bundle defines what runs.
USER root
COPY deploy/s6-overlay/s6-rc.d/ /etc/s6-overlay/s6-rc.d/
# worker_exec._handle_launch creates a private HOME under this same root;
# s6-setuidgid alone changes identity but would retain root's inaccessible HOME.
RUN set -eu; \
    if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        home="$(mktemp -d /var/lib/mimir-worklink/homes/gh-build-XXXXXX)"; \
        trap 'rm -rf "$home"' EXIT; \
        chown worklink:worklink "$home"; \
        chmod 0700 "$home"; \
        /command/s6-setuidgid worklink env HOME="$home" XDG_CONFIG_HOME="$home/.config" GH_CONFIG_DIR="$home/.config/gh" gh --version; \
        /command/s6-setuidgid worklink env HOME="$home" XDG_CONFIG_HOME="$home/.config" GH_CONFIG_DIR="$home/.config/gh" gh api --help | grep -q -- --allow-escape-sequences; \
    fi
RUN set -eu; \
    if [ "$(cat /opt/cultivate-docker/selected)" != 1 ]; then \
        rm -rf /etc/s6-overlay/s6-rc.d/worklink-execd \
               /etc/s6-overlay/s6-rc.d/user/contents.d/worklink-execd; \
    fi
# Source builds must use the checkout's assembler: the published core-only
# wheel can predate plugin support. BuildKit leaves this COPY unreachable for
# source-free PyPI contexts, which use the selected wheel's assembler instead.
FROM base AS source-assembly
COPY mimir/ /opt/mimir-image-assembly/mimir/
FROM base AS pypi-assembly
# An omitted mode must reach provenance-validation's actionable diagnostic,
# rather than fail Docker's stage-name parser on "-assembly".
FROM provenance-validation AS invalid-assembly
FROM ${MIMIR_INSTALL_MODE}-assembly AS runtime
# MIMIR_PLUGIN_SELECTION chooses enabled providers at build time; disabled pinned
# distributions add no services unless MIMIR_PLUGIN_ALL_INSTALLED=1 is explicit.
# A selected Cultivate is pinned to the version controller.sh installed.
RUN set -eu; \
    packages="$MIMIR_PLUGIN_PACKAGES"; \
    if [ "$(cat /opt/cultivate-docker/selected)" = 1 ]; then \
        version="$(/home/mimir/venv/bin/python -c 'import importlib.metadata as m; print(m.version("mimir-cultivate"))')"; \
        packages="${packages:+$packages,}mimir-cultivate==$version"; \
    fi; \
    if [ "$MIMIR_INSTALL_MODE" = source ]; then export PYTHONPATH=/opt/mimir-image-assembly; fi; \
    /home/mimir/venv/bin/python -c 'from mimir.plugin_dockerfile import main; main()' \
        --packages "$packages" \
        --enable "$MIMIR_PLUGIN_SELECTION" \
        --all-installed "$MIMIR_PLUGIN_ALL_INSTALLED" \
        --destination /etc/s6-overlay/s6-rc.d \
        --scripts /usr/local/libexec/mimir-plugin-services
RUN if [ -d /etc/s6-overlay/s6-rc.d/worklink-execd ]; then \
        sh /opt/cultivate-docker/service.sh; \
    else \
        chmod +x /etc/s6-overlay/s6-rc.d/mimir/run /etc/s6-overlay/s6-rc.d/watchdog/run; \
    fi
ENTRYPOINT ["/init"]
