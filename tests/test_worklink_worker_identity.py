"""Contracts of the root-image worker, independent of the CI proof script."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"
SCRIPTS = ROOT / "plugins/cultivate/mimir_cultivate/docker"
SERVICE = ROOT / "deploy/s6-overlay/s6-rc.d/worklink-execd"


def test_distinct_controller_and_worker_identities() -> None:
    text = DOCKERFILE.read_text() + (SCRIPTS / "users.sh").read_text()
    assert "groupadd --gid 1001 mimir" in text
    assert "groupadd --gid 1002 worklink" in text
    assert "--uid 1001 --gid mimir --groups worklink" in text
    assert "--uid 1002 --gid worklink" in text
    assert "install -d -o root -g mimir -m 0771 /var/lib/mimir-worklink/checkouts" in text
    assert "install -d -o root -g worklink -m 0710 /var/lib/mimir-worklink/homes" in text


def test_source_worker_keeps_immutable_commit_and_overlays() -> None:
    text = (SCRIPTS / "executor.sh").read_text()
    source = text.split('git check-ref-format "$MIMIR_GIT_REF"', 1)[1]
    sha = 'fetch --no-tags --depth=1 origin "$MIMIR_EXECUTOR_COMMIT"'
    ref = 'fetch --no-tags --depth=1 origin "$MIMIR_GIT_REF"'
    assert source.index(sha) < source.index(ref)
    assert source[source.index(sha) + len(sha):source.index(ref)].strip() == "\\\n    || git -C /opt/mimir-worklink/source"
    assert 'test "$(git -C /opt/mimir-worklink/source rev-parse FETCH_HEAD)" = "$MIMIR_EXECUTOR_COMMIT"' in source
    assert 'printf \'%s\\n\' "$MIMIR_EXECUTOR_COMMIT" > /opt/mimir-worklink/executor-source-commit' in source
    assert 'pip install --no-cache-dir --no-deps /opt/mimir-worklink/source/plugins/cultivate' in source
    assert 'UV_CACHE_DIR=/opt/mimir-worklink/uv-cache uv sync' in source
    assert 'rm -rf /opt/mimir-worklink/source' in source


def test_pypi_worker_and_controller_install_same_archives_and_extras() -> None:
    root = DOCKERFILE.read_text()
    assert 'MIMIR_INSTALL_MODE="$MIMIR_INSTALL_MODE" MIMIR_VERSION="$MIMIR_VERSION"' in root
    assert 'cp /opt/cultivate-docker/wheels/*.whl /opt/mimir-worklink/wheels/' in root
    for name in ("controller", "executor"):
        script = (SCRIPTS / f"{name}.sh").read_text()
        pypi = script.split('if [ "${MIMIR_INSTALL_MODE:-}" = pypi ]; then', 1)[1].split("\nfi", 1)[0]
        assert '"/opt/mimir-worklink/wheels/$MIMIR_AGENT_WHEEL[${MIMIR_EXTRAS}]"' in pypi
        assert '"/opt/mimir-worklink/wheels/$MIMIR_CULTIVATE_WHEEL"' in pypi
        assert '"$MIMIR_VERSION" "$MIMIR_AGENT_WHEEL" "$MIMIR_AGENT_SHA256"' in pypi
        assert '"$MIMIR_CULTIVATE_WHEEL" "$MIMIR_CULTIVATE_SHA256"' in pypi
        assert "git " not in pypi and "--no-deps /opt/mimir-worklink/source" not in pypi
        assert "local-embeddings" not in pypi
        assert "install_identity" in pypi
    assert 'chmod -R go-w /opt/mimir-worklink' in (SCRIPTS / "executor.sh").read_text()


def test_s6_registers_one_root_executor_service() -> None:
    run = (SERVICE / "run").read_text()
    assert (SERVICE / "type").read_text() == "longrun\n"
    assert (SERVICE / "dependencies.d/base").is_file()
    assert (ROOT / "deploy/s6-overlay/s6-rc.d/user/contents.d/worklink-execd").is_file()
    assert "s6-setuidgid" not in run
    assert run.rstrip().endswith("exec /usr/local/libexec/worklink-execd")
