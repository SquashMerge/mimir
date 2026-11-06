from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
from types import SimpleNamespace
import zipfile

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from mimir import server as core_server
from mimir_cultivate import install_identity as identity
from mimir_cultivate import server as cultivate_server
from mimir_cultivate.worklink import worker_client


@pytest.fixture
def installed_pair(tmp_path):
    wheels = tmp_path / "wheels"
    site = tmp_path / "site"
    wheels.mkdir()
    site.mkdir()
    pair = {}
    for name in identity.PROJECTS:
        stem = name.replace("-", "_")
        prefix = f"{stem}-1.2.dist-info"
        files = {
            f"{stem}/__init__.py": b"installed package\n",
            f"{prefix}/METADATA": f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.2\n".encode(),
        }
        rows = []
        for path, data in files.items():
            target = site / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            rows.append((path, f"sha256={digest}", str(len(data))))
        record = f"{prefix}/RECORD"
        rows.append((record, "", ""))
        text = io.StringIO()
        csv.writer(text, lineterminator="\n").writerows(rows)
        files[record] = text.getvalue().encode()
        (site / record).write_bytes(files[record])
        filename = f"{stem}-1.2-py3-none-any.whl"
        with zipfile.ZipFile(wheels / filename, "w") as archive:
            for path, data in files.items():
                archive.writestr(path, data)
        pair[name] = {"version": "1.2", "wheel": filename,
                      "sha256": hashlib.sha256((wheels / filename).read_bytes()).hexdigest()}
    marker = tmp_path / "identity.json"
    declared = {"mode": "pypi", "distributions": pair}
    marker.write_text(json.dumps(declared))
    return marker, wheels, site, declared


def test_verified_pair_requires_archives_and_installed_record(installed_pair):
    marker, wheels, site, declared = installed_pair
    assert identity.verify_install_identity(marker, wheels=wheels, site=site) == declared


@pytest.mark.parametrize("mutation", ["marker", "mode", "hash", "version", "wheel", "record", "record_drift", "content", "missing_archive", "archive_drift"])
def test_identity_fails_closed(installed_pair, mutation):
    marker, wheels, site, declared = installed_pair
    agent = declared["distributions"]["mimir-agent"]
    if mutation == "marker":
        marker.unlink()
    elif mutation == "mode":
        declared["mode"] = "source"
    elif mutation == "hash":
        agent["sha256"] = "0" * 64
    elif mutation == "version":
        agent["version"] = "2.0"
    elif mutation == "wheel":
        agent["wheel"] = "mimir_cultivate-1.2-py3-none-any.whl"
    elif mutation == "record":
        (site / "mimir_agent-1.2.dist-info/RECORD").unlink()
    elif mutation == "record_drift":
        (site / "mimir_agent-1.2.dist-info/RECORD").write_text("mimir_agent/__init__.py,,\n")
    elif mutation == "content":
        (site / "mimir_agent/__init__.py").write_text("tampered")
    elif mutation == "missing_archive":
        (wheels / agent["wheel"]).unlink()
    else:
        with (wheels / agent["wheel"]).open("ab") as stream:
            stream.write(b"extra")
    if mutation in {"mode", "hash", "version", "wheel"}:
        marker.write_text(json.dumps(declared))
    with pytest.raises(identity.InstallIdentityError):
        identity.verify_install_identity(marker, wheels=wheels, site=site)


def test_installed_record_rejects_extra_module_but_allows_installer_metadata(installed_pair):
    marker, wheels, site, _ = installed_pair
    record = site / "mimir_agent-1.2.dist-info/RECORD"
    with record.open("a") as stream:
        stream.write("mimir_agent-1.2.dist-info/INSTALLER,,\n")
    assert identity.verify_install_identity(marker, wheels=wheels, site=site)
    with record.open("a") as stream:
        stream.write("mimir_agent/backdoor.py,,\n")
    with pytest.raises(identity.InstallIdentityError, match="unexpected installed RECORD"):
        identity.verify_install_identity(marker, wheels=wheels, site=site)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [
    "marker", "archive_unreadable", "metadata", "metadata_error", "record", "content", "extra_module",
])
async def test_health_rejects_corrupt_installed_evidence(installed_pair, tmp_path, monkeypatch, mutation):
    marker, wheels, site, declared = installed_pair
    controller_marker = tmp_path / identity.CONTROLLER_MARKER
    controller_marker.write_bytes(marker.read_bytes())
    monkeypatch.setattr(cultivate_server.sys, "prefix", str(tmp_path))
    monkeypatch.setattr(
        identity, "controller_identity",
        lambda: identity.verify_install_identity(controller_marker, wheels=wheels, site=site),
    )
    socket = tmp_path / "executor.sock"
    socket.touch()
    monkeypatch.setattr(worker_client, "DEFAULT_EXECUTOR_SOCKET", socket)

    async def executor():
        return identity.verify_install_identity(marker, wheels=wheels, site=site)

    monkeypatch.setattr(worker_client, "verify_executor_identity", executor)
    monkeypatch.setattr(core_server, "_server_hooks", lambda policy, kind: {
        "capture_source": (cultivate_server.capture_controller_source_commit,),
        "health": (cultivate_server.executor_health,),
    }.get(kind, ()))
    app = web.Application()
    app["plugin_manager"] = SimpleNamespace(policy=None, plugins=())
    app["check_worker_executor_health"] = True
    app.on_startup.append(core_server._capture_controller_source_commit)
    app.router.add_get("/health", core_server._handle_health)
    async with TestClient(TestServer(app)) as client:
        response = await client.get("/health")
        assert response.status == 200
        assert (await response.json())["worklink_install_status"] == "matched"
        if mutation == "marker":
            controller_marker.write_text("not json")
        elif mutation == "archive_unreadable":
            archive = wheels / declared["distributions"]["mimir-agent"]["wheel"]
            digest = identity._digest

            def unreadable(path):
                if path == archive:
                    raise PermissionError("archive denied")
                return digest(path)

            monkeypatch.setattr(identity, "_digest", unreadable)
        elif mutation == "metadata":
            (site / "mimir_agent-1.2.dist-info/METADATA").write_bytes(b"\xff")
        elif mutation == "metadata_error":
            def malformed_metadata(*, path):
                raise TypeError("invalid distribution metadata")

            monkeypatch.setattr(identity.metadata, "distributions", malformed_metadata)
        elif mutation == "record":
            (site / "mimir_agent-1.2.dist-info/RECORD").write_text("invalid\n")
        elif mutation == "content":
            (site / "mimir_agent/__init__.py").write_text("changed")
        else:
            with (site / "mimir_agent-1.2.dist-info/RECORD").open("a") as stream:
                stream.write("mimir_agent/backdoor.py,,\n")
        response = await client.get("/health")
        body = await response.json()
        assert response.status == 503
        assert body["ok"] is False
        assert body["worklink_install_status"] == "mismatched"
