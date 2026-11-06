from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import zipfile

import pytest

from mimir_cultivate import install_identity as identity


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
