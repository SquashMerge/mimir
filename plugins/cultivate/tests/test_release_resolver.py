from __future__ import annotations

import hashlib
from io import BytesIO
import json
import time
from zipfile import ZipFile

import pytest

from mimir_cultivate import release_resolver
from mimir_cultivate.release_resolver import ReleaseError, fetch_bytes, resolve_release


def wheel(project, version, *, dependency=None, requirements=None, tag="py3-none-manylinux2014_x86_64"):
    name = project.replace("-", "_")
    filename = f"{name}-{version}-{tag}.whl"
    content = f"Name: {project}\nVersion: {version}\n"
    if dependency:
        content += f"Requires-Dist: mimir-agent=={dependency}\n"
    for requirement in requirements or ():
        content += f"Requires-Dist: {requirement}\n"
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr(f"{name}-{version}.dist-info/METADATA", content)
        from mimir.plugin_dockerfile import _CONTEXT_SCRIPTS, _S6_FILES
        resources = (["mimir/docker_context/Dockerfile", *(
            "mimir/docker_context/deploy/s6-overlay/s6-rc.d/" + item for item in _S6_FILES)]
            if project == "mimir-agent" else ["mimir_cultivate/docker/" + item for item in _CONTEXT_SCRIPTS])
        for resource in resources:
            archive.writestr(resource, "fixture resource\n")
    data = output.getvalue()
    url = f"https://files.pythonhosted.org/packages/{filename}"
    return {"filename": filename, "url": url, "packagetype": "bdist_wheel",
            "digests": {"sha256": hashlib.sha256(data).hexdigest()}, "data": data}


def index(agents, cultivates):
    sources = {}
    for project, releases in (("mimir-agent", agents), ("mimir-cultivate", cultivates)):
        listing = {}
        for version, options in releases.items():
            listing[version] = []
            for option in options:
                entry = option.copy()
                sources[entry["url"]] = entry.pop("data")
                listing[version].append(entry)
        sources[f"https://pypi.org/pypi/{project}/json"] = json.dumps({"releases": listing}).encode()

    def fetch(url, limit):
        return sources[url]

    return fetch, sources


def pair(version):
    return wheel("mimir-agent", version), wheel("mimir-cultivate", version, dependency=version)


def test_stable_first_and_verified_download():
    stable = pair("1.0.0")
    rc = pair("2.0.0rc1")
    fetch, _ = index({"1.0.0": [stable[0]], "2.0.0rc1": [rc[0]]},
                     {"1.0.0": [stable[1]], "2.0.0rc1": [rc[1]]})
    selected = resolve_release(fetch=fetch)
    assert selected.version == "1.0.0"
    assert selected.agent.data == stable[0]["data"]
    assert selected.cultivate.sha256 == stable[1]["digests"]["sha256"]
    assert resolve_release("2.0.0rc1", fetch=fetch).version == "2.0.0rc1"


def test_newest_common_modern_manylinux_pair_on_the_pinned_base():
    # The pinned trixie base has glibc 2.41: a 2.37 wheel installs there (it did
    # not on the former bookworm pin), and a 2.42 wheel still does not (#2121).
    old = pair("1.0.0")
    recent = (wheel("mimir-agent", "2.0.0", tag="cp311-cp311-manylinux_2_37_x86_64"),
              wheel("mimir-cultivate", "2.0.0", dependency="2.0.0",
                    tag="cp311-cp311-manylinux_2_37_x86_64"))
    too_new = (wheel("mimir-agent", "3.0.0", tag="cp311-cp311-manylinux_2_42_x86_64"),
               wheel("mimir-cultivate", "3.0.0", dependency="3.0.0",
                     tag="cp311-cp311-manylinux_2_42_x86_64"))
    fetch, _ = index({"1.0.0": [old[0]], "2.0.0": [recent[0]], "3.0.0": [too_new[0]]},
                     {"1.0.0": [old[1]], "2.0.0": [recent[1]], "3.0.0": [too_new[1]]})
    selected = resolve_release(fetch=fetch, architecture="x86_64")
    assert selected.version == "2.0.0"
    assert selected.agent.sha256 == recent[0]["digests"]["sha256"]
    assert selected.cultivate.sha256 == recent[1]["digests"]["sha256"]


def test_arm_target_ignores_newer_x86_only_pair():
    arm = (wheel("mimir-agent", "1.0.0", tag="cp311-cp311-manylinux_2_28_aarch64"),
           wheel("mimir-cultivate", "1.0.0", dependency="1.0.0",
                 tag="cp311-cp311-manylinux_2_28_aarch64"))
    x86 = pair("2.0.0")
    fetch, _ = index({"1.0.0": [arm[0]], "2.0.0": [x86[0]]},
                     {"1.0.0": [arm[1]], "2.0.0": [x86[1]]})
    selected = resolve_release(fetch=fetch, architecture="aarch64")
    assert selected.version == "1.0.0"
    assert selected.agent.sha256 == arm[0]["digests"]["sha256"]
    assert selected.cultivate.sha256 == arm[1]["digests"]["sha256"]


def test_prerelease_fallback_skips_unpaired_and_yanked():
    old = pair("1.0.0rc1")
    newest = pair("1.0.0rc2")
    yanked = pair("2.0.0")
    yanked[1]["yanked"] = True
    fetch, _ = index({"1.0.0rc1": [old[0]], "1.0.0rc2": [newest[0]],
                      "2.0.0": [yanked[0]], "3.0.0": [wheel("mimir-agent", "3.0.0")]},
                     {"1.0.0rc1": [old[1]], "1.0.0rc2": [newest[1]], "2.0.0": [yanked[1]]})
    assert resolve_release(fetch=fetch).version == "1.0.0rc2"
    with pytest.raises(ReleaseError, match="non-yanked"):
        resolve_release("2.0.0", fetch=fetch)


@pytest.mark.parametrize("change,expected", [
    ("hash", "SHA-256 mismatch"), ("metadata", "metadata name or version mismatch"),
    ("dependency", "does not require"), ("filename", "name or version disagrees"),
    ("python", "non-yanked"), ("tags", "non-yanked"),
])
def test_rejects_bad_wheels(change, expected):
    agent, cultivate = pair("1.0.0")
    if change == "hash":
        agent["digests"]["sha256"] = "0" * 64
    elif change == "metadata":
        output = BytesIO()
        with ZipFile(BytesIO(agent["data"])) as original, ZipFile(output, "w") as archive:
            for member in original.namelist():
                archive.writestr(member, "Name: mimir-agent\nVersion: 2.0.0\n"
                                 if member.endswith("/METADATA") else original.read(member))
        agent["data"] = output.getvalue()
        agent["digests"]["sha256"] = hashlib.sha256(agent["data"]).hexdigest()
    elif change == "dependency":
        cultivate["data"] = wheel("mimir-cultivate", "1.0.0", dependency="2.0.0")["data"]
        cultivate["digests"]["sha256"] = hashlib.sha256(cultivate["data"]).hexdigest()
    elif change == "filename":
        agent["filename"] = agent["filename"].replace("1.0.0", "2.0.0")
    elif change == "python":
        agent["requires_python"] = ">=3.12"
    else:
        agent["filename"] = agent["filename"].replace("manylinux2014_x86_64", "win_amd64")
    fetch, _ = index({"1.0.0": [agent]}, {"1.0.0": [cultivate]})
    with pytest.raises(ReleaseError, match=expected):
        resolve_release(fetch=fetch)


@pytest.mark.parametrize('project,missing', [
    ('mimir-agent', 'mimir/docker_context/Dockerfile'),
    ('mimir-cultivate', 'mimir_cultivate/docker/verify_wheels.py'),
])
def test_rejects_release_missing_build_resources(project, missing):
    agent, cultivate = pair('1.0.0')
    target = agent if project == 'mimir-agent' else cultivate
    output = BytesIO()
    with ZipFile(BytesIO(target['data'])) as original, ZipFile(output, 'w') as archive:
        for member in original.namelist():
            if member != missing:
                archive.writestr(member, original.read(member))
    target['data'] = output.getvalue()
    target['digests']['sha256'] = hashlib.sha256(target['data']).hexdigest()
    fetch, _ = index({'1.0.0': [agent]}, {'1.0.0': [cultivate]})
    with pytest.raises(ReleaseError, match='Docker context'):
        resolve_release('1.0.0', fetch=fetch)


def test_no_pair_and_invalid_version():
    agent, cultivate = pair("1.0.0")
    fetch, _ = index({"1.0.0": [agent]}, {"2.0.0": [wheel("mimir-cultivate", "2.0.0", dependency="2.0.0")]})
    with pytest.raises(ReleaseError, match="No common"):
        resolve_release(fetch=fetch)
    with pytest.raises(ReleaseError, match="Invalid exact"):
        resolve_release("latest", fetch=fetch)


def test_invalid_index_and_unreachable():
    def unavailable(url, limit):
        raise ReleaseError("Cannot reach PyPI")

    with pytest.raises(ReleaseError, match="Cannot reach PyPI"):
        resolve_release(fetch=unavailable)
    with pytest.raises(ReleaseError, match="Invalid PyPI JSON"):
        resolve_release(fetch=lambda url, limit: b"not json")
    with pytest.raises(ReleaseError, match="Cannot reach PyPI"):
        resolve_release(fetch=lambda url, limit: (_ for _ in ()).throw(OSError("unavailable")))


def test_rejects_unreachable_wheel_and_oversized_index():
    agent, cultivate = pair("1.0.0")
    fetch, sources = index({"1.0.0": [agent]}, {"1.0.0": [cultivate]})
    del sources[agent["url"]]

    def unavailable(url, limit):
        if url == agent["url"]:
            raise OSError("unavailable")
        return fetch(url, limit)

    with pytest.raises(ReleaseError, match="Cannot download"):
        resolve_release(fetch=unavailable)
    with pytest.raises(ReleaseError, match="exceeds"):
        resolve_release(fetch=lambda url, limit: b" " * (limit + 1))


def test_rejects_bad_index_digest_and_wheel_url():
    agent, cultivate = pair("1.0.0")
    agent["digests"]["sha256"] = "unknown"
    fetch, _ = index({"1.0.0": [agent]}, {"1.0.0": [cultivate]})
    with pytest.raises(ReleaseError, match="URL or SHA-256"):
        resolve_release(fetch=fetch)
    agent["digests"]["sha256"] = "a" * 64
    agent["url"] = "http://files.pythonhosted.org/packages/" + agent["filename"]
    fetch, _ = index({"1.0.0": [agent]}, {"1.0.0": [cultivate]})
    with pytest.raises(ReleaseError, match="URL or SHA-256"):
        resolve_release(fetch=fetch)


@pytest.mark.parametrize("requirements,accepted", [
    (["mimir-agent==2.0; python_version >= '3.12'",
      "mimir-agent==1.0; python_version < '3.12' and sys_platform == 'linux'"], True),
    (["mimir-agent==1.0; sys_platform == 'darwin'",
      "mimir-agent==2.0; sys_platform == 'linux' and platform_machine == 'x86_64'"], False),
])
def test_dependency_markers_use_target_not_host(monkeypatch, requirements, accepted):
    import packaging.markers

    monkeypatch.setattr(packaging.markers, "default_environment", lambda: {
        "python_version": "3.12", "python_full_version": "3.12.0",
        "sys_platform": "darwin", "platform_machine": "arm64", "extra": "",
    })
    agent = wheel("mimir-agent", "1.0")
    cultivate = wheel("mimir-cultivate", "1.0", requirements=requirements)
    fetch, _ = index({"1.0": [agent]}, {"1.0": [cultivate]})
    if accepted:
        assert resolve_release(fetch=fetch).version == "1.0"
    else:
        with pytest.raises(ReleaseError, match="does not require mimir-agent"):
            resolve_release(fetch=fetch)


@pytest.mark.parametrize("resource", ["index", "wheel"])
def test_fetch_has_total_deadline_under_trickling_response(monkeypatch, resource):
    class Trickle:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def geturl(self):
            return "https://files.pythonhosted.org/packages/test.whl"

        def read(self, size):
            time.sleep(0.02)
            return b"x"

    monkeypatch.setattr(release_resolver, "urlopen", lambda *args, **kwargs: Trickle())
    monkeypatch.setattr(release_resolver, "FETCH_DEADLINE", 0.08)
    limit = release_resolver.INDEX_LIMIT if resource == "index" else release_resolver.WHEEL_LIMIT
    start = time.monotonic()
    with pytest.raises(ReleaseError, match="time limit"):
        fetch_bytes("https://files.pythonhosted.org/packages/test.whl", limit)
    assert time.monotonic() - start < 0.5
