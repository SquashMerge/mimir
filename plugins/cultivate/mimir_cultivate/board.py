"""Cultivate-owned board data providers."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from .repository import RepositoryInventory, valid_repository_slug


_cache: dict[Path, tuple[float, dict[str, Any]]] = {}
_lock = asyncio.Lock()
_evidence_name = re.compile(r"^(\d+)-(\d+)\.json$")


def worklink_evidence(home: Path) -> dict[int, dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    directory = home / "state" / "worklink" / "evidence"
    if not directory.is_dir():
        return latest
    for path in directory.glob("*.json"):
        match = _evidence_name.fullmatch(path.name)
        if match is None:
            continue
        issue, attempt = map(int, match.groups())
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(payload, dict) or (issue in latest and latest[issue]["attempt"] > attempt):
            continue
        text = lambda key: payload[key] if isinstance(payload.get(key), str) else ""
        transcript = text("transcript")
        relative = str(path.relative_to(home))
        latest[issue] = {
            "issue": issue, "attempt": attempt, "backend": text("backend"),
            "status": text("status") or "unknown", "branch": text("branch"),
            "started_at": text("started_at"), "finished_at": text("finished_at"),
            "diff_stat": text("diff_stat"),
            "tests": payload.get("tests") if isinstance(payload.get("tests"), dict) else None,
            "pr_url": text("pr_url"), "blocked_reason": text("blocked_reason"),
            "transcript": transcript,
            "transcript_href": f"/api/v1/chainlink-board/artifact?path={transcript}" if transcript else "",
            "evidence_path": relative,
            "evidence_href": f"/api/v1/chainlink-board/artifact?path={relative}",
        }
    return latest


def _empty(error: str | None = None) -> dict[str, Any]:
    return {"available": False, "error": error, "generated_at": datetime.now(timezone.utc).isoformat(),
            "repo": [], "errors": {}, "pull_requests": [], "truncated": False, "total_count": 0}


def _github_repo(remote: str) -> str | None:
    remote = remote.strip()
    scp = re.fullmatch(r"(?:[^@]+@)?github\.com:([^/]+/[^/]+?)(?:\.git)?/?", remote)
    if scp:
        return scp.group(1)
    parsed = urlparse(remote)
    if parsed.hostname != "github.com":
        return None
    parts = parsed.path.strip("/").removesuffix(".git").split("/")
    return "/".join(parts) if len(parts) == 2 and all(parts) else None


def _configured_repos(home: Path) -> list[str]:
    inventory = RepositoryInventory.load(home / "repositories.yaml")
    if inventory.declared:
        return [repo.slug for repo in inventory.repositories]
    return [repo for part in os.environ.get("GITHUB_REPOS", "").split(",")
            if (repo := part.strip()) and valid_repository_slug(repo)
            and all(segment not in {".", ".."} for segment in repo.split("/"))]


def _origin_repo(home: Path) -> tuple[str | None, str | None]:
    try:
        remote = subprocess.run(["git", "remote", "get-url", "origin"], cwd=home,
                                 capture_output=True, text=True, timeout=8)
    except FileNotFoundError:
        return None, "git CLI not on PATH"
    except subprocess.TimeoutExpired:
        return None, "git remote timed out after 8.0s"
    except (OSError, ValueError) as exc:
        return None, str(exc)[:500]
    if remote.returncode:
        return None, remote.stderr.strip()[:500] or f"git remote exit code {remote.returncode}"
    repo = _github_repo(remote.stdout)
    if repo is None:
        return None, "origin remote is not a parseable GitHub repo"
    return repo, None


def _list_prs(home: Path, repo: str) -> tuple[list[dict[str, Any]] | None, str | None]:
    try:
        prs = subprocess.run(["gh", "pr", "list", "-R", repo, "--state", "open",
                               "--limit", "1000", "--json", "number,title,url,author,createdAt,reviewDecision,isDraft"],
                               cwd=home, capture_output=True, text=True, timeout=8)
    except FileNotFoundError:
        return None, "gh CLI not on PATH"
    except subprocess.TimeoutExpired:
        return None, "gh pr list timed out after 8.0s"
    except (OSError, ValueError) as exc:
        return None, str(exc)[:500]
    if prs.returncode:
        return None, prs.stderr.strip()[:500] or f"gh pr list exit code {prs.returncode}"
    try:
        raw = json.loads(prs.stdout)
    except json.JSONDecodeError as exc:
        return None, f"gh output: {exc}"
    if not isinstance(raw, list):
        return None, "gh returned non-list payload"
    rows = []
    for item in raw:
        if not isinstance(item, dict) or type(item.get("number")) is not int:
            continue
        number = item["number"]
        author = item.get("author")
        rows.append({
            "repo": repo,
            "number": number, "title": item.get("title") if isinstance(item.get("title"), str) else f"PR #{number}",
            "url": item.get("url") if isinstance(item.get("url"), str) else "",
            "author": author.get("login", "") if isinstance(author, dict) and isinstance(author.get("login"), str) else "",
            "created_at": item.get("createdAt") if isinstance(item.get("createdAt"), str) else "",
            "review_decision": item.get("reviewDecision") if isinstance(item.get("reviewDecision"), str) else "",
            "is_draft": item.get("isDraft") if isinstance(item.get("isDraft"), bool) else False,
        })
    return rows, None


def _load_prs(home: Path) -> dict[str, Any]:
    errors: dict[str, str] = {}
    try:
        configured = _configured_repos(home)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        configured = []
        errors["repositories.yaml"] = str(exc)[:500]
    origin, origin_error = _origin_repo(home)
    if origin_error:
        errors["origin"] = origin_error
    repos = list(dict.fromkeys(repo.lower() for repo in configured))
    if origin and origin.lower() not in repos:
        repos.append(origin)
    rows: list[dict[str, Any]] = []
    available = False
    for repo in repos:
        listed, error = _list_prs(home, repo)
        if error is not None:
            errors[repo] = error
        else:
            available = True
            rows.extend(listed or [])
    return {"available": available, "error": None if available else next(iter(errors.values()), "No repositories configured"),
            "generated_at": datetime.now(timezone.utc).isoformat(), "repo": repos, "errors": errors,
            "pull_requests": rows[:100], "truncated": len(rows) > 100, "total_count": len(rows)}


async def build_pr_board_payload(home: Path | None) -> dict[str, Any]:
    if home is None:
        return _empty("home path not configured")
    key = home.resolve()
    async with _lock:
        cached = _cache.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
    payload = await asyncio.to_thread(_load_prs, key)
    async with _lock:
        _cache[key] = (time.monotonic() + 90, payload)
    return payload
