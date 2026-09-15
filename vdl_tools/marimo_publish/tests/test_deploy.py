"""deploy: every object leaves with an explicit Cache-Control, index.html last.

2026-09-15: a report republished to S3 with a new public/report_stats.json at
the same URL rendered with two sections missing. The bucket held the right
file; browsers kept their cached copy because the objects carried no
Cache-Control (S3 sends none by default, so heuristic caching applied).
"""

import argparse
from pathlib import Path

import pytest

from vdl_tools.marimo_publish import __main__ as mp

# Names from a real `marimo export html-wasm` (marimo 0.x, Vite build).
HASHED = [
    "index-BQhdFMY1.js",
    "KaTeX_AMS-Regular-BQhdFMY1.woff2",
    "Inputs-C4Oo7q-H.css",  # hash itself contains '-'
    "_baseFor-DKD1r8uL.js",
    "loro_wasm_bg-WKwciKJa.wasm",
    "Lora-VariableFont_wght-CZceb_kH.woff2",
]
UNHASHED = [
    "pyodide.asm.wasm",
    "favicon.ico",
    "manifest.json",
    "android-chrome-192x192.png",
    "index-abc.js",  # too short to be a hash
    "index-BQhdFMY1",  # no extension
]


def _export(tmp_path: Path, assets=("index-BQhdFMY1.js", "loro_wasm_bg-WKwciKJa.wasm")) -> Path:
    src = tmp_path / "app_export"
    (src / "assets").mkdir(parents=True)
    (src / "public").mkdir()
    (src / "index.html").write_text("<html></html>")
    (src / "public" / "report_stats.json").write_text("{}")
    (src / "favicon.ico").write_bytes(b"")
    (src / "CLAUDE.md").write_text("marimo's agent prompt, not part of the app")
    for name in assets:
        (src / "assets" / name).write_bytes(b"x")
    return src


def _deploy(monkeypatch, src: Path, **overrides) -> list[list[str]]:
    calls: list[list[str]] = []
    monkeypatch.setattr(mp, "need", lambda tool: tool)
    monkeypatch.setattr(mp, "run", lambda cmd, **_: calls.append(list(cmd)))
    monkeypatch.setattr(
        mp, "stack_output", lambda app, key: {"DistributionId": "E123"}.get(key, "")
    )
    args = {
        "app": "demo",
        "src": str(src),
        "dest": "s3://bucket/prefix",
        "apply": False,
        "prune": True,
    }
    args.update(overrides)
    assert mp.cmd_deploy(argparse.Namespace(**args)) == 0
    return calls


def _uploads(calls):
    return [c for c in calls if c[1:3] in (["s3", "cp"], ["s3", "sync"])]


def _flag(cmd, name):
    return cmd[cmd.index(name) + 1]


def _flags(cmd, name):
    return [v for f, v in zip(cmd, cmd[1:], strict=False) if f == name]


def _target(cmd):
    """The S3 destination of an s3 cp/sync command."""
    return next(a for a in cmd if a.startswith("s3://"))


def test_every_upload_carries_an_explicit_cache_control(monkeypatch, tmp_path):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path)))
    assert len(uploads) >= 4  # tree, assets, wasm fix-up, index.html (+ prune)
    for cmd in uploads:
        assert _flags(cmd, "--cache-control") in ([mp.NO_CACHE], [mp.IMMUTABLE]), cmd


def test_data_and_page_are_no_cache_and_public_is_not_excluded(monkeypatch, tmp_path):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path)))
    tree = next(c for c in uploads if "--recursive" in c and "assets/*" in c)
    assert _flag(tree, "--cache-control") == mp.NO_CACHE
    # public/* rides in the tree copy: nothing but the page, the junk and the
    # separately-handled assets/ is excluded from it.
    assert set(_flags(tree, "--exclude")) == {"index.html", "CLAUDE.md", ".nojekyll", "assets/*"}

    index = next(c for c in uploads if _target(c) == "s3://bucket/prefix/index.html")
    assert _flag(index, "--cache-control") == mp.NO_CACHE
    assert _flag(index, "--content-type") == "text/html; charset=utf-8"


def test_content_hashed_assets_stay_immutable(monkeypatch, tmp_path, capsys):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path)))
    assets = next(c for c in uploads if _target(c) == "s3://bucket/prefix/assets/")
    assert _flag(assets, "--cache-control") == mp.IMMUTABLE
    wasm = next(c for c in uploads if _target(c).endswith(".wasm"))
    assert _flag(wasm, "--cache-control") == mp.IMMUTABLE
    assert _flag(wasm, "--content-type") == "application/wasm"
    out = capsys.readouterr().out
    assert f"Cache-Control: {mp.NO_CACHE} on every object; assets/ {mp.IMMUTABLE}" in out


def test_one_unhashed_asset_downgrades_the_folder_to_no_cache(monkeypatch, tmp_path, capsys):
    src = _export(tmp_path, assets=("index-BQhdFMY1.js", "pyodide.asm.wasm"))
    uploads = _uploads(_deploy(monkeypatch, src))
    for cmd in uploads:
        assert _flag(cmd, "--cache-control") == mp.NO_CACHE, cmd
    out = capsys.readouterr().out
    assert "1 file(s) carry no content hash" in out
    assert "pyodide.asm.wasm" in out


def test_uploads_copy_rather_than_sync_so_reruns_restamp_every_object(monkeypatch, tmp_path):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path)))
    # `sync` skips files whose size and mtime match S3, leaving their old
    # (or absent) headers in place. Only the prune pass may sync, and only to
    # delete.
    syncs = [c for c in uploads if c[2] == "sync"]
    assert len(syncs) == 1 and "--delete" in syncs[0] and "--size-only" in syncs[0]
    assert "index.html" in _flags(syncs[0], "--exclude")
    assert all(c[2] == "cp" for c in uploads if c is not syncs[0])


def test_index_html_is_the_last_copy_and_prune_follows_it(monkeypatch, tmp_path):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path)))
    copies = [c for c in uploads if c[2] == "cp"]
    assert _target(copies[-1]) == "s3://bucket/prefix/index.html"
    assert uploads[-1][2] == "sync"  # stale assets go only once the new page is live


def test_no_prune_means_no_sync_at_all(monkeypatch, tmp_path):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path), prune=False))
    assert all(c[2] == "cp" for c in uploads)


def test_dry_run_touches_nothing_and_apply_still_invalidates(monkeypatch, tmp_path):
    src = _export(tmp_path)
    dry = _deploy(monkeypatch, src)
    assert all("--dryrun" in c for c in _uploads(dry))
    assert not any("cloudfront" in c for c in dry)

    live = _deploy(monkeypatch, src, apply=True)
    assert not any("--dryrun" in c for c in _uploads(live))
    inval = live[-1]
    assert inval[1:3] == ["cloudfront", "create-invalidation"]
    assert _flag(inval, "--distribution-id") == "E123" and _flag(inval, "--paths") == "/*"


def test_dest_trailing_slash_does_not_double_up_keys(monkeypatch, tmp_path):
    uploads = _uploads(_deploy(monkeypatch, _export(tmp_path), dest="s3://bucket/prefix/"))
    for cmd in uploads:
        assert "//" not in _target(cmd).removeprefix("s3://"), cmd


@pytest.mark.parametrize("name", HASHED)
def test_hashed_name_pattern_accepts_real_build_output(name):
    assert mp.HASHED_NAME.match(name)


@pytest.mark.parametrize("name", UNHASHED)
def test_hashed_name_pattern_rejects_non_content_addressed_files(name):
    assert not mp.HASHED_NAME.match(name)
