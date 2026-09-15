"""audit: filesystem calls on mo.notebook_location() paths fail only in WASM."""

import argparse
import ast
import textwrap
from pathlib import Path

import pytest

from vdl_tools.marimo_publish import __main__ as mp

FIXTURES = Path(__file__).parent / "fixtures"


def _hits(src: str) -> list[tuple[int, str]]:
    return mp._location_fs_calls(ast.parse(textwrap.dedent(src)))


def _audit(path: Path) -> int:
    return mp.cmd_audit(argparse.Namespace(notebook=str(path)))


def test_read_text_on_notebook_location_is_flagged(capsys):
    assert _audit(FIXTURES / "location_read_text.py") == 1
    out = capsys.readouterr().out
    assert '.read_text()  stats = json.loads((_D / "stats.json").read_text())' in out
    assert "urllib.request.urlopen(s).read().decode()" in out


def test_read_csv_str_and_url_branch_are_not_flagged(capsys):
    assert _audit(FIXTURES / "location_read_csv.py") == 0
    assert "notebook_location() paths" not in capsys.readouterr().out


@pytest.mark.parametrize(
    "src, expected",
    [
        # builtin open() and json.load(open(...))
        (
            """
            _p = mo.notebook_location() / "public" / "a.json"
            data = json.load(open(_p))
            """,
            [(3, "open()")],
        ),
        # str() does not make it a local file
        (
            """
            s = str(mo.notebook_location() / "public" / "a.json")
            text = open(s).read()
            """,
            [(3, "open()")],
        ),
        # Path(...) around it, chained directly off the call
        (
            """
            ok = Path(str(mo.notebook_location())).exists()
            names = list((mo.notebook_location() / "public").iterdir())
            csvs = sorted(mo.notebook_location().parent.glob("*.csv"))
            """,
            [(2, ".exists()"), (3, ".iterdir()"), (4, ".glob()")],
        ),
        # globals carry across cells, whatever order the cells are in
        (
            """
            @app.cell
            def _(data_dir):
                raw = (data_dir / "a.bin").read_bytes()
                return (raw,)

            @app.cell
            def _(mo):
                data_dir = mo.notebook_location() / "public"
                return (data_dir,)
            """,
            [(4, ".read_bytes()")],
        ),
        # a helper cell that returns the path
        (
            """
            @app.cell
            def _(mo):
                def data_url(name):
                    return mo.notebook_location() / "public" / name
                return (data_url,)

            @app.cell
            def _(data_url):
                with data_url("a.txt").open() as f:
                    text = f.read()
                return (text,)
            """,
            [(10, ".open()")],
        ),
    ],
)
def test_flagged(src, expected):
    assert _hits(src) == expected


@pytest.mark.parametrize(
    "src",
    [
        # pandas fetches the URL itself
        'df = pd.read_csv(str(mo.notebook_location() / "public" / "a.csv"))',
        # the result of a read is data, not a path
        """
        df = pd.read_csv(str(mo.notebook_location() / "a.csv"))
        ok = df.exists()
        """,
        # _-prefixed names are private to their cell in marimo
        """
        @app.cell
        def _(mo):
            _D = mo.notebook_location() / "public"
            return

        @app.cell
        def _():
            _D = Path("/tmp")
            ok = _D.exists()
            return
        """,
        # a helper's parameter shadows a same-named notebook global
        """
        path = mo.notebook_location() / "public"

        def read_local(path):
            return open(path).read()
        """,
        # ternary that branches on the URL
        """
        s = str(mo.notebook_location() / "a.txt")
        text = urlopen(s).read() if s.startswith("http") else open(s).read()
        """,
        # a local literal path is a different audit rule
        'text = Path("data/a.txt").read_text()',
    ],
)
def test_not_flagged(src):
    assert _hits(src) == []


def test_export_removes_marimo_claude_md(tmp_path, monkeypatch):
    nb = tmp_path / "nb.py"
    nb.write_text("import marimo\napp = marimo.App()\n")
    out = tmp_path / "nb_export"

    def fake_export(cmd, **kw):
        out.mkdir()
        (out / "index.html").write_text("<html></html>")
        (out / "CLAUDE.md").write_text("# Marimo notebook assistant\n")

    monkeypatch.setattr(mp, "run", fake_export)
    mp.cmd_export(argparse.Namespace(notebook=str(nb), out=str(out), mode="run"))
    assert (out / "index.html").is_file()
    assert not (out / "CLAUDE.md").exists()
