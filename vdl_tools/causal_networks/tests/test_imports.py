"""Every module imports, and py2mappr imports without a [postgres] config section."""
import os
import subprocess
import sys


def test_causal_network_modules_import():
    from vdl_tools.causal_networks import load_data, metrics, ensemble, pipeline, plots, player  # noqa: F401


def test_py2mappr_imports_without_postgres_config(tmp_path):
    (tmp_path / "config.ini").write_text("[general]\n")
    env = {**os.environ, "VDL_GLOBAL_CONFIG_PATH": str(tmp_path / "config.ini")}
    result = subprocess.run([sys.executable, "-c", "import vdl_tools.py2mappr, vdl_tools.causal_networks.player"],
                            cwd=tmp_path, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
