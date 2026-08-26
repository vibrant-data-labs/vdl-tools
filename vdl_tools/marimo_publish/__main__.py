"""Publish a marimo notebook as a private, authenticated static web app.

    python -m vdl_tools.marimo_publish audit       notebook.py
    python -m vdl_tools.marimo_publish credentials my-app
    python -m vdl_tools.marimo_publish publish     notebook.py --app my-app

`publish` runs export -> provision -> deploy. The individual steps are exposed
so they can be run separately when iterating.

Infrastructure lives in infra.yaml (CloudFormation): a private S3 bucket, a
CloudFront distribution reaching it via Origin Access Control, and a
viewer-request function doing HTTP basic auth. The bucket is never public.
"""

from __future__ import annotations

import argparse
import ast
import base64
import json
import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path

TEMPLATE = Path(__file__).parent / "infra.yaml"
CRED_DIR = Path.home() / ".vdl" / "marimo-publish"
WORDS = Path("/usr/share/dict/words")

# Readers whose first positional argument is a path. A bare string literal here
# resolves against Pyodide's virtual filesystem in a WASM build -- which holds
# only the notebook -- so it must be rewritten to use mo.notebook_location().
PATH_READERS = {
    "read_csv",
    "read_parquet",
    "read_json",
    "read_excel",
    "read_feather",
    "read_table",
    "read_pickle",
    "scan_csv",
    "scan_parquet",
    "open",
}


def die(msg: str) -> None:
    sys.exit(f"error: {msg}")


def run(cmd: list[str], secrets: tuple[str, ...] = (), **kw) -> subprocess.CompletedProcess:
    """Run a command, echoing it with any secret substrings masked.

    Anything printed here lands in shell history, CI logs and agent transcripts,
    so credentials must never appear verbatim -- base64 is encoding, not
    encryption, and decodes straight back to user:password.
    """
    shown = []
    for part in cmd:
        for secret in secrets:
            if secret and secret in part:
                part = part.replace(secret, "***REDACTED***")
        shown.append(part)
    print("  $ " + " ".join(shown))
    return subprocess.run(cmd, check=True, **kw)


def need(tool: str) -> str:
    """Resolve a CLI, preferring the copy beside the running interpreter.

    On pyenv setups the PATH `aws` is usually a shim that resolves against the
    *active* version, which is often not the env this module was invoked from --
    it then fails with "command not found" despite aws being installed.
    """
    sibling = Path(sys.executable).parent / tool
    if sibling.is_file() and os.access(sibling, os.X_OK):
        return str(sibling)
    path = shutil.which(tool)
    if not path:
        die(f"{tool!r} not found beside {sys.executable} or on PATH")
    return path


# ----------------------------------------------------------------- audit ----


def _literal_path_calls(tree: ast.AST) -> list[tuple[int, str, str]]:
    hits = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        fn = node.func
        name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", None)
        if name not in PATH_READERS:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            hits.append((node.lineno, name, first.value))
    return hits


def cmd_audit(args) -> int:
    nb = Path(args.notebook).resolve()
    if not nb.is_file():
        die(f"{nb} not found")
    src = nb.read_text()
    tree = ast.parse(src)

    hardcoded = _literal_path_calls(tree)
    uses_location = "notebook_location" in src
    public_dir = nb.parent / "public"

    print(f"notebook: {nb}")
    print(f"public/ next to notebook: {'yes' if public_dir.is_dir() else 'NO'}")
    print(f"uses mo.notebook_location(): {'yes' if uses_location else 'NO'}")
    print()

    if hardcoded:
        print(f"{len(hardcoded)} hardcoded data path(s) -- these WILL fail in WASM:")
        for lineno, fn, value in hardcoded:
            print(f"  {nb.name}:{lineno}  {fn}({value!r})")
        print()
        print("  Each must become, with the file moved into public/:")
        print('    pd.read_csv(str(mo.notebook_location() / "public" / "name.csv"))')
    else:
        print("No hardcoded data paths found.")

    if public_dir.is_dir():
        total = sum(f.stat().st_size for f in public_dir.rglob("*") if f.is_file())
        print()
        print(f"public/ payload: {total / 1e6:.1f} MB")
        for f in sorted(public_dir.rglob("*")):
            if f.is_file():
                print(f"  {f.stat().st_size / 1e6:8.2f} MB  {f.relative_to(public_dir)}")
        if total > 10e6:
            print()
            print("  Over 10 MB. Pyodide downloads and parses this in the browser --")
            print("  check whether the notebook reads every column before shipping it.")

    return 1 if (hardcoded and not uses_location) else 0


# ----------------------------------------------------------- credentials ----


def _passphrase(n_words: int = 4) -> str:
    if WORDS.is_file():
        pool = [
            w for w in WORDS.read_text().split() if 4 <= len(w) <= 7 and w.isalpha() and w.islower()
        ]
        if len(pool) >= 1000:
            return "-".join(secrets.choice(pool) for _ in range(n_words))
    return secrets.token_urlsafe(16)


def _cred_path(app: str) -> Path:
    return CRED_DIR / f"{app}.json"


def load_credentials(app: str) -> dict | None:
    p = _cred_path(app)
    return json.loads(p.read_text()) if p.is_file() else None


def cmd_credentials(args) -> int:
    app = args.app
    existing = load_credentials(app)
    if existing and not args.rotate:
        print(f"credentials for {app!r} already exist at {_cred_path(app)}")
        print(f"  username: {existing['username']}")
        print("  password: (stored; pass --rotate to replace)")
        return 0

    creds = {"username": args.username, "password": _passphrase()}
    CRED_DIR.mkdir(parents=True, exist_ok=True)
    p = _cred_path(app)
    p.write_text(json.dumps(creds, indent=2) + "\n")
    p.chmod(0o600)
    print(f"{'rotated' if existing else 'created'} credentials for {app!r}")
    print(f"  stored at: {p} (chmod 600)")
    print(f"  username : {creds['username']}")
    if args.quiet:
        # Never echo the password when something other than a human is reading
        # stdout -- an agent transcript or a CI log burns it permanently.
        print(f"  password : (written to file; read it with: cat {p})")
    else:
        print(f"  password : {creds['password']}")
    print()
    print("Store this in 1Password. Re-run `provision` to push a rotation live.")
    return 0


def auth_base64(app: str) -> str:
    creds = load_credentials(app)
    if not creds:
        die(
            f"no credentials for {app!r} -- run: "
            f"python -m vdl_tools.marimo_publish credentials {app}"
        )
    raw = f"{creds['username']}:{creds['password']}".encode()
    return base64.b64encode(raw).decode()


# ---------------------------------------------------------------- export ----


def cmd_export(args) -> int:
    nb = Path(args.notebook).resolve()
    if not nb.is_file():
        die(f"{nb} not found")
    out = Path(args.out).resolve()
    if out.exists():
        shutil.rmtree(out)
    print(f"exporting {nb.name} -> {out}")
    run(
        [
            sys.executable,
            "-m",
            "marimo",
            "export",
            "html-wasm",
            str(nb),
            "-o",
            str(out),
            "--mode",
            args.mode,
        ],
        cwd=nb.parent,
    )
    if not (out / "index.html").is_file():
        die("export produced no index.html")
    files = sum(1 for f in out.rglob("*") if f.is_file())
    size = sum(f.stat().st_size for f in out.rglob("*") if f.is_file())
    print(f"  {files} files, {size / 1e6:.1f} MB")
    if not (out / "public").is_dir():
        print("  note: no public/ in the export -- the notebook ships no data files")
    return 0


# ------------------------------------------------------------- provision ----


def stack_name(app: str) -> str:
    return f"marimo-{app}"


def stack_output(app: str, key: str) -> str:
    aws = need("aws")
    r = subprocess.run(
        [
            aws,
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            stack_name(app),
            "--query",
            f"Stacks[0].Outputs[?OutputKey=='{key}'].OutputValue",
            "--output",
            "text",
        ],
        capture_output=True,
        text=True,
    )
    value = r.stdout.strip()
    return "" if r.returncode or value in ("", "None") else value


def cmd_provision(args) -> int:
    # Validate before printing or touching AWS, so a bad invocation fails
    # cleanly instead of announcing work it will not do.
    if args.domain and not args.cert_arn:
        die("--domain needs --cert-arn (an ACM certificate issued in us-east-1)")
    if args.cert_arn and not args.domain:
        die("--cert-arn is meaningless without --domain")
    if args.cert_arn and ":us-east-1:" not in args.cert_arn:
        die(
            "the certificate must be in us-east-1 -- CloudFront reads certs "
            f"only from that region, got: {args.cert_arn}"
        )

    app = args.app
    bucket = args.bucket or f"vdl-{app}"
    aws = need("aws")
    print(f"provisioning stack {stack_name(app)!r} (bucket {bucket})")
    print("  CloudFront distributions take 5-15 minutes to propagate.")
    b64 = auth_base64(app)
    # NOTE: the AWS CLI takes stack parameters only via argv, so this value is
    # briefly visible to `ps` on this machine while the deploy runs. It is
    # redacted from everything we print.
    run(
        [
            aws,
            "cloudformation",
            "deploy",
            "--template-file",
            str(TEMPLATE),
            "--stack-name",
            stack_name(app),
            "--parameter-overrides",
            f"AppName={app}",
            f"BucketName={bucket}",
            f"AuthBase64={b64}",
            f"DomainName={args.domain or ''}",
            f"AcmCertificateArn={args.cert_arn or ''}",
            "--no-fail-on-empty-changeset",
        ],
        secrets=(b64,),
    )
    print(f"  view URL: {stack_output(app, 'ViewURL')}")
    if args.domain:
        print(f"  CNAME {args.domain} -> {stack_output(app, 'CnameTarget')}")
        print("  Set that record to DNS-only. A proxied record puts a second CDN")
        print("  in front of this one, with its own cache and TLS termination.")
    return 0


# ---------------------------------------------------------------- deploy ----

IMMUTABLE = "public,max-age=31536000,immutable"


def cmd_deploy(args) -> int:
    app = args.app
    src = Path(args.src).resolve()
    if not (src / "index.html").is_file():
        die(f"{src}/index.html missing -- run export first")
    aws = need("aws")

    dest = args.dest or stack_output(app, "BucketURI")
    if not dest:
        die(f"no bucket for {app!r} -- run provision first, or pass --dest")
    dry = [] if args.apply else ["--dryrun"]
    prune = ["--delete"] if args.prune else []

    print(f"{src} -> {dest}")

    # Content-hashed asset filenames, so they can be cached indefinitely.
    run(
        [
            aws,
            "s3",
            "sync",
            f"{src}/",
            f"{dest}/",
            *dry,
            *prune,
            "--exclude",
            "index.html",
            "--exclude",
            "public/*",
            "--exclude",
            "CLAUDE.md",
            "--exclude",
            ".nojekyll",
            "--cache-control",
            IMMUTABLE,
        ]
    )

    # Belt and braces: some CLI builds guess binary/octet-stream for .wasm, and
    # WebAssembly.instantiateStreaming rejects a non-application/wasm response.
    for wasm in sorted((src / "assets").glob("*.wasm")):
        run(
            [
                aws,
                "s3",
                "cp",
                str(wasm),
                f"{dest}/{wasm.relative_to(src)}",
                *dry,
                "--content-type",
                "application/wasm",
                "--cache-control",
                IMMUTABLE,
            ]
        )

    if (src / "public").is_dir():
        run(
            [
                aws,
                "s3",
                "sync",
                f"{src}/public/",
                f"{dest}/public/",
                *dry,
                "--cache-control",
                "public,max-age=300",
            ]
        )

    # index.html last and uncached, so a partial sync is never the live page.
    run(
        [
            aws,
            "s3",
            "cp",
            str(src / "index.html"),
            f"{dest}/index.html",
            *dry,
            "--content-type",
            "text/html; charset=utf-8",
            "--cache-control",
            "no-cache",
        ]
    )

    if args.apply:
        dist = stack_output(app, "DistributionId")
        if dist:
            run(
                [
                    aws,
                    "cloudfront",
                    "create-invalidation",
                    "--distribution-id",
                    dist,
                    "--paths",
                    "/*",
                    "--query",
                    "Invalidation.Status",
                    "--output",
                    "text",
                ]
            )
        url = stack_output(app, "ViewURL")
        print(f"\ndeployed: {url or dest}")
    else:
        print("\ndry run only -- pass --apply to upload")
    return 0


# --------------------------------------------------------------- publish ----


def cmd_publish(args) -> int:
    out = Path(args.out or f"{Path(args.notebook).stem}_export").resolve()
    cmd_export(argparse.Namespace(notebook=args.notebook, out=out, mode="run"))
    cmd_provision(
        argparse.Namespace(
            app=args.app, bucket=args.bucket, domain=args.domain, cert_arn=args.cert_arn
        )
    )
    return cmd_deploy(
        argparse.Namespace(app=args.app, src=out, dest=None, apply=args.apply, prune=True)
    )


# ------------------------------------------------------------------- cli ----


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m vdl_tools.marimo_publish",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("audit", help="report what blocks this notebook from running in WASM")
    a.add_argument("notebook")
    a.set_defaults(func=cmd_audit)

    c = sub.add_parser("credentials", help="create or rotate basic-auth credentials")
    c.add_argument("app")
    c.add_argument("--username", default="vdl")
    c.add_argument("--rotate", action="store_true")
    c.add_argument(
        "--quiet", action="store_true", help="do not echo the password (use when stdout is logged)"
    )
    c.set_defaults(func=cmd_credentials)

    e = sub.add_parser("export", help="build the WASM export")
    e.add_argument("notebook")
    e.add_argument("--out", required=True)
    e.add_argument("--mode", default="run", choices=["run", "edit"])
    e.set_defaults(func=cmd_export)

    v = sub.add_parser("provision", help="create/update the bucket + distribution")
    v.add_argument("app")
    v.add_argument("--bucket", default=None)
    v.add_argument(
        "--domain", default=None, help="optional custom hostname, e.g. app.vibrantdatalabs.org"
    )
    v.add_argument(
        "--cert-arn", default=None, help="ACM certificate ARN for --domain (must be us-east-1)"
    )
    v.set_defaults(func=cmd_provision)

    d = sub.add_parser("deploy", help="upload an export and invalidate the CDN")
    d.add_argument("app")
    d.add_argument("--src", required=True)
    d.add_argument("--dest", default=None)
    d.add_argument("--apply", action="store_true")
    d.add_argument("--prune", action="store_true")
    d.set_defaults(func=cmd_deploy)

    b = sub.add_parser("publish", help="export + provision + deploy")
    b.add_argument("notebook")
    b.add_argument("--app", required=True)
    b.add_argument("--bucket", default=None)
    b.add_argument("--domain", default=None)
    b.add_argument("--cert-arn", default=None)
    b.add_argument("--out", default=None)
    b.add_argument("--apply", action="store_true")
    b.set_defaults(func=cmd_publish)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
