---
name: publish-marimo-app
description: Turn a marimo notebook into a private, password-protected web app — audit it for WASM compatibility, slim its data, export to WebAssembly, and deploy to S3 behind CloudFront. Use when someone wants to share a notebook with a client or teammate without molab or a running server.
---

# /publish-marimo-app — marimo notebook → deployed app

All logic lives in `vdl_tools/marimo_publish/`. Do not write export, upload, or
CloudFormation logic here — this file coordinates the steps that need judgment.

Python: `~/.pyenv/versions/vdl-tools-312/bin/python`

## 1. Audit

```bash
python -m vdl_tools.marimo_publish audit <notebook.py>
```

Exits non-zero when the notebook cannot work in WASM. Read the output before
touching anything.

## 2. Gate: rewrite hardcoded data paths

A WASM export puts **only the notebook** in Pyodide's virtual filesystem, so
`pd.read_csv("data.csv")` raises `FileNotFoundError` in the browser no matter
where the file sits on disk. For each path the audit reported:

1. Move the file into `public/` next to the notebook.
2. Rewrite the read through `mo.notebook_location()`, which resolves to the
   local directory under `marimo edit` and to the served URL under WASM:

```python
pd.read_csv(str(mo.notebook_location() / "public" / "data.csv"))
```

Prefer one `data_url()` helper cell over repeating the expression. Rename files
to URL-safe names — spaces need percent-encoding and will bite.

**Do not skip this because the notebook runs locally.** Local success proves
nothing about WASM; the filesystems are unrelated.

## 3. Gate: slim the data

The audit prints the `public/` payload. Anything over ~10 MB is worth cutting,
because Pyodide downloads *and parses* it in the browser on every cold load.

Find what the notebook actually reads, rather than guessing:

```python
import csv
src = open("notebook.py").read()
cols = next(csv.reader(open("data.csv")))
used = [c for c in cols if f'"{c}"' in src or f"'{c}'" in src]
```

Then check for **dynamically referenced columns** before dropping anything —
`df[df["metric_column_name"].iloc[0]]` names a column whose value lives in the
data, not the source. Write the pruning as a committed script, not a one-off, so
the build is reproducible.

Confirm the notebook still runs end to end afterwards:

```bash
python <notebook.py>
```

## 4. Credentials

```bash
python -m vdl_tools.marimo_publish credentials <app-name> --quiet
```

Generates a passphrase and stores it at `~/.vdl/marimo-publish/<app>.json`.

**Always pass `--quiet` when you are the one running this.** Without it the
password is echoed to stdout, which in an agent transcript or a CI log burns it
permanently. Tell the user to read the file themselves and put it in 1Password.
Never paste a password into a file in the repo, and never echo one the user gave
you back into the transcript.

## 5. Publish

```bash
python -m vdl_tools.marimo_publish publish <notebook.py> --app <app-name> --apply
```

Export → provision → deploy. The first run creates a CloudFront distribution and
takes 5–15 minutes; later runs are fast. Drop `--apply` to dry-run the upload.

## 6. Verify — do not skip

```bash
curl -sI https://vdl-<app-name>.s3.amazonaws.com/index.html | head -1
```

Must be **403**. That is the proof the bucket is private and the only route in
is CloudFront. Then open the view URL, confirm the browser asks for credentials,
and confirm the app renders after signing in.

## Optional: custom domain

Default is the assigned `dxxxx.cloudfront.net` hostname, which is fine for
internal links. Use a real hostname when the app is going to an external
audience or somewhere people will bookmark it.

A DNS record alone does **not** work — CloudFront rejects any `Host` header it
has not been told to answer to. Start here:

```bash
python -m vdl_tools.marimo_publish domain <app-name> --hostname app.vibrantdatalabs.org
```

That finds a certificate covering the hostname, or requests one and prints the
DNS record needed to validate it. It prints the exact `provision` command to run
next. Add `--provision` to attach it automatically once the certificate is
`ISSUED`, or `--wait-seconds 300` to poll while validation completes.

**VDL already holds an issued wildcard for `*.vibrantdatalabs.org` in
us-east-1**, so any single-label subdomain there needs no new certificate and no
validation record — `domain` reuses it and you go straight to `provision`. A
hostname on another domain, or a deeper name like `a.b.vibrantdatalabs.org`,
needs its own certificate.

Finally, CNAME the hostname to the `CnameTarget` that `provision` prints.

**VDL DNS is Cloudflare, not Route53**, so steps 1 and 3 are manual — no AWS
API can write those records. Set the CNAME to **DNS-only** (grey cloud). A
proxied record puts Cloudflare's CDN in front of CloudFront: two caches to
invalidate, Cloudflare's SNI reaching CloudFront instead of the real hostname,
and the auth header taking an extra hop. It often half-works, which is worse
than failing outright.

## Sharing externally

Edge auth is a single shared credential. There is no per-person access, no
revocation for one recipient, and no record of who viewed what. That is usually
fine for a client engagement, but it means:

- **Rotate when an engagement ends**, or when someone with the password leaves:

```bash
python -m vdl_tools.marimo_publish credentials <app-name> --rotate --quiet
python -m vdl_tools.marimo_publish provision <app-name>
```

  Rotating the credential alone changes nothing — `provision` is what pushes
  the new value into the edge function.
- **Ask what is in `public/` before sharing outward.** Everyone with the
  password can fetch every file there directly. Ship the columns the notebook
  renders, not whatever the pipeline happened to emit.
- Use a **separate app name per audience** rather than one link for everyone,
  so revoking access for one party does not disrupt the others.

## What this does and does not protect

Auth runs in a CloudFront Function on every viewer request, before anything
reaches S3, so the credential check is genuine and the data files cannot be
fetched around it.

An in-notebook password field is **not** equivalent: a WASM export ships its own
source, so any password embedded in the notebook is readable from `index.html`,
and `public/*.csv` stays a plain GET away. If a notebook already has a gate like
that, treat it as cosmetic and rely on the edge auth instead.
