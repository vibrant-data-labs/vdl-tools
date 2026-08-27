# marimo_publish

Publish a marimo notebook as a static WebAssembly app on a **private** S3 bucket
behind CloudFront, with HTTP basic auth enforced at the edge. Lets a notebook go
to a client or teammate without molab and without a server running anywhere.

## Installing the skill

The module works from any directory — `vdl-tools` is installed as a package, so
`python -m vdl_tools.marimo_publish` resolves everywhere.

The **skill** does not. Claude Code discovers skills from `.claude/skills/` in
the repo you are currently working in, so the copy in this repo only fires when
you are working inside `vdl-tools` — which is not where you publish notebooks
from. Copy it to wherever you need it:

From a `vdl-tools` checkout (the skill is not part of the installed Python
package, so copy it from the repo, not from site-packages):

```bash
# For one project repo:
cp -r .claude/skills/publish-marimo-app /path/to/your-project/.claude/skills/
```

```bash
# Or for every repo you work in (per-user, not version controlled):
cp -r .claude/skills/publish-marimo-app ~/.claude/skills/
```

The copy in this repo is the canonical one — change it here, then re-copy.

## Commands

```bash
python -m vdl_tools.marimo_publish audit       notebook.py
python -m vdl_tools.marimo_publish credentials my-app --quiet
python -m vdl_tools.marimo_publish publish     notebook.py --app my-app --apply
```

`publish` is `export` + `provision` + `deploy`; each is also callable on its own
while iterating.

| Command | Does |
|---|---|
| `audit` | Reports what stops a notebook working in WASM. Non-zero exit if it cannot. |
| `credentials` | Creates or rotates basic-auth credentials in `~/.vdl/marimo-publish/`. |
| `export` | Builds the WASM export. |
| `provision` | Creates/updates the bucket, distribution, OAC, edge function, bucket policy. |
| `deploy` | Uploads an export and invalidates the CDN. |
| `publish` | All three. |

Always pass `--quiet` to `credentials` when something other than a human reads
stdout — otherwise the password lands in shell history, CI logs or an agent
transcript.

## Run `audit` first, always

A WASM export puts **only the notebook** into Pyodide's virtual filesystem, so

```python
pd.read_csv("data.csv")          # FileNotFoundError in the browser
```

fails no matter where the file sits on disk. Every read has to resolve through
the served URL:

```python
pd.read_csv(str(mo.notebook_location() / "public" / "data.csv"))
```

This is invisible locally: the notebook runs fine under `marimo edit` and dies
in the browser. `audit` AST-walks for it, and also reports the `public/` payload
size, because Pyodide downloads *and parses* that in the browser on every cold
load. Ship the columns the notebook renders, not whatever the pipeline emitted.

## What provision creates

A private S3 bucket (all public access blocked), a CloudFront distribution
reaching it via Origin Access Control, and a viewer-request function that
enforces basic auth and rewrites directory URIs onto `index.html`. Defined in
`infra.yaml`, deployed as CloudFormation, so re-running reconciles rather than
duplicating.

Free at normal usage: CloudFront's perpetual free tier covers 1 TB egress,
10M requests and 2M function invocations per month, and the S3 storage for a
~30 MB build rounds to a tenth of a cent.

## Security

Auth runs at the edge on every viewer request, before anything reaches S3, so
data files cannot be fetched around it.

A password field inside a notebook is **not** equivalent and should not be
relied on. A WASM export ships its own source, so any embedded value is
recoverable from `index.html`, and `public/*.csv` stays a plain GET away.

Verify after any deploy — this is the check that matters:

```bash
curl -sI https://<bucket>.s3.amazonaws.com/index.html | head -1   # expect 403
curl -sI https://<dist>.cloudfront.net/public/<file>.csv | head -1 # expect 401
```

Basic auth is one shared credential: no per-person access, no selective
revocation, no record of who viewed what. Rotate when an engagement ends, and
use a separate app name per audience.

```bash
python -m vdl_tools.marimo_publish credentials my-app --rotate --quiet
python -m vdl_tools.marimo_publish provision  my-app   # rotation is not live until this runs
```

Known residue: the AWS CLI accepts stack parameters only via argv, so the
encoded credential is briefly visible to `ps` on the machine running a deploy.
It is redacted from everything the tool prints.

## Custom domain (optional)

Defaults to the assigned `*.cloudfront.net` hostname. For a real hostname, note
that a DNS record alone does nothing — CloudFront rejects any `Host` header it
has not been told to answer to.

```bash
python -m vdl_tools.marimo_publish domain my-app --hostname app.vibrantdatalabs.org
```

Finds a certificate covering the hostname or requests one, prints the validation
record if it needs one, and prints the `provision` command to run next.
`--provision` attaches it automatically once `ISSUED`; `--wait-seconds N` polls
while validation completes.

VDL already holds an issued wildcard for `*.vibrantdatalabs.org` in us-east-1,
so single-label subdomains there skip certificate issuance entirely. ACM
wildcards match one label only: `a.b.vibrantdatalabs.org` would need its own.

Then CNAME the hostname to the `CnameTarget` that `provision` prints.

VDL DNS is Cloudflare, so steps 1 and 3 are manual. Set the record to
**DNS-only** — a proxied record puts a second CDN in front of this one, with its
own cache and TLS termination.
