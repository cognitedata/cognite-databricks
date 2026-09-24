---
name: whl-local-pre-release-testing
description: >-
  Build and install cognite-pygen-spark + cognite-databricks 0.0.0 wheels for
  Databricks pre-PyPI testing. Use when the user asks for wheel files, local
  pre-release testing, %pip install of .whl paths, or hits ResolutionImpossible
  between cognite-databricks and cognite-pygen-spark==0.0.0.
---

# Wheel-based local pre-PyPI release testing

Workflow for testing **cognite-pygen-spark** and **cognite-databricks** on Databricks
**before** a PyPI release. Versioning is the fragile part — follow it exactly.

This skill lives in `cognite-databricks`. Pygen-spark is a sibling checkout (or path the
user provides).

## Versioning rules (critical)

| Context | Package version in source | `cognite-databricks` dep on pygen-spark |
|---------|---------------------------|----------------------------------------|
| Dev / PR branches | `0.0.0` placeholder (`pyproject.toml` + `_version.py`) | Release pin, e.g. `>=0.4.0` |
| Local wheel testing | Keep `0.0.0` in source | Temporarily `>=0.0.0` **only while building** the databricks wheel |
| Real release | `dev.py bump` replaces `0.0.0` → next semver | Restore release pin (e.g. `>=0.4.0`) |

- Source versions stay at `0.0.0` until release CI runs `dev.py bump`.
- A databricks wheel built with `Requires-Dist: cognite-pygen-spark>=0.4.0` **cannot** install against a `0.0.0` pygen-spark wheel → `ResolutionImpossible`.
- Repo note in `pyproject.toml`: *For local builds, use `>=0.0.0` to allow `0.0.0` wheels. For releases, align with latest pygen-spark.*

**Never commit** the temporary `>=0.0.0` dependency change.

## Build workflow

Agents do this every time the user asks for wheels for local testing.

### 1. Build pygen-spark (unchanged)

```powershell
cd <pygen-spark-root>
Remove-Item dist\*.whl -ErrorAction SilentlyContinue
uv build --wheel
```

Output: `dist/cognite_pygen_spark-0.0.0-py3-none-any.whl`

### 2. Temporarily loosen databricks dep, build, restore

In this repo's `pyproject.toml`:

```toml
# BEFORE build (local testing only):
"cognite-pygen-spark>=0.0.0",

# AFTER build (restore for release / PR):
"cognite-pygen-spark>=0.4.0",  # use whatever the release pin currently is
```

```powershell
cd <cognite-databricks-root>
# edit pyproject.toml dep → >=0.0.0
Remove-Item dist\*.whl -ErrorAction SilentlyContinue
uv build --wheel
# restore pyproject.toml dep → release pin (e.g. >=0.4.0)
```

Output: `dist/cognite_databricks-0.0.0-py3-none-any.whl`

### 3. Verify wheel METADATA (do not skip)

```powershell
python -c "from zipfile import ZipFile; z=ZipFile(r'<path-to-cognite_databricks-0.0.0.whl>'); m=[n for n in z.namelist() if n.endswith('METADATA')][0]; print([l for l in z.read(m).decode().splitlines() if 'Requires-Dist: cognite-pygen-spark' in l][0])"
```

Must print: `Requires-Dist: cognite-pygen-spark>=0.0.0`  
If it still says `>=0.4.0` (or another release pin), rebuild — the old METADATA is baked in.

### 4. Hand paths to the user

Give absolute paths to both wheels. Remind: upload/copy into Databricks Workspace (e.g. `/Workspace/Users/<email>/wheels/`), then install.

## Databricks install

Install **both wheels in one command**, pygen-spark path first. Use `--force-reinstall`.

```python
%pip install --force-reinstall \
  /Workspace/Users/<email>/wheels/cognite_pygen_spark-0.0.0-py3-none-any.whl \
  /Workspace/Users/<email>/wheels/cognite_databricks-0.0.0-py3-none-any.whl
```

Then **restart the Python kernel**.

Docs reference: `docs/session_scoped/installation.md` (Option 2).

## Smoke check after install

```python
import cognite.pygen_spark as ps
import cognite.databricks as db
print(ps.__version__, db.__version__)  # expect 0.0.0 / 0.0.0 for local wheels
from cognite.databricks import generate_udtf_notebook, DataModelQueryRewriter
```

## Common failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ResolutionImpossible`: databricks needs `cognite-pygen-spark>=0.4.0` but user has `0.0.0` wheel | Databricks wheel built with release pin | Rebuild databricks with temporary `>=0.0.0`, verify METADATA |
| Old code still runs after `%pip` | Kernel not restarted | Restart Python kernel |
| Only one wheel installed | Partial install / cached PyPI | `--force-reinstall` **both** wheels together |
| Accidental `>=0.0.0` committed | Forgot restore | Revert `pyproject.toml` before push |
| Hive metastore / unqualified UDTF name | Default catalog is hive_metastore | Use `catalog.schema.udtf_name` or `USE CATALOG f0connectortest` |
| Workspace write failed on generate | `/Workspace/...` not writable | Use `output_dir="/local_disk0/tmp/pygen_udtf"` (default) |

## Agent checklist

```
- [ ] Built pygen-spark 0.0.0 wheel
- [ ] Temporarily set cognite-databricks dep to cognite-pygen-spark>=0.0.0
- [ ] Built cognite-databricks 0.0.0 wheel
- [ ] Restored release pin in pyproject.toml (not committed as >=0.0.0)
- [ ] Verified wheel METADATA Requires-Dist is >=0.0.0
- [ ] Gave absolute wheel paths + %pip --force-reinstall snippet + kernel restart
```

## Out of scope

- Do **not** bump off `0.0.0` for local testing (that is release/`dev.py bump` only).
- Do **not** publish these `0.0.0` wheels to PyPI.
- Release pin bump after a real pygen-spark release is a separate change (changelog + `>=x.y.z`).
