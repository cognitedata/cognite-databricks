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

Shell snippets below use **POSIX/bash** (`rm -f`, forward slashes) so agents and CI runners
on Linux/macOS work without PowerShell.

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

```bash
cd <pygen-spark-root>
rm -f dist/*.whl
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

```bash
cd <cognite-databricks-root>
# edit pyproject.toml dep → >=0.0.0
rm -f dist/*.whl
uv build --wheel
# restore pyproject.toml dep → release pin (e.g. >=0.4.0)
```

Output: `dist/cognite_databricks-0.0.0-py3-none-any.whl`

### 3. Verify wheel METADATA (do not skip)

```bash
python -c "from zipfile import ZipFile; z=ZipFile('<path-to-cognite_databricks-0.0.0.whl>'); metas=[n for n in z.namelist() if n.endswith('METADATA')]; lines=(z.read(metas[0]).decode().splitlines() if metas else []); print('\n'.join(l for l in lines if 'Requires-Dist: cognite-pygen-spark' in l) or 'MISSING: Requires-Dist: cognite-pygen-spark')"
```

Must print: `Requires-Dist: cognite-pygen-spark>=0.0.0`  
If it prints `MISSING: ...` or still says `>=0.4.0` (or another release pin), rebuild — the old METADATA is baked in.

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

## Cogsail validation (standard setup)

Run this on every pre-release wheel pair. The `sailboat` model keeps its own instances in the model space,
so the seed adds dedicated instance spaces where instance space differs from view space.

### 1. Seed and live-test CDF (pygen-spark checkout)

```bash
cd <pygen-spark-root>
uv run python -m tests.test_live.cogsail          # idempotent seed
uv run pytest tests/test_live -m live -v          # filter / aggregate JSON against the seed
```

| Instance space | SmallBoat nodes | Without `description` |
|----------------|-----------------|-----------------------|
| `inst_sailboat_fleet_a` | `seed_small_boat_fleet_a_01`..`_03` | `_03` |
| `inst_sailboat_fleet_b` | `seed_small_boat_fleet_b_01`, `_02` | — |

### 2. Register on Databricks

Generate for `DataModelId(space="sailboat", external_id="sailboat", version="v1")` into
`f0connectortest.sailboat_sailboat_v1`, then register with `if_exists="replace"`. Replacing functions
registered by the previous release also checks the upgrade path when the signature grows.

```python
secret_scope = "cdf_sailboat_sailboat"
generator.register_udtfs(secret_scope=secret_scope, if_exists="replace")
generator.register_views(secret_scope=secret_scope, if_exists="replace")  # must not raise UNRECOGNIZED_PARAMETER_NAME
```

```sql
DESCRIBE FUNCTION f0connectortest.sailboat_sailboat_v1.small_boat_udtf;
-- Input must end with: instance_space, external_id, _exists, _not_exists, _gt, _gte, _lt, _lte,
-- _row_limit, _query_mode, _aggregates, _group_by, base_url
```

Registration backfills a missing `base_url` secret from the TOML-loaded client and never overwrites one:

```python
print([s.key for s in dbutils.secrets.list(secret_scope)])  # must include 'base_url'
print(dbutils.secrets.get(secret_scope, "base_url"))        # redacted in output; cogsail = public URL
```

### 3. Query instance-space pushdown end to end

Reuse the generated view SQL (it passes every argument) and bind the pushdown args:

```python
view_sql = generator.code_generator.generate_views(
    secret_scope=secret_scope, catalog="f0connectortest", schema="sailboat_sailboat_v1"
).view_sqls["SmallBoat"]
udtf_call = view_sql.split(" AS\n", 1)[1]

fleet_a = udtf_call.replace("instance_space => NULL", "instance_space => 'inst_sailboat_fleet_a'")
display(spark.sql(fleet_a))  # expect exactly seed_small_boat_fleet_a_01.._03

count_b = (
    udtf_call.replace("instance_space => NULL", "instance_space => 'inst_sailboat_fleet_b'")
    .replace("_query_mode => NULL", "_query_mode => 'aggregate'")
    .replace("_aggregates => NULL", """_aggregates => '[{"fn": "count", "property": "externalId"}]'""")
)
display(spark.sql(count_b))  # expect count 2 (in the external_id column)
```

Prove the UDTF uses the `base_url` secret at query time (cogsail is a public cluster, so point it at an
unreachable host, expect the query to fail, then restore):

```python
generator.secret_helper.store_secrets(secret_scope, {"base_url": "https://base-url-check.invalid"})
spark.sql(fleet_a).collect()  # expect a connection error mentioning base-url-check.invalid
generator.secret_helper.store_secrets(secret_scope, {"base_url": "https://westeurope-1.cognitedata.com"})
display(spark.sql(fleet_a))   # rows again
```

Also try a `DataModelQueryRewriter` call, which passes only the bound args. Unity Catalog Python
functions cannot declare `DEFAULT`, so record whether this succeeds or fails with a missing-parameter error:

```python
from cognite.databricks import DataModelQueryRewriter

rewritten = DataModelQueryRewriter.rewrite_to_udtf_sql(
    "SELECT * FROM f0connectortest.sailboat_sailboat_v1.SmallBoat WHERE space = 'inst_sailboat_fleet_a' LIMIT 10",
    secret_scope=secret_scope,
)
display(spark.sql(rewritten))
```

## Common failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ResolutionImpossible`: databricks needs `cognite-pygen-spark>=0.4.0` but user has `0.0.0` wheel | Databricks wheel built with release pin | Rebuild databricks with temporary `>=0.0.0`, verify METADATA |
| Old code still runs after `%pip` | Kernel not restarted | Restart Python kernel |
| Only one wheel installed | Partial install / cached PyPI | `--force-reinstall` **both** wheels together |
| Accidental `>=0.0.0` committed | Forgot restore | Revert `pyproject.toml` before push |
| Hive metastore / unqualified UDTF name | Default catalog is `hive_metastore` | Use `catalog.schema.udtf_name` or `USE CATALOG f0connectortest` |
| Workspace write failed on generate | `/Workspace/...` not writable | Use `output_dir="/local_disk0/tmp/pygen_udtf"` (default) |
| `UNRECOGNIZED_PARAMETER_NAME: instance_space` on `register_views()` | UC signature missing pushdown params (cognite-databricks 0.4.0) | Install wheels that include the pushdown parameter registry, re-run `register_udtfs(if_exists="replace")` |
| Fleet query returns `sailboat` nodes or nothing | Seed missing, or view space used as instance space | Re-run the seed; filter on `inst_sailboat_fleet_*`, not `sailboat` |
| View query fails with a missing `base_url` secret | Scope predates runtime `base_url` and views were created without re-registering | Re-run `register_udtfs()` / `register_views()` (they backfill), or `set_cdf_credentials(..., base_url=...)` |
| `403` / connection error on Private Link | `base_url` secret holds the public URL | `set_cdf_credentials(..., base_url="https://p001.plink.<cluster>.cognitedata.com")` |

## Agent checklist

```
- [ ] Built pygen-spark 0.0.0 wheel
- [ ] Temporarily set cognite-databricks dep to cognite-pygen-spark>=0.0.0
- [ ] Built cognite-databricks 0.0.0 wheel
- [ ] Restored release pin in pyproject.toml (not committed as >=0.0.0)
- [ ] Verified wheel METADATA Requires-Dist is >=0.0.0
- [ ] Seeded cogsail instance spaces and ran live tests
- [ ] Gave absolute wheel paths + %pip --force-reinstall snippet + kernel restart
- [ ] Gave the cogsail validation steps (register, DESCRIBE FUNCTION, fleet queries, rewriter call)
```

## Out of scope

- Do **not** bump off `0.0.0` for local testing (that is release/`dev.py bump` only).
- Do **not** publish these `0.0.0` wheels to PyPI.
- Release pin bump after a real pygen-spark release is a separate change (changelog + `>=x.y.z`).
