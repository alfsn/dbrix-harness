# dbrix-harness — Full Specification (v0 scope)

> Derived from `v0_spec.md`. §14 records every decision that changes or clarifies it. §13 lists what was deliberately deferred.

## 0. Purpose

A **LangChain v1 / Deep Agents middleware** that gives AI agents a safe, cheap, offline stand-in for a Databricks workspace.

The agent writes Spark SQL or PySpark against production table names. For each tool call, the middleware:

1. **checks** the code against a security policy and finds out whether it writes data,
2. **pauses for human approval** if the code writes data (READ_WRITE only),
3. **runs** it on local Spark inside an isolated container, where table references are **rewritten** at runtime to point at a small, FK-consistent sandbox, and
4. **returns** a compact result, or a structured error the agent can act on.

The agent's code needs no changes. It believes it is talking to `prod_catalog` (§6.5).

### v0 scope at a glance

| In v0 | Deferred (see §13) |
|---|---|
| One engine: **local PySpark + delta-spark** (closest to Databricks) | DuckDB engine |
| One front door: **middleware** (LangChain v1 `AgentMiddleware`) plus the `create_sandboxed_deep_agent` factory | MCP server, CLI, Claude Agent SDK adapter, generic decorator |
| READ_ONLY and **READ_WRITE with human approval** | — |
| Docker isolation, adversarial threat model | gVisor/nsjail |
| Bronze / Silver / Gold, with flat key columns in Bronze | Messy nested-JSON Bronze |

### Threat model

The agent is **adversarial, or may be prompt-injected**. It may try to get around checks with `exec`, `getattr`, string building, path tricks, or Spark internals. As a result:

- The **security boundary is a per-session Docker container**. `prod_catalog` is never mounted, the network is off, and the root filesystem is read-only (§7).
- The **policy layer (§5) is not the boundary.** It has two jobs: find writes (so the mode and approval rules can apply), and block common misuse early with a clear, actionable error. It does not try to be complete.
- Running agent code through **our two tools is the only path**. All other code-execution tools in the harness are removed (§8.4).

### Glossary

| Term | Meaning |
|---|---|
| prod catalog | The immutable synthetic "production" Delta tables in `prod_catalog/` |
| sandbox | A downsampled, FK-consistent copy of the prod catalog for one `(agent_id, session_id)` |
| manifest | YAML file that declares tables, columns, PKs, FKs, layers, and Gold transforms |
| anchor | The table and key whose sampled values define the sandbox slice (default `silver.brands.brand_id`) |
| operating mode | `READ_ONLY` or `READ_WRITE`. Set by the host **per tool call**, never by the agent |
| executor | The per-session Docker container that runs agent code |
| runtime wrapper | Code inside the executor that wraps `spark.sql`, `spark.table`, and table writes. It runs the policy check and the rewrite on every SQL string and table name at runtime |
| metastore | Spark's built-in Hive metastore, stored in an embedded Derby database inside each catalog dir. It remembers which tables exist |
| jobs container | A short-lived container that runs **trusted** data jobs (generate, sample) |

---

## 1. Architecture

```
┌──────────────────────── host process (trusted) ─────────────────────────┐
│  create_sandboxed_deep_agent(...)                                       │
│   ├─ tools: run_sql, run_pyspark          (fail-closed stubs, §8.1)     │
│   ├─ DbrixSandboxMiddleware               (wrap_tool_call, §8.2)        │
│   │    policy.check ─▶ [interrupt if write] ─▶ executor client          │
│   ├─ CompositeBackend: /catalog (ro), /results (ro), / (agent scratch)  │
│   ├─ memory: AGENTS.md · skills: databricks-analyst                     │
│   └─ subagents: explorer (same middleware, enforced)                    │
└───────────────┬─────────────────────────────────────────┬───────────────┘
                │ NDJSON over attached stdin/stdout       │ one-off run
                ▼                                         ▼
┌──── executor container (per session, untrusted) ──┐  ┌── jobs container (trusted) ──┐
│ long-lived SparkSession + Derby metastore         │  │ prod_catalog ro · sandbox rw │
│ runtime wrapper: policy.check ─▶ rewrite ─▶ run   │  │ generate / sample / gold     │
│ /sandbox rw (own only)                            │  └──────────────────────────────┘
│ --network none · read-only rootfs · non-root      │
└───────────────────────────────────────────────────┘
```

Both containers use one image, `dbrix-spark`, in two roles. The host needs no JVM.

### Request flow (one tool call)

1. The agent calls `run_sql(query=…)` or `run_pyspark(code=…)`.
2. `DbrixSandboxMiddleware.wrap_tool_call` intercepts it. The real tool body never runs.
3. `SandboxManager.get_or_create(agent_id, session_id)`: on first use, run the sampler (jobs container) and start the executor (Phases 2 and 5). Safe to call twice (§9.5).
4. `policy.check(code, language)` returns a `PolicyReport` (reads, writes, dynamic SQL) or raises `SandboxSecurityException` (Phase 3). It does not know the mode.
5. The middleware applies the mode for this call:
   - READ_ONLY and the report has writes → error back to the agent.
   - READ_WRITE and the report has writes (or dynamic SQL that may write) → **LangGraph interrupt** for human approval (§8.3).
6. The executor client sends `{run_id, language, code, allow_writes}` to the container. `allow_writes` is true only in READ_WRITE after approval.
7. In the container, the runtime wrapper checks and rewrites every SQL string and table name as it is used (§6.1), runs the code, and writes the full result to `/results/<run_id>.csv`.
8. The lifecycle layer appends to `runs.jsonl` and updates `_sandbox_meta.json`.
9. The middleware returns a `ToolMessage` with the preview, the path to the full result, and any error. All names are shown as prod names (§6.5).

---

## 2. Repository layout

```
dbrix-harness/
├── pyproject.toml                 # uv-managed, Python 3.12
├── dbrix.toml.example
├── manifests/influencer_marketing.yaml
├── docker/spark.Dockerfile        # dbrix-spark image (executor + jobs roles)
├── src/dbrix/
│   ├── config.py      # every default and constant lives here, nowhere else
│   ├── manifest/      # pydantic models, loader, validation
│   ├── generator/     # Phase 1 (runs in jobs container)
│   ├── sampler/       # Phase 2 (runs in jobs container)
│   ├── policy/        # Phase 3: sql.py, python.py, report.py, exceptions.py, spark_functions.txt
│   ├── rewrite/       # Phase 4: name rewrite + reverse mapping (used in containers)
│   ├── executor/      # client.py (host), runtime/ (in container)
│   ├── lifecycle/     # Phase 5: manager.py, meta.py, runlog.py, gc.py, locks.py
│   ├── catalog_docs/  # /catalog markdown (written by the sampler job), the agent instructions
│   ├── middleware/    # DbrixSandboxMiddleware, tools.py (fail-closed stubs)
│   ├── deepagents.py  # create_sandboxed_deep_agent(...)
│   └── skills/databricks-analyst/SKILL.md
├── examples/
│   ├── deep_agent_demo.py
│   └── langchain_agent_demo.py
└── tests/ (unit/, integration/, redteam/)
```

Module entry points, not a CLI product: `python -m dbrix.generator`, `python -m dbrix.lifecycle.gc`, `python -m dbrix.executor.build_image`.

---

## 3. Phase 1 — Local medallion data generator

**Objective:** a deterministic, offline, prod-like catalog to test against.

### 3.1 Manifest

One YAML manifest is the source of truth for the generator, the sampler, the rewriter, and the agent-facing catalog docs.

**Naming rule (used everywhere in this project):**
- **All SQL uses 3-part names**: `prod_catalog.<schema>.<table>`. This covers agent code, Gold `transform`s, catalog docs, examples, and tests. There is one rewriter (§6.1) and it handles all of them.
- **Manifest keys and FK references** use `<schema>.<table>`, because the catalog name is declared once at the top of the manifest (`catalog:`). They are identifiers inside that catalog, not SQL.

```yaml
catalog: prod_catalog
default_anchor: { table: silver.brands, key: brand_id }

tables:
  silver.brands:
    layer: silver
    description: Brands (customers) running influencer campaigns.
    primary_key: [brand_id]
    columns:
      brand_id:   { type: string, gen: uuid, description: Brand identifier }
      name:       { type: string, gen: company }
      industry:   { type: string, gen: { choice: [beauty, fashion, gaming, food, tech, travel] } }
      country:    { type: string, gen: country_code }
      created_at: { type: timestamp, gen: { between: [2018-01-01, 2024-01-01] } }
    rows: 100                          # multiplied by --scale

  silver.campaigns:
    layer: silver
    primary_key: [campaign_id]
    foreign_keys:
      - { columns: [brand_id], references: silver.brands(brand_id) }
    rows_per_parent: { parent: silver.brands, min: 2, max: 12 }
    columns: { ... }

  bronze.raw_post_events:
    layer: bronze
    primary_key: [event_id]
    foreign_keys:                      # flat key columns, so normal traversal applies
      - { columns: [post_id], references: silver.posts(post_id) }
    columns:
      event_id:    { type: string, gen: uuid }
      post_id:     { type: string }
      ingested_at: { type: timestamp }
      raw_payload: { type: string, description: Original JSON, kept for realism, not used for keys }

  gold.campaign_performance:
    layer: gold
    primary_key: [campaign_id]
    transform: |                       # Spark SQL, 3-part names; rerun inside each sandbox
      SELECT c.campaign_id, c.brand_id, COUNT(DISTINCT p.post_id) AS total_posts, ...
      FROM prod_catalog.silver.campaigns c
      LEFT JOIN prod_catalog.silver.posts p ON p.campaign_id = c.campaign_id ...
```

FKs may be **composite** (`columns: [a, b]` referencing a composite PK).

Manifest validation (fails fast with a clear error):
- every FK points at an existing table and its full PK, column by column, with compatible types;
- the FK graph among non-Gold tables is a **DAG**: no cycles and **no self-references** (a table may not reference itself);
- Gold tables have a `transform` and no `rows`; non-Gold tables have no `transform`;
- each Gold `transform` parses as Spark SQL (sqlglot) and references only manifest tables, by 3-part name.

### 3.2 Default domain: influencer marketing

| Layer | Table | PK | FKs / notes |
|---|---|---|---|
| bronze | `raw_post_events` | event_id | `post_id → silver.posts`. Duplicates and late arrivals on purpose |
| bronze | `raw_engagement_events` | event_id | `post_id → silver.posts`. Engagement snapshots |
| bronze | `raw_creator_profiles` | event_id | `creator_id → silver.creators`. Scraped profile snapshots |
| silver | `brands` | brand_id | **Default anchor** |
| silver | `campaigns` | campaign_id | `brand_id → brands` |
| silver | `creators` | creator_id | — |
| silver | `social_accounts` | account_id | `creator_id → creators`. 1–4 platform accounts per creator |
| silver | `campaign_creators` | (campaign_id, creator_id) | Many-to-many, plus fee and status |
| silver | `posts` | post_id | `campaign_id → campaigns` (**nullable**: organic posts), `account_id → social_accounts` |
| silver | `post_metrics_daily` | (post_id, metric_date) | `post_id → posts` |
| gold | `campaign_performance` | campaign_id | Posts, impressions, engagements, engagement rate, cost per engagement |
| gold | `creator_brand_summary` | (creator_id, brand_id) | Posts, engagements, and fees per creator per brand |
| gold | `brand_daily_engagement` | (brand_id, metric_date) | Daily rollup per brand |

### 3.3 Generation rules

- **Runs in the jobs container, Spark only.** Rows are built with Spark DataFrames (`spark.range` plus column expressions) and written as Delta with delta-spark. Gold `transform`s then run through the rewriter (§6.1) and `spark.sql`. The whole project uses one engine and one Delta writer.
- **Storage:** `prod_catalog/` uses the same layout as a sandbox (§4.3): a Derby metastore in `prod_catalog/_metastore/`, and databases `bronze`, `silver`, `gold` created with `LOCATION '/prod_catalog/<schema>'`, so each table's files are at `prod_catalog/<schema>/<table>/`. Tables are written with `saveAsTable`.
- **How determinism works:** generators never use `rand()`, because its output depends on how Spark partitions the data. Every random value comes from a hash: `xxhash64(seed, table, column, row_id)`, mapped onto the column's distribution (uniform, choice, skewed/Zipf, between). The result is the same regardless of partitioning or core count. Child rows get their parent through the same hashing (e.g. `rows_per_parent`), so FKs are valid by construction.
- **Order:** topological over the FK graph, then Bronze derived from Silver, then Gold.
- **Integrity:** every non-null FK resolves and there are no duplicate PKs. Checked after generation; any violation fails the run. This is the same function the sampler uses (§4.2 step 7).
- **Determinism:** `--seed` (default in `config.py`: `42`). The same seed, scale, and manifest give identical row content per table.
- **Scale:** `--scale` (default `1`) multiplies base row counts. At scale 1: about 100 brands, 5k creators, 1.5k campaigns, 200k posts, and 2M metric rows. Target is under 2 min including Spark startup.
- **Immutability:** `_catalog_meta.json` records the manifest hash, seed, scale, and each table's Delta version. The sampler refuses a catalog whose manifest hash doesn't match.

---

## 4. Phase 2 — Relational downsampling engine

**Objective:** a small, isolated sandbox in which every FK still resolves. It runs in the **jobs container**, with `prod_catalog` mounted read-only at `/prod_catalog` and the new sandbox dir mounted read-write at `/sandbox`.

### 4.1 Inputs

| Param | Default (`config.py`) | Notes |
|---|---|---|
| `agent_id`, `session_id` | required | Must match `[A-Za-z0-9-]{1,64}`. See §4.4 |
| `anchor_table`, `anchor_key` | manifest `default_anchor` | Must be a non-Gold table and one of its PK columns |
| `sample_size` | `5` | Number of distinct anchor keys. If larger than available, take all and log a warning |
| `unrelated_sample_fraction` | `0.05` | Used for null-FK rows and for tables with no selected parent |
| `seed` | derived from the IDs | `xxhash64(agent_id, session_id)` unless set. The same IDs always give the same starting data, so a sandbox rebuilt after GC matches the original (§9.6). Recorded in meta |

### 4.2 Algorithm

Write `S[T]` for the set of selected rows of table `T`.

1. **Seed:** sample `sample_size` distinct `anchor_key` values from `anchor_table` → `S[anchor]`.
2. **Down pass (children):** walk tables in topological order. For each table `C` with an FK to a parent `P` that the down pass already reached, add to `S[C]` the rows whose FK value is in `S[P]`. If several down-pass parents reach `C`, take the **union**.
3. **Null-FK rows:** for each FK through which the down pass reaches `C`, also add a random sample (at `unrelated_sample_fraction`) of the rows of `C` where that FK is null. A composite FK counts as null when any of its columns is null. Whether an FK has nulls is read from the data, not the manifest: an FK with no null rows simply adds nothing. These rows are part of the down pass, so their own children follow in later steps of step 2.
   *Example:* organic posts (`posts.campaign_id IS NULL`) are sampled here, and their `post_metrics_daily` and Bronze events follow them down.
4. **Up pass (parents only):** for each selected row and each of its non-null FKs, add the referenced parent row if it is missing. **Never** expand downward from a row the up pass added into a table that steps 2–3 already reached. Done as one walk over the tables, children first (reverse topological order), checking each table's selected rows when the walk reaches it. Because the FK graph is a DAG, a parent added here can only point at tables later in the walk, so one walk gives the same result as repeating until nothing changes.
   *Example:* brand → campaigns → posts → (up) social_accounts → (up) creators. Those creators do **not** bring in their posts for other brands.
5. **Fill and islands:** walk the non-Gold tables that steps 2–4 did not reach, in topological order (parents first). For each table:
   - **Fill**, if any of its parents has selected rows: select the rows where **every non-null FK** points at a row that is already selected, and at least one FK is non-null (semi-joins). No up pass is needed, because those parents are already present. A row with an FK pointing outside the sandbox is left out, rather than pulling in an unrelated parent.
   - **Island**, otherwise (no FK, or no parent with selected rows): a random sample at `unrelated_sample_fraction`, followed by an up pass.

   Because the walk goes parents first, an island's children are filled from the island's rows ("sample the parent, then take its children"), not sampled separately.
   *Example (fill):* `raw_creator_profiles` is not reached by the down pass, because its parent `creators` only enters through the up pass. Step 5 adds the profiles of the creators already in the sandbox, instead of a random 5% of all profiles (which would pull in about 2,000 unrelated creators).
   *Example (island):* a table `agencies` with no FK, and `agency_staff` with an FK to it, neither reachable from brands. `agencies` gets a 5% sample, then `agency_staff` gets all staff of those agencies. The default domain has no such tables.
   *Example (several FKs):* `account_follows(follower_id, followed_id)`, both FKs to `social_accounts`. With accounts X and Y in the sandbox, `X→Y` is kept and `X→Z` is left out, so Z is not pulled in.
6. **Gold:** do not copy. Rerun each Gold `transform` (through the rewriter) over the sampled Silver tables, so Gold always matches the sandbox.
7. **Validate:** every non-null FK resolves and there are no duplicate PKs (shared function, §3.3). On failure, creation fails (§9.5).

Nullable FKs with a null value are ignored by the up pass.

The algorithm is implemented as Spark semi-joins over Delta tables. Sets are never collected to the driver. Every random choice (the anchor keys, null-FK rows, islands) is a hash of `(seed, table, primary key)`, never `rand()` or `DataFrame.sample()`, so the result does not depend on partitioning (same rule as §3.3). Prod tables are read by path (`delta.`/prod_catalog/<schema>/<table>``); this is trusted code, so the policy does not apply.

**Scope of the sampler:** it builds into an empty directory it is given and returns the `sampling` block and `row_counts` of `_sandbox_meta.json` (§9.1). The lock, the `.creating-<id>` directory, the rename, and writing the meta file belong to the lifecycle layer (§9.5).

**Known limitation (accepted):** rows added by the up pass are **incomplete**: a creator added because one of their posts was selected only has the social accounts and posts that are linked to the sampled brands. Queries such as "accounts per creator" or "posts per creator across all brands" give smaller numbers in the sandbox than in prod. The agent-facing docs (`/catalog/README.md`) state this.

### 4.3 Outputs

```
sandbox_catalog/
├── .locks/<sandbox_id>.lock   # one empty lock file per sandbox (§9.5)
├── .creating-<sandbox_id>/    # only while being created
└── agent_{agent_id}_{session_id}/
    ├── _sandbox_meta.json
    ├── runs.jsonl
    ├── _metastore/            # Derby metastore (§6.3)
    ├── _results/              # full query results (CSV), exposed to the agent as /results
    ├── bronze/<table>/        # Delta
    ├── silver/<table>/        # Delta
    └── gold/<table>/          # Delta; agent-created tables land in their schema dir too
```

The sampler creates databases `bronze`, `silver`, `gold` with `LOCATION '/sandbox/<schema>'` in the sandbox metastore and writes each table with `saveAsTable`. Paths stored in the metastore are always `/sandbox/...`, the container mount point, so renaming the host dir from `.creating-<id>` to its final name changes nothing inside.

The logical namespace is `sandbox_catalog.agent_{agent_id}_{session_id}`, used in logs and metadata. Row counts per table are stored in the meta file.

### 4.4 IDs and the sandbox id

- `agent_id` and `session_id` must match `[A-Za-z0-9-]{1,64}`: letters, digits, and `-`. **No `_`, `.`, or `/`.**
- The sandbox id is `agent_{agent_id}_{session_id}`. Because neither ID can contain `_`, the id always splits back into exactly one `(agent_id, session_id)` pair. Two different pairs can never produce the same id (for example, `a_b` + `c` and `a` + `b_c` are both rejected).
- No `.` or `/` means no path injection (`../`).
- `session_id` is the LangGraph `thread_id` (§8.2). UUIDs pass. A `thread_id` that doesn't match is a host configuration error: the middleware raises `InvalidSandboxIdError` into the graph (it is not the agent's fault, so it is not sent back as feedback).

### 4.5 Speed (measured, to optimize later)

**Measured** (2026-10-25, scale 1, Docker Desktop with 8 CPUs and 8 GB): one sandbox takes **about 120 s**. The §11.4 target (under 30 s, including the executor start) is **not met yet**.

| Step | Time | Note |
|---|---|---|
| Start Spark | 6 s | |
| Select rows (§4.2 steps 1–5) | 48 s | About 20 checkpointed queries, 1–5 s each; the first is about 10 s (JVM warm-up) |
| Create schemas | 5 s | First use of the Derby metastore |
| Write 10 tables | 9 s | About 1 s each |
| Gold | 18 s | 3 transforms |
| Integrity check (read back) | 30 s | About 25 count queries |
| Row counts | 4 s | |

**The cause is not the data** (at most about 100k rows per table). Each query spends 3–5 s in **planning** and about 1 s running. The first time a query touches a Delta table, Delta reconstructs its snapshot from `_delta_log` with extra Spark jobs. The first job of each kind also pays JVM warm-up. File access through the bind mount is not a factor: reads inside the container's own filesystem are no faster.

**Tried, no gain (reverted):** running the integrity check as one Spark job (30 s → 29 s), and lazy `cache()` instead of eager `localCheckpoint()` (no change).

**Proposals, in the order to try them.** Measure each one on its own and keep only what helps:

1. **Delta settings for small tables.** `spark.databricks.delta.snapshotPartitions` defaults to 50, so every snapshot rebuild runs 50 tasks. Set it to 1–2. Also try `spark.sql.shuffle.partitions` at 1–2 and turning off Delta's statistics collection on write. These are config-only changes in `dbrix/spark.py`, and they help the generator and the executor too.
2. **Check integrity before writing.** Run the check on the selected DataFrames, which are already in memory, instead of reading the tables back from Delta. Gold can likewise be computed from temp views over the selected DataFrames, then written. This avoids a second Delta snapshot per table.
3. **Fewer prod table touches.** Read each prod table once and reuse the DataFrame. Avoid scanning a large table twice in the down pass (semi-join plus the null-FK sample).
4. **Fewer up-pass checkpoints.** The up pass makes a new checkpointed copy of the parent table for every FK of every selected table, even when it can add nothing. For example, `posts → campaigns`: every non-null `campaign_id` already came from `S[campaigns]` in the down pass. After each island, the whole walk runs again. Skip the FK the down pass came through, and after an island walk only from that island's parents.
5. **Overlap startup with sampling.** Start the executor container's JVM while the sampler runs, and open the metastore only after the sampler finishes (§9.5). This hides about 6 s.
6. **Move table registration to the executor.** The sampler writes Delta by path with no metastore, and the executor registers the manifest's tables once (`CREATE TABLE … USING DELTA LOCATION`) when it first starts. The executor initializes the metastore anyway, so the sampler's metastore start (about 5–10 s) goes away. This changes §4.3 and §6.3.
7. **Read prod as Parquet, not Delta.** Prod is written once and never changed, so its Parquet files are its content. This skips snapshot reconstruction on prod, but breaks "one engine, real Delta" and would silently break if prod is ever updated. Last resort.
8. **Start sampling earlier.** Start building the sandbox when the agent is built, or when a thread starts, instead of on the first tool call. This hides the wait instead of shortening it.

Not an option: sampling inside the executor container to save one JVM start. The executor must never see `prod_catalog` (§7).

---

## 5. Phase 3 — Security & AST inspection layer

**Objective:** find out what code reads and writes, and reject common misuse early with a precise, actionable error. The container (§7) is the security boundary; this layer gives feedback and powers the mode and approval rules.

### 5.1 Interface

```python
def check(code: str, language: Literal["sql", "pyspark"]) -> PolicyReport:
    """Mode-free. Raises SandboxSecurityException for code blocked in every mode."""

TableRef = str                    # dotted name, lowercase, unquoted: "prod_catalog.silver.posts"

class Location(BaseModel):
    statement: int | None         # SQL: index of the statement, from 0
    line: int | None              # Python line/col; for SQL inside spark.sql, the call's position
    col: int | None

class PolicyReport(BaseModel):
    reads: set[TableRef]          # tables read, when statically known
    writes: list[WriteOp]         # every write found, in every mode
    has_dynamic_sql: bool         # SQL or table names built at runtime (§5.4)

class WriteOp(BaseModel):
    kind: Literal["INSERT", "UPDATE", "DELETE", "MERGE", "CREATE_TABLE", "CREATE_VIEW",
                  "ALTER_TABLE", "DROP_TABLE", "DROP_VIEW", "TRUNCATE", "SAVE_AS_TABLE", "WRITE_TO"]
    target: TableRef | None       # None when the name is built at runtime
    location: Location

class SandboxSecurityException(Exception):
    rule_id: str          # e.g. "SQL_STMT_NOT_ALLOWED", "PY_FORBIDDEN_IMPORT"
    message: str
    language: str
    location: Location | None   # statement index, or line/col for Python
    snippet: str | None
    hint: str | None            # how to achieve the goal legitimately, if possible
```

`check` does not take the mode. The caller decides what the mode allows:
- **Middleware (host):** READ_ONLY + writes → `ToolMessage` error. READ_WRITE + writes, or dynamic SQL → approval (§8.3).
- **Runtime wrapper (container):** a write when `allow_writes` is false → `SandboxSecurityException` (rule `WRITE_NOT_ALLOWED`).

`insertInto` is reported as `INSERT`, so `INSERT` covers both the SQL statement and the DataFrame call.

`check` raises on the first blocked construct. `WriteOp.kind` is an internal label for code and `runs.jsonl`; people and the agent see the statement itself (§8.3).

### 5.2 One policy, two call sites

The same `check` function runs in two places. Neither place repeats the other's work:

| Where | When | What it does |
|---|---|---|
| Host (middleware) | Before running | Checks the whole code. Finds writes so the mode and approval rules can apply before anything runs |
| Container (runtime wrapper) | Each time a SQL string or table name is used | Checks that SQL string (this catches dynamic SQL), then resolves and rewrites table names (§6.1), and blocks writes if `allow_writes` is false |

**Table names are checked only at runtime**, because only the container knows which tables and temp views exist right now (including tables the agent created and temp views from earlier calls). The host does not check table names.

Statement kinds, Python imports, and the modules an attribute chain may pass through are short **allowlists** (unknown is blocked). SQL functions use an **audited blocklist**: every function of the pinned Spark version was reviewed once, and a Docker test fails when the image's `SHOW FUNCTIONS` output changes, so an upgrade forces a new review. Code that fails to parse is blocked. The red-team suite (§11.3) proves the container contains whatever the policy misses.

### 5.3 SQL policy (sqlglot, dialect `spark`)

This applies to `run_sql` and to every SQL string passed to `spark.sql` in PySpark. Multiple statements are allowed, and each one is checked.

| Statement | Result |
|---|---|
| `SELECT` (and `UNION`/`INTERSECT`/`EXCEPT`), `WITH … SELECT`, `DESCRIBE` (incl. `DETAIL`, `HISTORY`, `EXTENDED`), `SHOW TABLES/SCHEMAS/DATABASES/COLUMNS/CATALOGS` | read |
| `EXPLAIN <stmt>` | same as `<stmt>` (a write inside still counts as a write, though EXPLAIN never runs it) |
| `CREATE TEMP VIEW`, `DROP VIEW <1-part name>` | read (session-local temp view) |
| `INSERT`, `UPDATE`, `DELETE`, `MERGE` | write |
| `CREATE TABLE [AS]`, `CREATE VIEW`, `ALTER TABLE`, `DROP TABLE`, `DROP VIEW <3-part name>`, `TRUNCATE` | write |
| `CREATE/DROP CATALOG/SCHEMA/DATABASE`, `USE`, `CREATE GLOBAL TEMP VIEW` | blocked |
| `SET`, `RESET`, `ADD JAR/FILE`, `CREATE FUNCTION`, `REFRESH`, `CACHE`/`UNCACHE`, `MSCK`, `VACUUM`, `OPTIMIZE`, `EXECUTE IMMEDIATE`, `BEGIN … END` scripts | blocked |
| anything else | blocked |

sqlglot keeps `SHOW` and `EXPLAIN` as unparsed text and fails on `DESCRIBE DETAIL`; `policy/sql.py` parses the table name or inner statement of these itself, so the rules below apply to them too.

Additional rules:
- **No path-based sources:** ``delta.`/path` ``, ``parquet.`/path` `` (any table name part containing `/`), table-valued functions with a string-literal argument (`read_files('/p')`; `range(10)` is fine), `LOCATION '…'`, `INSERT OVERWRITE DIRECTORY`, and `OPTIONS (path …)` are blocked.
- **Function blocklist** (audited against Spark 4.2.0's 577 functions, data in `policy/sql.py`):
  - call Java: `reflect`, `java_method`, `try_reflect`;
  - read a path: `table_changes_by_path`, `from_protobuf`, `to_protobuf` (descriptor file);
  - leak sandbox paths or names (§6.5): `input_file_name`, `input_file_block_start`, `input_file_block_length`, `current_path` (returns `spark_catalog.default`).
  - Any identifier `_metadata` is also blocked. `python_worker_logs` needs a `SET` to work, which is blocked.
- **Table names:** checked at runtime (§6.1).

### 5.4 PySpark policy (Python `ast`)

| Rule | Result |
|---|---|
| Imports allowed only from: `pyspark.sql`, `pyspark.sql.functions`, `pyspark.sql.types`, `pyspark.sql.window`, `math`, `datetime`, `decimal`, `json`, `re`, `collections`, `itertools`, `functools`, `statistics`, `typing` | others blocked |
| Builtins `exec`, `eval`, `compile`, `__import__`, `open`, `getattr`, `setattr`, `delattr`, `globals`, `locals`, `vars`, `input`, `breakpoint`, `memoryview`, `exit`, `quit`, `help` | blocked |
| Underscore names and attributes (see below) | blocked |
| Module attributes: a chain that starts at a module (`T.os`, `F.sys`, `typing.sys`, `pyspark.SparkContext` after `import pyspark.sql`) may only pass through allowed modules. A module name may only be used as `name.attr` (no `m = T`) | others blocked |
| Attributes `sparkContext`, `rdd`, `catalog`, `pipe`, `addFile`, `addPyFile`, `registerJavaFunction`, `newSession`, `builder`, `dataSource`, `addArtifact`, `addArtifacts`, `copyFromLocalToFs`, `inputFiles`, and `conf.set` | blocked (hints: SQL `SHOW TABLES`/`DESCRIBE` for `catalog`, DataFrame methods for `rdd`) |
| Python names of blocked SQL functions (`F.reflect`, `F.input_file_name`, …, the §5.3 list) and `F.call_function` | blocked |
| String literals `"_metadata"` or `"_metadata.…"` | blocked |
| SQL expression strings: a string literal passed to `F.expr`, `selectExpr`, `filter`, or `where` | checked with the §5.3 function and `_metadata` rules |
| Path-based reads: `spark.read.load/parquet/csv/json/text/orc`, `.format(...).load(...)`, `readStream` | blocked (hint: use `spark.table`) |
| `spark.table("…")`, `spark.read.table("…")`, `spark.sql("…")` | read |
| `.write…saveAsTable`, `.write…insertInto`, `.writeTo(…)` | write (table-name targets only) |
| Path-based writes (`.save(path)`, `.parquet(path)`, …), a writer `path` option (`.option("path", …)`, `.options(path=…)`, `.saveAsTable(…, path=…)`), `**` unpacking in `.options(…)` or `.saveAsTable(…)` (it may hide `path`), `.writeStream` | blocked |
| Python UDFs (`F.udf`, `pandas_udf`) | allowed; their bodies are part of the code, so the same rules apply |

The runtime wrapper also wraps `F.expr`, `selectExpr`, `filter`, and `where`, so SQL expression strings built at runtime are checked too (§6.1).

**Underscore rules.** Private attributes are the way from PySpark into the JVM (`spark._jvm`, `spark._jsparkSession`, `df._jdf`, `sc._jsc`) and into Python internals (`x.__class__.__bases__`). Plain local names are not. So:

| Code | Result | Why |
|---|---|---|
| Any name starting with `__` (`__builtins__`, `__name__`, `def __f()`) | blocked | Dunder names reach Python internals |
| Any attribute starting with `_` (`spark._jvm`, `df._jdf`, `x.__class__`, `row._1`) | blocked | Private API; the route into the JVM |
| A plain name starting with a single `_` (`for _ in …`, `_tmp = df`, `def _helper()`) | allowed | Normal Python; reaches nothing private |
| A string containing `_` (`row["_1"]`, `F.col("_c0")`) | allowed | Strings are not attribute access; `getattr` is blocked anyway |

The hint for a blocked `row._1` is `row["_1"]`.

**SQL inside PySpark:**
- **`spark.sql("…")` with a string literal:** the SQL is extracted and checked under §5.3, and its writes are added to the `PolicyReport`.
- **Dynamic SQL and table names** (f-strings, variables): allowed statically (`has_dynamic_sql=True`). The runtime wrapper checks them when they run (§5.2).
  - **READ_ONLY:** `allow_writes` is false, so a runtime write raises `SandboxSecurityException`.
  - **READ_WRITE:** code that the static check can't prove read-only is treated as **"may write"** and goes through approval, with a note that the targets are unknown (`WriteOp.target = None`).

### 5.5 Known limits (documented, accepted)

A static check of Python can't be complete. The red-team suite (§11.3) tracks known bypass classes, among them aliases that hide a call chain (`r = spark.read; r.parquet(p)`) and `str.format` reading private attributes (`"{0.__class__}".format(x)`; it can read but not call). Anything that gets past §5 is still confined by §7: no prod data, no network, nothing on disk outside its own `/sandbox`, and resource limits.

---

## 6. Phase 4 — Code rewriting & execution engine

**Objective:** run the agent's prod-targeted code against its sandbox, transparently.

### 6.1 Rewriting (one path: the runtime wrapper)

All rewriting happens **inside the container, at runtime**. The host never rewrites code. There is no static PySpark rewriter.

The runtime wrapper replaces `spark.sql`, `spark.table`, `spark.read.table`, `DataFrameWriter.saveAsTable`, `DataFrameWriter.insertInto`, `DataFrame.writeTo`, and the SQL-expression entry points `F.expr`, `DataFrame.selectExpr`, `DataFrame.filter`, and `DataFrame.where` (string arguments only; these get the §5.3 function and `_metadata` rules) before agent code runs. `run_sql` runs each statement through the wrapped `spark.sql`, so both tools take the same path. Gold `transform`s in the jobs container use the same rewriter.

- **Lowest entry point only.** It patches the classes, at the method the others call (for example `spark.table` calls `DataFrameReader.table`, so only that is patched). Each name is checked and rewritten exactly once; a test proves it.
- **Path methods are blocked at runtime too.** The reader and writer path methods (§5.4 `PATH_METHODS`) always raise. This closes the alias gap of §5.5 (`w = df.write; w.save(p)`), so `allow_writes` holds for every write.
- **`spark.sql` takes only the query string.** Extra arguments (`args=`, `{name}` keyword formatting) are blocked with the hint "use one SQL string", because they hide table names from the rewrite.
- **The wrapper is passed in at startup** (`install`). The red-team containment tests (§11.3) start the runtime without it: raw Spark, no checks, rewrites, or blocks. The host picks it in the start command; the agent has no way to choose it.

For each SQL string (parsed with sqlglot) or table name, the wrapper:
1. runs `policy.check` on it (§5.2), and blocks writes if `allow_writes` is false;
2. resolves every table reference:

| Reference | Result |
|---|---|
| `prod_catalog.<schema>.<table>`, schema in `bronze`/`silver`/`gold` | rewritten to `<schema>.<table>`. If the table doesn't exist, Spark's own "table not found" error is returned, reverse-mapped |
| 1-part name that is a CTE in the same statement, or a temp view in the session (from this call or an earlier one) | left as is |
| 1-part target of `CREATE TEMP VIEW` or `DROP VIEW` | left as is (the view may not exist yet) |
| any other 1-part name, any 2-part name | blocked, with a hint showing the 3-part form |
| any other catalog or schema | blocked |

3. replaces `current_catalog()` with the literal `'prod_catalog'` (§6.5);
4. runs the rewritten statement.

Because the metastore (§6.3) knows every table in the sandbox, **tables the agent created** (`CREATE TABLE prod_catalog.gold.my_summary …`) can be read in later calls like any other table. Temp views from earlier calls work too, until the container restarts. Spark drops them then, and the name is blocked like any other unknown 1-part name (`TABLE_NAME_NOT_ALLOWED`).

The wrapper returns the list of rewritten statements, which goes into `runs.jsonl` as `rewritten_sql`.

### 6.2 Tools and engine

There is one engine, **local PySpark 4.2 with delta-spark**, and two tools:

- **`run_sql(query: str)`** runs `spark.sql(query)` for each statement. The result is the last statement's DataFrame.
- **`run_pyspark(code: str)`** runs the code with `spark`, `F` (`pyspark.sql.functions`), and `T` (`pyspark.sql.types`) predefined. The result is the variable `result` if it is set, otherwise the value of the last expression if it is a DataFrame. Otherwise only stdout is returned.

### 6.3 Executor container

- Image `dbrix-spark:<version>` (`docker/spark.Dockerfile`): Python 3.12, JRE 17, pyspark (with its bundled Hive support), delta-spark (pinned Delta jars baked in, no runtime downloads), sqlglot, and the `dbrix` package. No DuckDB or delta-rs.
- **One container per sandbox**, started lazily and kept warm until GC (§9.4) or eviction (below). Its PID 1 is `python -m dbrix.executor.runtime`, which holds a **long-lived SparkSession** so Spark starts only once per container.
- **Metastore (Derby):** the SparkSession uses Spark's built-in Hive metastore, stored at `/sandbox/_metastore` (`spark.sql.catalogImplementation=hive`, Derby connection URL pointing there, `spark.sql.warehouse.dir=/sandbox`, `derby.system.home=/tmp` so `derby.log` doesn't hit the read-only root). Results:
  - Tables survive container restarts with no extra code. Nothing scans dirs to re-register tables.
  - Tables are managed, so `DROP TABLE` deletes the data and the table does not come back.
  - New tables land under `/sandbox/<schema>/<table>`, never on the read-only root.
  - Derby allows one process at a time. The sampler (jobs container) finishes before the executor starts, which the creation lock guarantees (§9.5).
  - Cost: a few seconds added to Spark startup.
  - Hive support and Delta together must be checked against the pinned versions when building the image.
  - `spark.sql.sources.default=delta`, so `CREATE TABLE … AS` and `saveAsTable` without a format make Delta tables, as on Databricks.
- **Protocol:** newline-delimited JSON on the container's stdin/stdout, attached by the host through the Docker SDK. There are no sockets and no network.
  - Request: `{run_id, language, code, allow_writes}`. Response: a `RunResult` (§6.4).
  - At startup, **before Spark starts**, the runtime moves the protocol channel to a private copy of file descriptor 1, and points descriptor 1 at descriptor 2 (stderr, kept by `docker logs`). Nothing the agent or the JVM prints can reach the protocol channel by accident, and no file fills `/tmp`.
  - After Spark starts, the runtime sends one `{"ready": true}` line. The client waits for it before the first request; startup does not count toward `run_timeout`.
  - The client attaches to stdin/stdout **before** starting the container and keeps that connection, so no line is lost or replayed.
  - `run_id` must match `[A-Za-z0-9_-]{1,128}`, because it names the result file.
  - The host treats every response as untrusted: it validates it with pydantic and rejects lines larger than `MAX_RESPONSE_BYTES` (`config.py`).
- **Logs:** Spark's log level is `WARN`. Python stdout and stderr are captured per run into `RunResult` (8 KB each). JVM logs go to the container's stderr and are kept by Docker (`docker logs <container>`), with no extra code.
- **Per-run timeout:** `run_timeout`. It counts only the time the code runs, not time spent waiting for the lock (§9.5). On timeout the container is stopped and the run is recorded as `TIMEOUT`; the sandbox's next run restarts it. Tables survive (metastore); temp views and cached data are lost.
  - A broken answer (larger than `MAX_RESPONSE_BYTES`, or the output closed) stops the container the same way, because its output may be out of step. `restart_executor` starts the same container again on a new connection.
- **Container limit:** at most `max_containers` (default `2`) executor containers run at once, counted by the label `dbrix.sandbox_id=<id>` across all host processes. To start a new one when the limit is reached, the client stops the least recently used container that is not running a run. Its sandbox stays on disk and its container restarts on demand.
  - If every container is busy running a run, the call **waits** for a free slot, up to `run_timeout`. If none frees up in time, the agent gets an error `ToolMessage`: "executor busy, try again". This wait does not count toward the call's own run timeout. In async code it happens in the worker thread (§8.2).

### 6.4 Result

```python
class RunResult(BaseModel):
    ok: bool
    preview_markdown: str | None   # first `preview_rows` rows
    row_count: int | None          # None when more than `result_file_max_rows`
    columns: list[Column] | None
    full_result_path: str | None   # "/results/<run_id>.csv" (agent-visible path)
    stdout: str                    # truncated to 8 KB
    stderr: str                    # Python stderr, truncated to 8 KB
    truncated: bool                # true when rows were cut off
    duration_ms: int
    rewritten_sql: list[str]       # for runs.jsonl only, never shown to the agent
    writes_applied: list[WriteOp]
    error: AgentError | None       # type, message (reverse-mapped), hint, rule_id if policy
```

**Result file (CSV):**
- CSV, because the agent reads it with `read_file` (text only), and CSV is what Databricks offers for downloading results.
- The runtime collects `df.limit(result_file_max_rows + 1)` once. If it got more than `result_file_max_rows` rows, the extra row is dropped, `truncated=True`, and `row_count=None` ("more than 10,000 rows"). This avoids running the query twice for a count.
- Written with Python's `csv` module to `/sandbox/_results/<run_id>.csv`. Struct, array, and map columns are written as JSON strings (`to_json`); binary columns as base64.

The `ToolMessage` text contains: the preview table, then "N rows total, showing 50; full result at /results/<id>.csv (read with read_file)", then stdout, then stderr, then the error.

### 6.5 Keeping the illusion

The agent should never see that it is not on Databricks `prod_catalog`. The places where sandbox details could leak, and what we do:

| Leak | Handling |
|---|---|
| Table names in errors and stack traces (`silver.posts`, `spark_catalog.silver.posts`, and Spark's quoted form `` `silver`.`posts` ``) | Reverse-mapped to `prod_catalog.silver.posts` (quoted names stay quoted). Errors carry Spark's message (no JVM stack trace by default); a Python traceback shows only frames of the agent's own code (compiled as `<agent>`) |
| Paths (`/sandbox/silver/posts`, `file:/sandbox/...`) in any text | `/sandbox/<schema>/<table>` → `prod_catalog.<schema>.<table>`; any other `/sandbox` path → `<sandbox>` |
| Output rows of `SHOW`, `DESCRIBE` (incl. `DETAIL`, `HISTORY`, `EXTENDED`), `EXPLAIN` | The same reverse mapping is applied to every string value in these results, so `SHOW CATALOGS` returns `prod_catalog` and locations hide sandbox paths |
| stdout and stderr | Same reverse mapping, always, even when the agent prints data rows (rare false matches are accepted) |
| `current_catalog()` | Rewritten to `'prod_catalog'` (§6.1) |
| `input_file_name()` and related, `_metadata` column | Blocked by the policy (§5.3), with a hint |
| Catalog docs (`/catalog/`) | Always generated with prod names |

Reverse mapping is **not** applied to the data rows of normal queries (a `SELECT` over user data), so real data is never changed.

**Known leaks (accepted):** the Spark version, the exact text of some Spark error messages, the `default` schema in `SHOW SCHEMAS` and `current_database()`, and the container user `dbrix` in `current_user()`.

The class name `SandboxSecurityException` is not shown to the agent: `AgentError.type` for policy errors is `SecurityException`.

---

## 7. Isolation (the security boundary)

**Executor** (untrusted code):

```
--network none
--read-only  --tmpfs /tmp:rw,exec,nosuid,size=1g          # exec: Spark loads native codecs from /tmp
--mount type=bind,src=<sandbox_dir>,dst=/sandbox      # only data mount
--user 10001:10001
--cap-drop ALL  --security-opt no-new-privileges
--pids-limit 512  --memory 4g  --cpus 2
--label dbrix.sandbox_id=<id>
```

**Jobs container** (trusted dbrix code only, never agent input): `prod_catalog` mounted **read-only** at `/prod_catalog`, the target sandbox dir mounted read-write at `/sandbox`, network off. The generator mounts `prod_catalog` read-write instead.

- `prod_catalog/` is **never** mounted into an executor.
- The executor can write to its sandbox dir in both modes. That's acceptable because the sandbox is a disposable copy; the mode and approval flow govern what the agent is *allowed* to do, and `allow_writes` blocks unapproved writes at runtime. Read-only data mounts in READ_ONLY mode are listed in §13.

---

## 8. Middleware & Deep Agents integration

### 8.1 Tools: fail-closed stubs

`dbrix.middleware.tools` provides `run_sql` and `run_pyspark`. They have the same names, signatures, and docstrings a real Databricks tool would have ("Run Spark SQL against the Databricks catalog…").

Their bodies **raise `NotSandboxedError`**. If any agent or subagent ever calls them without the middleware, the call fails closed and never silently reaches a real system. Users may instead pass their own existing Databricks tools by name, and the middleware intercepts them the same way.

### 8.2 `DbrixSandboxMiddleware` (LangChain v1 `AgentMiddleware`)

```python
DbrixSandboxMiddleware(
    manager: SandboxManager,       # owns sampling settings, containers, lifecycle
    tool_map: dict[str, ToolSpec] = {"run_sql": ToolSpec("sql", code_arg="query"),
                                     "run_pyspark": ToolSpec("pyspark", code_arg="code")},
    mode: OperatingMode = OperatingMode.READ_ONLY,
    agent_id: str = "agent",
)
```

- It implements `wrap_tool_call`, plus an async variant. Calls to tools in `tool_map` are handled entirely by the pipeline in §1 and never reach the handler. Other tools pass through.
- **Mode is per call.** Each middleware instance applies its own `mode` to the calls it intercepts. The parent agent and a READ_ONLY `explorer` subagent can share one sandbox with different modes. The mode is logged per run in `runs.jsonl`, not stored on the sandbox.
- **Session identity:** `session_id` is the LangGraph `thread_id` from the run config. `agent_id` comes from the constructor. Subagents use the **parent's** `(agent_id, session_id)`, so they share one sandbox. Both must pass §4.4.
- **Async:** the async variant runs the whole blocking part (lock wait, Docker I/O, run) in a worker thread with `asyncio.to_thread`, so a call waiting for its turn never freezes the event loop.
- **Errors as feedback:** policy violations and runtime errors become a `ToolMessage` with `status="error"` and a hint, so the agent can correct itself. They are never raised into the graph. (Host configuration errors, such as `InvalidSandboxIdError`, are raised.)
- **Lazy creation:** the first intercepted call creates the sandbox (sampling plus container start, roughly 15–30 s). The middleware streams a progress custom event so UIs can show it.
- **Recreation notice:** the middleware keeps `dbrix_sandbox_created_at` in agent state (middleware `state_schema`). If the sandbox's `created_at` no longer matches (it was GC'd and rebuilt), the `ToolMessage` starts with: "Note: your sandbox expired and was recreated from the same starting data. Tables you created or changed earlier are gone." The state is then updated.
- Checked against LangChain 1.4.7: `wrap_tool_call(request, handler)` / `awrap_tool_call` return a `ToolMessage` or a `Command`; the state update is a `Command(update={"messages": [...], "dbrix_sandbox_created_at": ...})`. `session_id` comes from `request.runtime.config["configurable"]["thread_id"]`; the progress event goes through `get_stream_writer()`.
- In READ_ONLY, code with static writes is answered with an error (`rule_id` `READ_ONLY_WRITE`, logged as `POLICY_BLOCKED`); dynamic SQL alone runs with `allow_writes=false`, so the runtime wrapper blocks it if it writes.

### 8.3 Human approval for writes (READ_WRITE)

`interrupt_on` in Deep Agents is configured **per tool name**, but we need approval **only when the code writes**. The middleware therefore raises the LangGraph `interrupt()` itself, using the **same request and decision shape as Deep Agents' HITL middleware**, so any client that handles `interrupt_on` handles ours with no changes.

- **Interrupt payload:** tool name, the code, the `WriteOp` list (kind, target table in prod names or "unknown", location), the dynamic-SQL warning if relevant, and the sandbox id. The person approving is shown each write as the agent's own statement (e.g. `DROP VIEW prod_catalog.gold.v`), not the internal `kind` label.
- **Decisions:**
  - `approve` runs the code as is, with `allow_writes=true`.
  - `edit` runs the human-edited code, which **goes through the policy again** and asks again if it still writes.
  - `reject` sends the agent a `ToolMessage` with the human's optional feedback, and nothing runs.
- Several tool calls in one step may each raise an interrupt; LangGraph returns them together.
- Requires a checkpointer. `create_sandboxed_deep_agent` sets `InMemorySaver` by default. The pending approval lives only in the checkpoint, not in sandbox metadata.
- Approvals and rejections are written to `runs.jsonl` (`approval: {decision, by, at}`).
- Gold is **not** recomputed after agent writes. The agent's changes stand as written.

**Resuming is safe to repeat.** When a run resumes after an interrupt, LangGraph runs the tool step again from the start, so everything before `interrupt()` runs twice. Rules that make this safe:
1. Before `interrupt()`, only two things happen: `get_or_create` (returns the existing sandbox when it exists) and `policy.check` (pure). Nothing writes to `runs.jsonl` or `_sandbox_meta.json` before `interrupt()`.
2. `run_id` is the LangChain `tool_call_id`. The run log skips a `run_id` it has already written, so a repeated step never logs twice.
3. The approved code runs after `interrupt()` returns, so it runs once per decision.
4. Accepted gap: if the host crashes after an approved write ran but before the checkpoint was saved, the write may run again on resume. The sandbox is disposable, so this is accepted.
5. Checked: LangChain 1.4.7 runs each tool call of a step as a separate task (`Send` per call), so only the interrupted call repeats.

### 8.4 `create_sandboxed_deep_agent(...)`

This factory wires everything correctly, so the secure setup is the default:

```python
agent = create_sandboxed_deep_agent(
    model="claude-sonnet-5-5",
    mode=OperatingMode.READ_WRITE,
    agent_id="analyst",
    sampling=SamplingConfig(sample_size=5),   # passed to the SandboxManager
    subagents=[...],          # optional extra subagents; middleware injected automatically
    extra_tools=[...],        # non-code tools only
    # also: config (default dbrix.toml), docker (default from_env()), checkpointer
)
agent.invoke({"messages": [...]}, config={"configurable": {"thread_id": "s1"}})
```

It configures:

| Deep Agents feature | Configuration |
|---|---|
| `tools=` | `run_sql`, `run_pyspark`, plus `extra_tools` |
| `middleware=` | `DbrixSandboxMiddleware(...)` |
| **Backend** | `CompositeBackend`: `/catalog/` and `/results/` → `SandboxFiles`, a read-only backend that finds the current thread's sandbox on each call (Deep Agents 0.7 takes only fixed backend objects, and the agent serves every thread) and reads its `_catalog/` or `_results/`; reading builds the sandbox when it doesn't exist yet. `/skills/` → `FilesystemBackend` over the packaged skills. `/` → `StateBackend` (agent scratch and notes) |
| **`permissions=`** | Deny `write` on `/catalog/**`, `/results/**`, and `/skills/**`, so the agent can read them but not change them |
| **Excluded tools** | No sandbox backend is ever configured, so Deep Agents hides `execute` itself (it drops tools the backend can't serve). No `HarnessProfile`: it is registered per process and model, not per agent. Deep Agents 0.7 has no `eval` tool. A test checks that neither appears. Our two tools are the only code-execution path |
| **Subagents** | Built-in **`explorer`**: profiles tables and returns findings, and gets the same tools with a middleware instance forced to READ_ONLY. Each user-supplied subagent has `DbrixSandboxMiddleware` **injected** into its `middleware` list. Deep Agents' default general-purpose subagent would not inherit our middleware, so the factory passes its own `general-purpose` (Deep Agents' `GENERAL_PURPOSE_SUBAGENT` plus the main tools and the middleware), which replaces the default |
| **`system_prompt=`** | The generated agent instructions (the planned `AGENTS.md`): what the catalog is, the 3-part naming rule, the medallion layers, how to use the tools, the current mode, and that writes need approval. Passed as the system prompt, not `memory=`: no file or route, and the agent can't edit it |
| **Skills** | `databricks-analyst` SKILL.md (`skills=["/skills/"]`): Spark SQL vs PySpark patterns, common joins in this domain, reading `/results`, and handling policy errors |
| Checkpointer | `InMemorySaver` unless one is supplied |

**`/catalog/` contents** are generated once, when the sandbox is created, by the sampler job at its end (Spark is running and the selected rows are in memory), into `<sandbox>/_catalog/`, always using prod-facing names: `README.md` (schema overview, ER diagram as text, and the sampling limitation from §4.2), and `<schema>/<table>.md` (description, columns with types and descriptions, PK, FKs, sandbox row count, five sample rows). Tables the agent creates later are not in `/catalog`; the agent can `DESCRIBE` them.

**Enforcement test:** before building the agent, the factory checks every subagent that can call a tool in `tool_map` (a spec without `tools` gets the main agent's). Each must have `DbrixSandboxMiddleware`, otherwise the factory raises `MisconfiguredSandboxError`. A compiled subagent (`CompiledSubAgent`) can't be given the middleware, so one whose graph has a code tool is rejected. The main agent gets it by construction. The fail-closed stubs (§8.1) are the second line of defense.

The same middleware also works with plain `langchain.agents.create_agent` (see `examples/langchain_agent_demo.py`). The backend, memory, skill, and subagent features are Deep Agents-only.

Checked against Deep Agents 0.7.32. The agent libraries (`langchain`, `langgraph`, `deepagents`) are a host-only dependency group (`agents`); the `dbrix-spark` image installs no default group (§6.3).

---

## 9. Phase 5 — State management & lifecycle

### 9.1 `_sandbox_meta.json`

```json
{
  "spec_version": 1,
  "sandbox_id": "agent_analyst_s1",
  "agent_id": "analyst",
  "session_id": "s1",
  "status": "IDLE",
  "created_at": "2026-10-23T19:30:00Z",
  "last_used_at": "2026-10-23T19:42:10Z",
  "active_run": null,
  "sampling": {
    "anchor_table": "silver.brands", "anchor_key": "brand_id",
    "sample_size": 5, "seed": 918273, "unrelated_sample_fraction": 0.05,
    "manifest_hash": "sha256:…", "prod_table_versions": { "silver.posts": 0 }
  },
  "row_counts": { "silver.brands": 5, "silver.posts": 9312 },
  "container_id": "3f2a…",
  "run_count": 7,
  "last_error": null
}
```

- `active_run` is `{"run_id": "…", "started_at": "…"}` while a run is executing, otherwise `null`. GC uses it (§9.4).
- Writes are atomic (temp file + `os.replace`) and made while holding the sandbox lock (§9.5).

### 9.2 Status machine

```
CREATING ──ok──▶ IDLE ──run──▶ ACTIVE ──success──▶ IDLE
    │                            │
    └─fail─▶ FAILED ◀──failure───┘
                └──run──▶ ACTIVE   (a failed sandbox can be retried)
```

- `CREATING` exists only in a `.creating-<id>` dir (§9.5). A finished sandbox starts at `IDLE`.
- `FAILED` means the last run or creation failed. A run is code that reached the executor: any outcome other than `OK` (`RUNTIME_ERROR`, `TIMEOUT`, or a block by the runtime wrapper) sets `FAILED`, and the next run moves it on. Calls that never ran (`POLICY_BLOCKED` on the host, `REJECTED`) are logged and count as activity (`last_used_at`), but change neither the status nor `run_count`. A failed creation is kept until the TTL expires so it can be debugged.
- A pending approval is not a status. It lives in the LangGraph checkpoint (§8.3).

### 9.3 `runs.jsonl` (append-only, one line per run)

`run_id` (the `tool_call_id`), `started_at`, `duration_ms`, `tool`, `language`, `mode`, `code`, `rewritten_sql`, `outcome` (`OK | POLICY_BLOCKED | RUNTIME_ERROR | TIMEOUT | REJECTED`), `rule_id`, `error_message`, `row_count`, `writes`, `approval`.

The writer skips a `run_id` that is already in the file (§8.3). Policy-blocked attempts are logged too, which makes them useful for spotting adversarial behavior.

### 9.4 Garbage collection

`dbrix.lifecycle.gc.run(sandbox_root: Path, ttl_seconds: int, run_timeout: int, docker: DockerClient, now: datetime | None = None, dry_run: bool = False) -> GcReport`, also runnable as `python -m dbrix.lifecycle.gc` (which reads `dbrix.toml` and passes the values in). It is idempotent. The README ships cron and systemd-timer examples to run it hourly.

For each `sandbox_root/agent_*` directory (GC never waits for a lock: a busy sandbox is in use and skipped; under the lock, steps 1–3 are checked again):
1. Read the meta. If it is missing or corrupt, use the directory's mtime as `last_used_at`.
2. Skip it if `active_run` is set and its `started_at` is less than `2 × run_timeout` ago. An older `active_run` means a crashed run, which counts as idle.
3. If `now − last_used_at > ttl_seconds` (measured from **last activity**): take the sandbox lock, stop and remove its container (by label), then delete the directory.

Also: delete `.creating-*` directories older than `ttl_seconds` whose lock is free (left behind by a crash). Lock files in `.locks/` are left in place; they are empty.

A sandbox can be GC'd while an approval is pending. On resume it is rebuilt from the same seed and the agent gets the recreation notice (§8.2, §9.6).

### 9.5 Locking, creation, and concurrency

- **One lock per sandbox:** `sandbox_root/.locks/<sandbox_id>.lock`, an `fcntl` exclusive lock. It lives **outside** the sandbox dir, so it can be taken before the dir exists.
- **Runs are serialized:** a run holds the lock for its whole duration. Explained simply: the agent may ask for two queries at once, or two subagents may run at once. They share one container that runs one thing at a time, so the second call waits its turn. In async code the wait happens in a worker thread (§8.2), so nothing else freezes.
- **Creation happens once:**
  1. take the lock;
  2. if `agent_{…}` exists, release and use it (another caller finished first);
  3. if a `.creating-<id>` dir exists, it is left from a crash: delete it;
  4. build the sandbox in `.creating-<id>` (jobs container mounts it at `/sandbox`);
  5. write the meta with status `IDLE`, then rename the dir to `agent_{…}` (a rename is atomic);
  6. release the lock. The executor is started by the first run, not here: runs are the only place a container starts (§6.3 container limit).
  If building fails, the `.creating-<id>` dir is kept with status `FAILED` and the error, the agent gets an error `ToolMessage`, and the next call starts over at step 3.
- The jobs container and the executor never open the same Derby metastore at once, because the executor only starts after step 5.
- A run takes the lock, then makes sure the sandbox exists (rebuilding it if GC removed it while an approval was pending) and returns the sandbox it ran on, so the middleware can compare `created_at`.
- **Container limit:** a global lock `.locks/_executors.lock` makes "count running containers, evict, start" one step across host processes. Eviction stops (does not remove) the least recently used container (by `last_used_at`) whose sandbox lock is free. A waiting call looks again every second.
- A host process keeps a handle only for containers it started. A container of the sandbox it has no handle for (another process, a host restart) is removed and a new one started, because attaching to a running container could lose lines. A handle whose container id differs from the meta's `container_id` (the sandbox was rebuilt) is dropped the same way.

### 9.6 Rebuilding after GC

Because the default seed comes from `(agent_id, session_id)` (§4.1), a sandbox rebuilt for the same thread starts with **the same data** as the original. Anything the agent wrote is gone, and the recreation notice (§8.2) tells the agent so.

---

## 10. Configuration

**`config.py` is the single source of truth.** It defines one pydantic model per section below, with every default, plus fixed constants (`MAX_RESPONSE_BYTES`, the 8 KB output limits). No other module states a default; other parts of this spec refer here.

Values can be changed in `dbrix.toml`. Constructor arguments override the file. There are no environment-variable overrides.

```toml
prod_catalog_dir   = "./prod_catalog"
sandbox_root       = "./sandbox_catalog"
manifest           = "./manifests/influencer_marketing.yaml"
default_mode       = "READ_ONLY"

[generator]
seed  = 42
scale = 1

[sampling]
sample_size = 5
unrelated_sample_fraction = 0.05

[executor]
image = "dbrix-spark:0.1"
run_timeout = 120
max_containers = 2
memory = "4g"
cpus = 2
preview_rows = 50
result_file_max_rows = 10000

[lifecycle]
ttl_seconds = 86400
```

---

## 11. Quality, testing & acceptance criteria

**Tooling:** uv, Python 3.12, pytest, ruff, mypy (strict on `src/`). Integration and red-team tests need Docker and run locally: `pytest -m "docker and not slow" --basetemp=.pytest_docker_tmp`. The two scale-1 speed tests (generator, sampler) are marked `slow` and run on demand with `-m slow`, because each needs a scale-1 catalog and takes minutes. Docker runs use their own `--basetemp`, because pytest empties its basetemp at every start and a unit run started meanwhile would delete the Docker run's catalogs. Every Spark container costs 1–2 min (§4.5), so tests keep containers few: properties that only need the selected rows (determinism, the 5 seeds) run in memory in one container, and only one test builds a real sandbox. Agent-level tests use a scripted fake chat model, so no API key is needed.

### 11.1 Unit
- Manifest validation catches every error class in §3.1, including self-references and composite FK mismatches.
- The generator is deterministic: same seed gives an identical content hash per table.
- Sampler: no dangling FKs and no duplicate PKs for 5 random seeds. The up pass never adds children to tables reached by the down pass. Organic posts and their metrics appear. `raw_creator_profiles` contains only profiles of creators already in the sandbox. Gold equals the transform rerun over the sampled Silver. The same IDs give the same sandbox.
- IDs: `_`, `.`, `/`, and over-long IDs are rejected.
- Policy: table-driven cases for every row of §5.3 and §5.4, including each underscore case. `PolicyReport.writes` is correct. `check` gives the same result regardless of mode (it has no mode).
- Policy (Docker, slow): the image's `SHOW FUNCTIONS` output equals the audited list in `policy/spark_functions.txt`; a change means a new review (§5.2).
- Rewriter: every row of the §6.1 table. 3-part names round-trip. Reverse mapping removes every sandbox name and path from errors, stdout, and metadata output (§6.5), and leaves normal data rows unchanged.
- Run log: a repeated `run_id` is written once.
- GC: fake-clock tests for TTL, `active_run` skip, stale `active_run`, corrupt meta, and leftover `.creating-*` dirs.

### 11.2 Integration (Docker)
- Deep Agent end to end with a scripted model: lazy sandbox creation, `run_sql`, `run_pyspark`, preview, `/results` file readable with `read_file`, and `/catalog` readable but not writable.
- **HITL:** a write in READ_WRITE triggers an interrupt. Approve applies it; reject applies nothing and returns the feedback; edit re-runs the policy. Resuming does not log the run twice.
- **Agent tables:** after an approved `CREATE TABLE prod_catalog.gold.my_summary`, a later call can read it. It survives a container restart. After `DROP TABLE` it stays gone after a restart.
- **Temp views:** a temp view created in one call can be read in the next.
- **Illusion:** `SHOW CATALOGS`, `DESCRIBE DETAIL`, `EXPLAIN`, `current_catalog()`, and a "table not found" error show only prod names and no `/sandbox` paths.
- **Subagents:** the `explorer` and a user subagent both go through the middleware and share the sandbox; the explorer's write is blocked while the parent is READ_WRITE. A subagent deliberately built without middleware raises `MisconfiguredSandboxError` at build time, and the stub raises `NotSandboxedError` if forced.
- **Concurrency:** two parallel first calls create one sandbox. Two parallel async calls both finish, one after the other, and the event loop stays responsive.
- **Container limit:** a third sandbox evicts the least recently used idle container; the evicted sandbox works again on its next call. When both containers are busy, a third call waits and runs once one frees up, or gets "executor busy, try again" after `run_timeout`.
- **Rebuild:** after GC deletes a sandbox, the next call rebuilds it with the same data and the agent gets the recreation notice.
- `execute` and `eval` don't appear in the agent's tool list.
- A timeout kills and restarts the container, and the next run succeeds.
- Agent `print` output never breaks the protocol.
- The same middleware works with plain `create_agent`.

### 11.3 Red team (must all fail to touch prod or escape)
`getattr(df, "wr"+"ite")`, `__import__("os")`, `().__class__.__bases__…`, `exec(base64…)`, `spark.sparkContext._jvm`, `spark._jsparkSession`, `rdd.pipe("sh")`, `T.os.system("sh")`, `F.sys.modules["os"]`, `F.reflect(…)`, `F.call_function("reflect", …)`, `F.expr("reflect(…)")`, `df.write.option("path", "/x").saveAsTable(…)`, `df.write.saveAsTable(…, path="/x")`, `df.write.options(**{"path": "/x"})`, `spark.catalog.listTables()`, `spark.read.parquet("/prod_catalog/…")`, ``SELECT * FROM delta.`/prod_catalog/silver/posts` ``, `SELECT reflect('java.lang.Runtime', …)`, `EXECUTE IMMEDIATE 'DROP TABLE …'`, `ADD JAR`, `SET spark.…`, `CREATE FUNCTION … USING JAR`, `../` and `_` in agent/session IDs, dynamic `spark.sql(f"DROP TABLE {t}")` in READ_ONLY, an unapproved dynamic write in READ_WRITE (`allow_writes=false`), and network access attempts.

For each case: the policy should block it (this is the feedback layer). Separately, with the policy **disabled** in both places, the container must still keep the attempt away from prod data and the network (this is the security boundary). Both are tested on their own. The containment test sends requests straight to the executor client (no middleware) and starts the runtime without the wrapper (§6.1), with the same isolation flags as a normal executor.

### 11.4 Acceptance (v0 is done when)
1. `python -m dbrix.generator` at scale 1 finishes in under 2 min and passes integrity checks.
2. Sandbox creation with defaults finishes in under 30 s, including first Spark start, and passes FK validation. *Not met yet: about 120 s measured; see §4.5.*
3. A Deep Agent built with `create_sandboxed_deep_agent` and a real Claude model answers "Which creator had the best engagement rate for brand X last month?" using an unchanged `run_sql` tool.
4. In READ_WRITE, an agent's `CREATE TABLE prod_catalog.gold.my_summary AS …` pauses for approval. After approval the table exists only in that sandbox, and the agent can query it in its next call.
5. Every red-team case is blocked by the policy **and** contained when the policy is disabled.
6. GC removes expired sandboxes and their containers, and keeps active and unexpired ones.

---

## 12. Implementation order (suggested)

1. `config.py`, manifest + validation → generator (with metastore) → integrity checks.
2. Sampler + Gold recompute + FK validation.
3. Executor image + runtime (metastore, protocol, CSV results) + client (no policy yet) → run a query end to end.
4. Policy + runtime wrapper (rewrite, `allow_writes`) + reverse mapping, with the red-team suite alongside.
5. Middleware for plain `create_agent`, including locking, creation, and the container limit.
6. HITL approval flow.
7. `create_sandboxed_deep_agent`: backends, permissions, memory, skill, subagents, enforcement.
8. Lifecycle meta, run log, GC, recreation notice.

---

## 13. Deferred to later versions

- **DuckDB engine** (lighter and faster than Spark; needs Spark→DuckDB transpilation and a Delta write-back strategy).
- **MCP server** for off-the-shelf clients (Claude Code, Claude Desktop).
- **CLI** (`dbrix generate / sandbox / gc`).
- **Claude Agent SDK adapter** and a framework-agnostic `@sandboxed` decorator.
- **Messy nested-JSON Bronze** with `json_foreign_keys` JSONPath extraction.
- Read-only data mounts in READ_ONLY mode; gVisor/nsjail runtimes.
- Connecting to a **real** Databricks workspace as the prod source.
- 1- and 2-part names and `USE CATALOG/SCHEMA`.
- Row-budget closure sampling; anchors drawn from Gold grain.
- Self-referencing FKs.
- A full SQL function allowlist (v0 uses a short blocklist; the container is the boundary).
- Environment-variable config overrides.
- Stopping containers after an idle period, and removing orphan containers (v0 relies on the container limit and GC).
- `AWAITING_APPROVAL` and `EXPIRED` statuses.
- CI (GitHub Actions).

---

## 14. Decision log (v0_spec → this spec)

| # | Topic | v0_spec | Decision |
|---|---|---|---|
| 1 | Form | unspecified | **Middleware** (LangChain v1 `AgentMiddleware`) plus the full `create_sandboxed_deep_agent` factory. Other front doors deferred |
| 2 | Engine | "DuckDB/PySpark" | **Local PySpark + delta-spark only**, because it's the closest to Databricks. Two tools: `run_sql` (`spark.sql`) and `run_pyspark`. AGENTS.md updated to match |
| 3 | Threat model | implicit | Adversarial. The container is the boundary; the policy gives feedback and finds writes |
| 4 | Policy style | keyword denylist | Short allowlists for statement kinds and imports, short blocklist for SQL functions. One `check` function, called on the host (before running) and in the container (at runtime). Returns a `PolicyReport` that lists writes |
| 5 | READ_WRITE | undefined | Writes allowed only into the sandbox, **each paused for human approval** (LangGraph interrupt, Deep Agents HITL shape) |
| 6 | Sampling | filter tables that have the anchor column, otherwise random | **FK graph traversal**: down from the anchor (plus a sample of null-FK rows), up for parents only, then fill unreached tables from selected parents |
| 7 | Anchor | Gold table, `user_id` | Default `silver.brands.brand_id`. Gold is derived |
| 8 | Gold | filtered | **Recomputed** with Spark from sampled Silver |
| 9 | Bronze | — | Kept, with **flat key columns**, so normal traversal applies |
| 10 | FK source | — | YAML manifest is the single source of truth. Composite FKs supported; self-references not |
| 11 | Naming | `prod_catalog.silver_orders` | All SQL uses 3-part `prod_catalog.<layer>.<table>`, including Gold transforms. Manifest keys use `<layer>.<table>` |
| 12 | Domain | users/orders | Influencer marketing: brands, campaigns, creators, accounts, posts, metrics |
| 13 | Rewriting | "regex or AST" | AST only (sqlglot), in **one place**: the runtime wrapper in the container. Dynamic SQL allowed. Table names are checked there. Reverse-map names in errors and metadata output |
| 14 | Execution | ephemeral session | Docker container per session with a long-lived SparkSession and a Derby metastore in the sandbox. No network, no prod mount. At most 2 containers at once |
| 15 | Generator engine | DuckDB, writing via delta-rs | **Spark only.** Hash-based deterministic generation, written by delta-spark. Generator and sampler run in a trusted jobs container (same image), so the host needs no JVM |
| 16 | Result | DataFrame / terminal output | `RunResult`: 50-row preview plus the full result as a CSV under `/results`; stdout and Python stderr captured; JVM logs in `docker logs` |
| 17 | Deep Agents | — | Read-only `/catalog` + `/results` backends, enforced middleware on all subagents, `AGENTS.md` memory, analyst skill, `execute`/`eval` removed |
| 18 | Status | ACTIVE → COMPLETED/FAILED | CREATING / IDLE / ACTIVE / FAILED, plus `runs.jsonl` |
| 19 | TTL | 24 h from creation, regardless of status | 24 h from **last activity**. Running sandboxes are protected via `active_run` |
| 20 | Cleanup | cron or daemon | `gc.run()` function and module entry point, run hourly by cron/systemd |
| 21 | Creation | — | Lazy on first intercepted tool call. `session_id` is the LangGraph `thread_id`. Built in `.creating-<id>`, then renamed; lock outside the dir |
| 22 | Scale | — | Small default (about 200k posts), `--scale` flag, fixed seed |
| 23 | Tooling | — | uv, Python 3.12, pytest, ruff, mypy strict, red-team suite, scripted fake model. Docker tests run locally; no CI in v0 |
| 24 | Mode | per session | **Per tool call.** `policy.check` is mode-free; the middleware and the runtime (`allow_writes`) apply the mode |
| 25 | IDs | — | `[A-Za-z0-9-]{1,64}`, no `_`, so `agent_{agent_id}_{session_id}` can't collide |
| 26 | Python underscores | — | Block `__` names and all `_` attributes; allow plain single-`_` names |
| 27 | Config | — | `config.py` holds every default; `dbrix.toml` and constructor args override; no env vars |
| 28 | Sandbox seed | random | Derived from `(agent_id, session_id)`, so rebuilds after GC give the same data; the agent is told its writes are gone |
| 29 | Approval resume | — | Nothing is written before `interrupt()`; `run_id = tool_call_id`; the run log skips duplicates |
| 30 | Concurrency | — | Runs serialized per sandbox; async waits in a worker thread; the timeout counts only running time |
| 31 | Container limit | — | At most 2 executors. Evict the least recently used idle one; if all are busy, wait up to `run_timeout`, then return "executor busy" |
| 32 | Sampler fill and islands | separate steps | One parent-first walk: an island is sampled, then its children are filled from it. A filled row needs every non-null FK to point at a selected row, so no unrelated parents are pulled in |
| 33 | Nullable FKs | — | Read from the data (any null rows), not from the manifest. An FK with no nulls adds no null-FK sample |
| 34 | Up pass | repeat until nothing changes | One children-first walk; same result on a DAG |
| 35 | Sampler scope | — | Builds into an empty dir and returns the `sampling` block and row counts. Lock, `.creating` dir, rename, and meta file are the lifecycle layer's job |
| 36 | SQL function list | short blocklist | **Audited blocklist**: all 577 functions of Spark 4.2.0 reviewed once; a Docker test fails when `SHOW FUNCTIONS` changes. Adds `try_reflect`, `table_changes_by_path`, `from_protobuf`/`to_protobuf`, `current_path` |
| 37 | Module attributes | — | Allowed modules expose others (`T.os`, `F.sys`, `typing.sys`). An attribute chain from a module may only pass through allowed modules; a module name is used only as `name.attr` |
| 38 | Spark 4 Python API | — | Block `F.reflect`/`java_method`/`try_reflect`/`call_function`, `spark.catalog`, `df.rdd`, `dataSource`, `addArtifact(s)`, `copyFromLocalToFs`, `inputFiles`, `readStream`, and a writer `path` option |
| 39 | SQL expression strings | — | `F.expr`, `selectExpr`, `filter`, `where` strings get the function and `_metadata` rules: literals statically, all strings in the runtime wrapper |
| 40 | `EXPLAIN` | read | `EXPLAIN X` is treated like X (a write inside needs approval though nothing runs). Less code; errs safe |
| 41 | `DROP VIEW` | — | 3-part name: write `DROP_VIEW`; 1-part (temp view): read. Approval shows the statement, not the label |
| 42 | `TableRef` | — | Lowercase, unquoted dotted name (Spark names ignore case) |
| 43 | Wrapper patch points | list of methods | Patch only the lowest entry point, on the classes, so each name is rewritten once. Path methods raise at runtime too, so aliases can't write without approval |
| 44 | `spark.sql` arguments | — | Only the query string; `args=` and keyword formatting are blocked |
| 45 | Shared statement parsing | — | Check and rewrite use one `parse_statement` (sqlglot plus the `SHOW`/`EXPLAIN`/`DESCRIBE DETAIL` forms sqlglot can't parse) |
| 46 | Stray fd 1 output | capture file in `/tmp` | Pointed at stderr (`docker logs`): same protection, nothing fills `/tmp` |
| 47 | Default table format | — | `spark.sql.sources.default=delta`, as on Databricks |
| 48 | Reverse mapping of stdout | — | Always applied (option A); quoted names and `file:` paths are covered |
| 49 | Policy-disabled red-team runs | — | The wrapper is passed to the runtime at startup; containment tests run without it (raw Spark), the strictest case. No flag the agent can reach |
| 50 | Container limit | Phase 4 | Built with locking in Phase 5 (§12 step 5). Phase 4 runs one container per sandbox: start, run, timeout, restart |
| 51 | Executor `/tmp` | `rw,size=1g` | `rw,exec,nosuid,size=1g`: Docker's tmpfs default is `noexec`, and Spark's snappy/lz4/zstd load a native library they unpack into `/tmp`. No new capability: `/sandbox` already allows exec |
| 52 | Temp view after a restart | Spark's "not found" error | Blocked as an unknown 1-part name (`TABLE_NAME_NOT_ALLOWED`): the wrapper can't tell a lost view from a wrong name |
| 53 | Broken executor answer | — | The container is stopped, as on timeout, and restarted in place. A closed connection or a response that fails validation counts as broken |
| 54 | Run status | `ACTIVE → FAILED` on failure | Any run that reached the executor and did not end `OK` sets `FAILED`; `run_count` counts only those runs. Blocked and rejected calls update `last_used_at` only |
| 55 | Executor start | at creation (§9.5 step 6) | Only in a run, so the container limit lives in one place. Creation releases the lock after the rename |
| 56 | Foreign containers | — | A container this host process did not start is removed and started fresh |
| 57 | GC and busy sandboxes | take the lock | Never wait: skip a busy sandbox, and check expiry again under the lock |
| 58 | Container limit across processes | label count | Count, evict, and start under one global lock; evict = stop the LRU container whose sandbox lock is free |
| 59 | Phase 5 scope | §9 | §9 plus all of §8 (middleware, approval, factory, docs, skill). There is no Phase 6 |
| 60 | Agent libraries | dependencies | Host-only dependency group `agents`; the Spark image (which runs untrusted code) does not install them |
| 61 | Removing `execute` / `eval` | `HarnessProfile(excluded_tools)` | Nothing to configure: Deep Agents hides `execute` without a shell backend, and has no `eval`. A profile would apply to the whole process. A test checks both are absent |
| 62 | General-purpose subagent | middleware injected | Our own `general-purpose` spec replaces the default, which would not inherit the middleware |
| 63 | Per-thread `/catalog` and `/results` | `FilesystemBackend` per route | `SandboxFiles`: a read-only backend that looks up the thread's sandbox per call. Reading builds the sandbox |
| 64 | Catalog docs and sample rows | at creation | Written by the sampler job into `_catalog/`; sample rows from the selected rows in memory |
| 65 | `AGENTS.md` | `memory=` | Generated text passed as the system prompt |
| 66 | Enforcement test | inspect graph nodes | Check subagent specs before building; reject compiled subagents that have a code tool |
| 67 | READ_ONLY and dynamic SQL | — | Static writes: error before running. Dynamic SQL only: runs with `allow_writes=false` |
