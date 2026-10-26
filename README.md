# dbrix-harness

**A practice copy of a Databricks workspace for AI agents.** The agent writes ordinary Spark SQL or PySpark against
production table names. dbrix runs it on a small, realistic sample inside a sealed container, and hands back the
answer. The agent never touches real data, and never knows it isn't on production.

Think of it as a flight simulator: the controls are the real ones, the plane isn't.

```
agent ──run_sql("SELECT … FROM prod_catalog.gold.…")──▶ dbrix
                                                         ├─ check the code (does it write? is it misuse?)
                                                         ├─ ask a human, if it writes
                                                         └─ run it in a sealed container, on the sample
agent ◀──────────── preview table, or an error with a hint ┘
```

## The problem

We want AI agents to answer questions from company data. Giving them the real workspace is risky (an agent can make
mistakes or be manipulated by text it reads) and costly. Giving them fake data makes the answers meaningless. dbrix
sits in between: real names, a real engine, a sample whose relationships still hold, behind a hard wall.

## Scope

| In v0 | Not yet |
|---|---|
| A synthetic "production" catalog (bronze, silver, gold) and a per-session sample of it | A connection to a real Databricks workspace |
| The agent's own `run_sql` / `run_pyspark` tools, unchanged | A faster engine (DuckDB) |
| A human approves every write (LangChain / Deep Agents) | Writes through MCP |
| Two front doors over one engine: LangChain / Deep Agents middleware, and an MCP server for Claude Code | Automated tests on every change (CI) |
| Automatic cleanup of idle sandboxes | Keeping one MCP sandbox across sessions |

## Constraints

- **The agent is not trusted.** It may be wrong, or prompt-injected into trying to escape.
- **No cloud cost for testing.** Everything runs on one machine with Docker.
- **The copy must behave like production,** or the agent's answers prove nothing.
- **A small team.** Every line of code has to earn its place ([AGENTS.md](AGENTS.md)).

## Why it is built this way

The full reasoning for every choice is in the decision log, [full_spec.md §14](full_spec.md).

**Safety comes from a wall, not a filter.** An agent can be manipulated into trying harmful things, and a fast
delivery could not come at the expense of company data. So each session runs in a sealed container that never sees
production: no network, a read-only system, only its own sample. The code checks still run, but their job is to give
the agent clear feedback early; they are not what keeps data safe. The price is a slower start for each sandbox. (§5, §7)

**The real engine, not a look-alike.** A lighter engine would start faster, but results that differ from Databricks
would undermine the whole point. dbrix runs Spark with Delta tables, the engine Databricks itself uses. Building a
sandbox takes about two minutes today; making that faster is planned. (§4.5)

**Samples that still add up.** Production is too big to copy, and a random slice breaks joins. dbrix samples a few
brands together with everything that belongs to them, so queries across tables give sensible answers. Totals across
all brands come out smaller than in production, and the agent is told so. (§4.2)

**A human in the loop only where it matters.** Agents sometimes need to create or change tables, but unchecked
changes are not acceptable. Reading is never blocked; any code that writes waits for a person to approve it. Through
MCP, v0 stays read-only. (§8.3)

**The agent believes it is on production.** An agent that knows it is being tested may behave differently. It sees
production names, and every trace of the sandbox is scrubbed from errors and output. (§6.5)

**One engine, two doors.** Teams use different agent frameworks, and a separate integration for each would multiply
the upkeep. One shared engine serves both LangChain / Deep Agents and an MCP server that Claude Code plugs into. (§8)

**Nothing piles up.** Every session leaves data on disk, while past sessions are rarely needed again. Sandboxes are
deleted after 24 hours idle, and one that is needed again comes back with the same data. (§9.4)

## Status

Works end to end: generating the catalog, building sandboxes, running and checking code, approvals, cleanup, the Deep
Agents factory, and the MCP server over stdio and HTTP. Each part has unit tests, Docker integration tests, and a
red-team suite of escape attempts (§11).

Known gaps, stated plainly:

- **Speed.** A sandbox takes about 120 s to build against a 30 s target. Measurements and a plan are in §4.5.
- **Local MCP is not a wall around Claude Code.** Claude Code has its own file and shell tools and can open
  `prod_catalog/` directly. Run the MCP server remotely, or deny those paths in Claude Code's settings. (§8.5)
- **One MCP sandbox per server process.** Every new Claude Code session builds a new one, and clients of one HTTP
  server share one.
- **Small leaks of the illusion** are accepted: the Spark version, some error texts, the container user name. (§6.5)

## Try it

You need Docker and [uv](https://docs.astral.sh/uv/). Settings live in `dbrix.toml`
(copy [dbrix.toml.example](dbrix.toml.example); every default is in `src/dbrix/config.py`).

```
uv sync
uv run python -m dbrix.executor.build_image   # the Spark image, once
uv run python -m dbrix.generator              # the synthetic prod catalog, once (under 2 min)
```

**With Claude Code (MCP).** From an empty folder, so Claude Code can't read the repo directly:

```
claude mcp add dbrix -- uv run --directory /path/to/dbrix-harness python -m dbrix.mcp_server
claude
```

Ask a question about the catalog. The first call builds the sandbox (about two minutes); if it times out, start
Claude Code with `MCP_TOOL_TIMEOUT=600000`. For a remote server, set `[mcp] token` and run with `--transport http`.

**With Deep Agents or LangChain.** See [examples/deep_agent_demo.py](examples/deep_agent_demo.py) and
[examples/langchain_agent_demo.py](examples/langchain_agent_demo.py) (they need an Anthropic API key).

**Cleanup.** `uv run python -m dbrix.lifecycle.gc` removes expired sandboxes and their containers.

**Tests.**

```
uv run pytest                                                         # unit
uv run pytest -m "docker and not slow" --basetemp=.pytest_docker_tmp  # Docker, about 10 min
```

## Where to read more

- [full_spec.md](full_spec.md): the specification. §1 architecture, §7 the security boundary, §13 what's deferred,
  §14 every decision and why.
- [AGENTS.md](AGENTS.md): the engineering rules (no code > DRY > SOLID).
