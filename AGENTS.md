# Agent Guidelines (`AGENTS.md`)

All agents working in this repository must follow these engineering and communication rules.

## Engineering Principles (Ordered by Priority)

### 1. The Best Code is No Code
Every line of code is a liability to read, test, debug, and maintain.
- **Delete before you add:** Always ask if a requirement can be met by deleting code, simplifying a flow, or using built-in features (e.g., native DuckDB, `delta-rs`, Python `ast` / `sqlglot`, standard library).
- **Build only what is needed now (YAGNI):** Implement only what [v0_spec.md](file:///home/alfred/github/dbrix-harness/v0_spec.md) explicitly requires. Do not add "future-proof" hooks, extra config flags, abstract base classes for single implementations, or unused parameters.
- **Use SQL over Python loops:** Use DuckDB queries or simple data structures instead of multi-line loops wherever possible.
- **Keep it small and direct:** Avoid wrapper classes or pass-through functions that only add indirection.

### 2. Be DRY (Don't Repeat Yourself)
Every piece of knowledge must live in one place.
- **Single source of truth:** Define schemas, catalog paths, table names, and configuration constants once. Never hardcode the same path pattern or SQL template in multiple files.
- **Share common tools:** Reuse catalog discovery, Delta table reading/writing, and AST parsing/rewriting functions across phases instead of writing them twice.
- **Don't abstract too early:** Wait until duplication actually exists before extracting a helper, and keep helpers as simple functions.

### 3. Be SOLID (Pragmatically Applied)
Keep components decoupled and testable without adding boilerplate:
- **S — Single Responsibility:** Each phase in [v0_spec.md](file:///home/alfred/github/dbrix-harness/v0_spec.md) (Generator, Downsampler, Security/AST Inspector, Rewriter/Executor, Lifecycle Manager) does one job and has one reason to change.
- **O — Open/Closed:** Write rules (like AST security checks or sampling strategies) as simple data or functions so new rules can be added without rewriting the core engine.
- **L — Liskov Substitution:** Swappable parts must take the exact same inputs and return the exact same outputs without hidden side effects.
- **I — Interface Segregation:** Pass only what a function actually needs (e.g., a `Path` or a single setting, not a whole global config object).
- **D — Dependency Inversion:** Pass paths and database connections into functions as arguments so every phase can be tested on its own.

### Priority Rule
When principles conflict, follow this order:
> **No Code > DRY > SOLID**
> Never build a class hierarchy (SOLID) or a complex helper (DRY) when a 10-line function using the standard library or DuckDB solves the problem.

## Communication & Workflow Rules

- **Speak simply:** Use plain, direct language. Keep explanations short and easy to read.
- **No buzzwords:** Avoid corporate, marketing, or filler jargon (e.g., "synergy", "paradigm", "robust", "seamless", "leverage", "enterprise-grade"). State facts and concrete actions.
- **Make no assumptions — ask questions to disambiguate:** If anything in a prompt or spec is unclear, incomplete, or can be interpreted in more than one way, stop and ask the user before writing code. Do not guess.
