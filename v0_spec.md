Phase 1: Local Medallion Data Generator (The Testbed)

Objective: Establish a deterministic, offline replica of a Databricks environment to validate sandbox logic without cloud costs.

    Core Logic: Use DuckDB to generate synthetic data representing three layers: Bronze (raw events), Silver (cleaned entities like users/orders), and Gold (aggregated metrics).

    Constraints: Must enforce strict Primary Key (PK) and Foreign Key (FK) relationships across the layers so the downsampler has valid relational graphs to process.

    Outputs: Writes out standard Delta Lake tables (delta-rs) into a prod_catalog directory, acting as the immutable source of truth.

Phase 2: Relational Downsampling Engine

Objective: Create lightweight, isolated environments that preserve data integrity across table joins.

    Inputs: Target sandbox directory, anchor_table (e.g., Gold metrics), anchor_key (e.g., user_id), and sample_size.

    Core Logic:

        Read the anchor_table and extract a randomized list of unique anchor_key values matching the sample_size.

        Iterate through all tables in the prod_catalog.

        If a table contains the anchor_key, filter it to include only the sampled keys. If it lacks the key, apply a standard random sample.

    Outputs: Writes the filtered Delta tables into an isolated directory structure mimicking Unity Catalog (sandbox_catalog.agent_{id}_{session}).

Phase 3: Security & AST Inspection Layer

Objective: Prevent unauthorized data modification or destructive actions before execution begins.

    Inputs: The raw PySpark/SQL code string from the agent, and the active operating_mode (Read-Only vs. Read-Write).

    Core Logic: Parse the code string into an Abstract Syntax Tree (AST). Traverse the tree searching for destructive SQL keywords (DROP, TRUNCATE, INSERT, UPDATE, DELETE) and equivalent DataFrame API calls (.write, .save(), .insertInto()).

    Constraints: If operating_mode == "READ_ONLY" and a destructive operation is found, execution is blocked instantly.

    Outputs: Returns a True boolean if safe, or raises a structured SandboxSecurityException outlining the exact violation.

Phase 4: Code Rewriting & Execution Engine

Objective: Seamlessly redirect agent queries meant for production into the localized sandbox without requiring the agent to change its code.

    Inputs: The validated agent code string and the specific sandbox namespace (e.g., sandbox_catalog.agent_123).

    Core Logic:

        Intercept table references in the code (e.g., FROM prod_catalog.silver_orders) and rewrite them using regex or AST manipulation to point to the sandbox directory.

        Mount the sandbox Delta tables into an ephemeral DuckDB/PySpark session.

        Execute the rewritten code.

    Outputs: Returns the query result (e.g., a DataFrame or terminal output) or captures runtime exceptions to pass back to the agent.

Phase 5: State Management & Lifecycle Daemon

Objective: Prevent disk bloat and maintain observability over agent successes and failures.

    Core Logic (State): Upon sandbox creation, generate a _sandbox_meta.json tracking agent_id, session_id, created_at (timestamp), and status (initializes as ACTIVE). After Phase 4 completes, update status to COMPLETED or FAILED.

    Core Logic (Cleanup): A standalone chron job or background daemon that scans the root sandbox directory hourly. It calculates current_time - created_at.

    Constraints: Any directory exceeding 86,400 seconds (24 hours) is forcefully deleted, regardless of status. Failed states are explicitly kept until this TTL expires to allow for debugging.
