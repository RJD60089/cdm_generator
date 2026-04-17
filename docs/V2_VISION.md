# CDM Workbench v2 - Design Vision

## Purpose of This Document

This document initiates a new Claude Code session to design and build **CDM Workbench v2**, a re-architected version of the current `cdm_generator` (v1). v1 is a working sequential Python CLI that generates Canonical Data Models from multiple source types. v2 should be a true agentic, event-driven, cloud-native platform with a web UI.

Start the v2 session by reading this document in full, then lead the user through a design conversation before writing any code.

---

## v1 Summary (What Exists Today)

**Repo:** `rjd60089/cdm_generator`
**Entrypoint:** `cdm_orchestrator.py` (Python CLI, interactive prompts)
**Flow:** Sequential steps 0-6

| Step | Purpose |
|------|---------|
| 0 | Config generation — prompts user for source files, ancillary types (driver/refiner/mapper), processing modes |
| 1 | Rationalization — converts raw source files (FHIR IGs, NCPDP, Guardrails XLSX, Glue, EDW DDL, Ancillary XLSX/JSON/DDL) into standard entity/attribute JSON via LLM |
| 2 | Build Foundational CDM — merges all rationalized sources into a unified CDM via a single large LLM call |
| 3 | Consolidation — LLM-driven entity consolidation (merge overlaps) with interactive approval |
| 4 | PK/FK Validation — LLM validates keys and relationships, interactive approval of fixes |
| 5 | Build Full CDM — generates per-source match files (LLM), applies them to populate source_lineage, generates gap report |
| 5p | Post-processing — rematch, sensitivity/PHI flagging, CDE identification, field code enrichment |
| 6 | Artifacts — generates Excel workbook, Word doc, DDL, LucidChart CSV from Full CDM JSON |

**LLM:** OpenAI gpt-5.4 (400K context, 922K prompt limit)

---

## v1 Pain Points (Things v2 Must Solve)

### 1. Monolithic Sequential Execution
- Each step blocks. A single run takes 2-4 hours for a mid-size CDM.
- Rationalization is embarrassingly parallel (FHIR, NCPDP, Guardrails, Glue, EDW, each ancillary) but runs sequentially.
- No progress persistence — a crash means restarting the current step.

### 2. Massive Prompt Sizes
- Foundational CDM prompt can hit 200K+ tokens because all rationalized sources are dumped into one prompt.
- Match generation sends entire CDM + entire source to LLM per entity.
- We've hit the 922K hard limit multiple times (e.g., a 3,000-row XLSX sheet).
- Workarounds: chunking, sheet-splitting, row-batching — all fragile.

### 3. Human-in-the-Loop Too Often
- Consolidation, PK/FK validation, refiner gate, per-source mapping choices — all require interactive Y/N prompts.
- Low-confidence decisions should auto-flag for end-of-run review, not halt execution.
- High-confidence decisions should auto-approve.

### 4. Single Generic "Data Architect" Prompt
- One prompt tries to handle FHIR semantics, NCPDP pharmacy terminology, EDW relational patterns, business domain rules, naming conventions — all at once.
- Leads to compromises: the prompt is so long it's brittle; specialists would produce better output.

### 5. Fragile Data Shapes
- LLM sometimes returns `name: "Child_to_Parent"` instead of `to/fk/type/description` for relationships (had to add explicit format spec).
- Attributes sometimes missing expected keys (`business_rules`, `validation_rules`) causing KeyErrors.
- Case sensitivity issues in entity/attribute matching.

### 6. Local-Only, No Collaboration
- CLI-only, runs on a developer's laptop.
- No multi-user access, no history, no review queue, no audit trail.
- Input files scattered on the C drive, gitignored so they don't transfer between machines.

### 7. No Observability
- Log output to stdout, no metrics, no tracing.
- Hard to answer: "why did this CDM end up with 19 entities instead of 29?"
- Token usage printed but not aggregated or billed.

---

## v2 Architectural Vision

### 1. Event Bus Core
- Kafka, NATS, or cloud-native equivalent (EventBridge, Pub/Sub).
- All pipeline steps become workers that consume and emit events.
- Master orchestrator agent publishes work events, listens for completion, decides next steps.
- Event log = audit log for free.

### 2. Specialist Agents
Replace the single "data architect" prompt with multiple specialists, each with its own tuned prompt and model choice:
- **FHIR Specialist** — deep HL7 knowledge
- **NCPDP Specialist** — pharmacy transaction terminology
- **EDW/Schema Specialist** — relational patterns, FK inference
- **Business Domain Specialist** — PBM-specific concepts (finance, claims, eligibility, etc.)
- **Relationship Validator** — ensures correct cardinality, FK placement, schema integrity
- **Naming Conventions Enforcer** — PascalCase/snake_case, standard terms
- **Decision Agent** — reviews ambiguous outputs and auto-approves or flags for humans

Each specialist subscribes to its topic; master agent routes work.

### 3. MCP for Source Access
Instead of stuffing 200K tokens of source data into every prompt, expose sources as MCP servers:
- `mcp://cdm-sources/fhir` — query FHIR entities, attributes, value sets
- `mcp://cdm-sources/ncpdp` — query NCPDP fields
- `mcp://cdm-sources/edw` — query EDW tables, columns
- `mcp://cdm-sources/ancillary/<source_id>` — per-ancillary source
- `mcp://cdm-current` — query in-progress CDM (entities, attributes, lineage)

Agents pull only what they need for the current decision. Prompts shrink dramatically.

### 4. Parallel Rationalization
All rationalizers run concurrently. Each source type is an independent worker that publishes completion events.

### 5. Web Front-End
- React + FastAPI (or similar).
- Features:
  - Upload/configure sources
  - Select CDM domain + processing modes
  - Kick off runs
  - Live pipeline view (which stage, what's running, token spend)
  - Review queue for flagged decisions
  - Run history & diff between runs
  - Download artifacts
- Runs are background jobs; the UI is a thin client over the event bus.

### 6. Cloud-Native
- Containerize everything (Docker).
- Deploy to Kubernetes or a managed platform (ECS, Cloud Run).
- Source files in object storage (S3, Azure Blob).
- Secrets management (Key Vault, AWS Secrets Manager).
- No hardcoded Windows paths.

### 7. Persistence & Resumability
- Each pipeline state change persisted (Postgres or event-sourced store).
- A run can resume from any checkpoint after a crash.
- Every LLM call, every decision, every source query logged with context.

### 8. Agentic Decisions
- Human interactive prompts replaced by decision agents.
- Decision agent gets the LLM output + context, auto-approves high-confidence, flags ambiguous for async human review.
- Human reviews via web UI at their leisure; agents wait on the "human decision" event.

---

## Key Decisions to Make in the v2 Kickoff

1. **New repo or monorepo?** (Recommendation: new repo `cdm_workbench_v2`, leave v1 untouched as reference.)
2. **Language(s).** Python for AI/orchestrator worker is natural. TypeScript/React for web UI. Consider Go for high-throughput event workers if needed.
3. **Event bus.** Local dev: NATS (simple). Production: Kafka, or a cloud provider's native bus if targeting one cloud.
4. **Agent framework.** Custom? Anthropic Agent SDK? LangGraph? AutoGen? Each has trade-offs — pick based on how much orchestration logic you want to own.
5. **MCP implementation.** Python MCP SDK for the source servers; agents consume via standard MCP client.
6. **LLM strategy.** Still OpenAI gpt-5.4? Claude for specialists where it excels? Mix per specialist? Budget/latency trade-offs.
7. **Storage.** Postgres for run history and CDM state. Object storage for source files and artifacts. Redis/Valkey for hot caching.
8. **Deployment target.** AWS? Azure? GCP? Kubernetes self-managed?
9. **Migration strategy.** Big bang rewrite vs. incremental — can v2 read v1's rationalized files so you don't redo Step 1?
10. **Minimum viable v2.** What's the smallest thing that demonstrates the new architecture (one specialist + event bus + web UI + one source type)?

---

## Agenda for the v2 Kickoff Session

1. Read this document; confirm or correct the problem statement.
2. Answer the "Key Decisions" above.
3. Produce an architecture diagram (ASCII or Mermaid in the repo).
4. Draft a phased delivery plan (phase 1 = thin vertical slice, phase 2+ = add specialists, sources, features).
5. Decide what's in scope for a first working prototype (end-to-end, one source, one specialist, one web UI page).
6. Create the new repo scaffold with folder structure, README, CI pipeline, local dev setup.

---

## Pointers to v1 Code (Useful Reference)

The v1 repo (`rjd60089/cdm_generator`) contains lessons and prompts worth carrying forward:

- `src/cdm_builder/build_foundational_cdm.py` — main CDM generation prompt (tuned, sensitive)
- `src/rationalizers/rationalize_*.py` — per-source rationalization patterns
- `src/cdm_full/match_generator.py` — per-entity mapping prompt
- `src/cdm_full/match_applier.py` — apply match files to populate source_lineage
- `src/config/config_gen_*.py` — config generation per source type
- `src/artifacts/excel/` — Excel artifact generation (Cross-Reference, Data Dictionary, etc.)

Do not port these files directly — extract the prompt content, the data shapes, and the business logic, then re-implement in the v2 architecture.

---

## Context for the New Session

Working with Ray (user `RJD60089`) on CDM generation for a pass-through PBM. Current focus is finance, claims, eligibility, plan, benefit, formulary, drug, pharmacy, prescriber, and UM CDMs. v1 has successfully produced several of these. v2 is about scaling the capability, improving quality, and enabling collaboration.

Work on a feature branch in the new repo. Do not push to main without explicit approval. Confirm architectural decisions before writing code.
