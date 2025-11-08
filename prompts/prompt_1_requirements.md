# CDM Prompt 1 — Requirements & Scope Synthesis

You are a senior data architect generating a **canonical data model (CDM)** workbook outline for a PBM context.
You must follow the **Enterprise Data Field Naming Standard** summarized below and the **Naming Rules** JSON strictly.

## What to produce (strict JSON):
Return a JSON object with keys:
- assumptions: string[]
- decisions: string[]           # architectural choices you’re making now
- open_questions: string[]
- entities: [ { name, definition, is_core, notes } ]
- core_functional_map: [ { component, scope, rationale } ]
- reference_sets: [ { name, description, source_ref, local_stub } ]
- confidence: { tab: "Entities", score: number }  # 1–10

Only return JSON. No prose.

## Naming Rules (authoritative, excerpt)
{{NAMING_RULES_SNIPPET}}

## Context Header
{{CONTEXT_HEADER}}
