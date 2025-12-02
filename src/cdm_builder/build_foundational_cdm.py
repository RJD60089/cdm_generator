# src/cdm_builder/build_foundational_cdm.py
"""
Step 3a: Build Foundational CDM

Generates a conceptual data model for a CDM domain using AI.
Injects ALL rationalized source files (FHIR, NCPDP, Guardrails, Glue)
to ensure CDM is derived from actual inputs, not hallucinated.

Input: AppConfig + rationalized source files
Output: Minimal CDM JSON with entities, attributes, relationships

Usage via orchestrator:
    python cdm_orchestrator.py plan  # Select step 3
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


# =============================================================================
# PROMPT BUILDER
# =============================================================================

def build_prompt(
    config: AppConfig,
    guardrails_data: Optional[Dict] = None,
    glue_data: Optional[Dict] = None,
    fhir_data: Optional[Dict] = None,
    ncpdp_data: Optional[Dict] = None
) -> str:
    """
    Build prompt to generate CDM from config and rationalized sources.
    
    Injects full rationalized JSON so AI has complete context.
    """
    
    def format_source(data: Optional[Dict], source_name: str) -> str:
        """Format source data as JSON, stripping only AI reasoning noise."""
        if not data:
            return f"No {source_name} data provided."
        
        entities = data.get('entities', data.get('rationalized_entities', []))
        if not entities:
            return f"No entities found in {source_name}."
        
        # Strip verbose AI metadata but keep everything else
        clean_entities = []
        for entity in entities:
            clean_entity = {
                'entity_name': entity.get('entity_name'),
                'description': entity.get('description'),
                'business_context': entity.get('business_context'),
                'technical_context': entity.get('technical_context'),
                'attributes': []
            }
            for attr in entity.get('attributes', []):
                clean_attr = {
                    'attribute_name': attr.get('attribute_name', attr.get('name')),
                    'description': attr.get('description'),
                    'data_type': attr.get('data_type', attr.get('type')),
                    'required': attr.get('required'),
                    'nullable': attr.get('nullable'),
                    'cardinality': attr.get('cardinality'),
                    'length': attr.get('length'),
                    'is_pii': attr.get('is_pii'),
                    'is_phi': attr.get('is_phi'),
                    'business_context': attr.get('business_context'),
                    'business_rules': attr.get('business_rules'),
                    'validation_rules': attr.get('validation_rules')
                }
                # Remove None values to reduce noise
                clean_attr = {k: v for k, v in clean_attr.items() if v is not None}
                clean_entity['attributes'].append(clean_attr)
            clean_entities.append(clean_entity)
        
        return json.dumps(clean_entities, indent=2)
    
    # Build source sections - full JSON
    guardrails_section = format_source(guardrails_data, "Guardrails")
    glue_section = format_source(glue_data, "Glue")
    fhir_section = format_source(fhir_data, "FHIR")
    ncpdp_section = format_source(ncpdp_data, "NCPDP")
    
    # Helper to get entities from either key
    def get_entities(data: Optional[Dict]) -> list:
        if not data:
            return []
        return data.get('entities', data.get('rationalized_entities', []))
    
    # Count what we have
    gr_count = len(get_entities(guardrails_data))
    glue_count = len(get_entities(glue_data))
    fhir_count = len(get_entities(fhir_data))
    ncpdp_count = len(get_entities(ncpdp_data))
    
    prompt = f"""You are a senior data architect responsible for generating a Conceptual Data Model (CDM) for the {config.cdm.domain} domain.

Your role is to synthesize the standards (FHIR, NCPDP, and any other supplied standards) and internal business structures (Guardrails, Glue, and other internal files) into a unified, normalized conceptual model that reflects real-world business entities, their attributes, and relationships.

=============================================================================
DOMAIN CONTEXT
=============================================================================

DOMAIN: {config.cdm.domain}
TYPE: {config.cdm.type}
DESCRIPTION: {config.cdm.description}

NOTE: The DOMAIN description is informational context ONLY. It is NOT a specification and MUST NOT restrict what entities, attributes,or relationships appear in the CDM. 

The CDM MUST be driven exclusively by the business entities, attributes, hierarchies, and relationships present in the supplied source files (Guardrails, Glue, FHIR, NCPDP, etc.). 

If the description omits or under-specifies important concepts, you MUST still include them based solely on what appears in the source files.

=============================================================================
CRITICAL PRINCIPLES
=============================================================================

** Do NOT create entities for event-level, message-level, or ingestion-layer representations (e.g., PlanCarrier events, PlanSponsor events, PlanOrganization events). Only include entities that represent stable business concepts across multiple records.

** The CDM MUST be derived solely from the inputs supplied below. NO hallucinated entities, attributes, or relationships.

** You MUST UNIFY equivalent attributes and entities across sources (e.g., carrier_code in Guardrails = payer_id in NCPDP when they clearly represent the same concept).

** You MUST model real business concepts that appear in the inputs, even if they are not present in standards. Internal business files represent actual business reality and MUST be reflected in the CDM.

** Reference/lookup entities should ONLY be created if they appear in the source files with actual enumerated code values. Do NOT invent reference tables. If there is no explicit reference set, prefer a VARCHAR code column on the parent entity.

** Internal business files (Guardrails, Glue, and similar) define the ACTUAL business hierarchy and entities. Standards (FHIR, NCPDP, etc.) provide semantic alignment and industry terminology. Where there is a conflict, internal files define "what exists"; standards refine "what it is called and how it behaves".

** Every distinct business entity that appears as a top-level object or recurring structure in ANY internal file (Guardrails, Glue, etc.) MUST appear as a Core entity in the CDM (unless clearly a pure technical/logging construct).

** When a standard resource (FHIR, NCPDP, etc.) appears in the supplied inputs and its attributes are well represented in the rationalized file (i.e., the file includes actual business fields, identifiers, references, or repeated structures derived from that resource), you SHOULD include the standard resource as a CDM entity. This applies only when the rationalized file contains meaningful business representation of the resource. If the resource is present but represented only by minimal or placeholder metadata, you may incorporate its attributes into existing entities instead of creating a standalone resource entity.

** If a standard resource (FHIR, NCPDP, etc.) is referenced by another resource in the inputs (e.g., via a Reference or foreign key-like field), the CDM MUST include a corresponding entity for that standard resource or explicitly map it into an existing business entity.

** If multiple sources describe the same conceptual entity under different names, you MUST unify them into a single CDM entity with a single set of attributes, not duplicate entities.

** You MUST include all business-relevant attributes that appear for a given concept in ANY of the inputs, unless the attribute is clearly technical-only (e.g., source filenames, AI metadata). Do NOT silently drop meaningful attributes.

** If the sources define a hierarchy (e.g., parent-child, group-subgroup, account-group, organization hierarchy), the CDM MUST represent each hierarchy level as its own entity with explicit relationships. Do not flatten hierarchies into a single table when multiple levels are clearly modeled in the inputs.

** When multiple standard resources represent the same concept at different technical levels (e.g., routing from NCPDP T and routing from internal files), unify them unless both levels are explicitly required for conceptual modeling.

=============================================================================
SOURCE FILES - INTERNAL BUSINESS ({gr_count + glue_count} entities)
=============================================================================

These define YOUR actual business entities and hierarchy. Use these as the PRIMARY source for identifying Core entities.

## GUARDRAILS ({gr_count} entities)
Business-defined entities from internal APIs and governance:

{guardrails_section}

## GLUE ({glue_count} entities)
Operational entities from data pipeline/warehouse:

{glue_section}

=============================================================================
SOURCE FILES - INDUSTRY STANDARDS ({fhir_count + ncpdp_count} entities)
=============================================================================

Use these for semantic alignment, attribute naming, and industry-standard terminology.

## FHIR ({fhir_count} entities)
HL7 FHIR standard resources:

{fhir_section}

## NCPDP ({ncpdp_count} entities)
NCPDP pharmacy standard fields:

{ncpdp_section}

=============================================================================
YOUR TASK
=============================================================================

Generate a unified CDM by:

1. **Identify CORE ENTITIES** from internal sources (Guardrails, Glue, and other internal files):
   - These are your actual business entities (You MUST infer the actual entities from the provided files).
   - Any business entity that appears as a top-level or recurring structure MUST be modeled as a Core entity.
   - Map these Core entities to FHIR/NCPDP (or other standards) equivalents where they exist.
   - If multiple sources describe the same concept, UNIFY into ONE entity with a coherent attribute set.

2. **Identify REFERENCE ENTITIES** only if:
   - The source files contain actual enumerated values for codes (e.g., status codes, type codes, role codes, identifier systems).
   - The reference has at least a code and human-readable display, and is used by one or more entities.
   - If the reference has 3+ attributes beyond code/name, prefer a separate Reference entity; otherwise, you may use a VARCHAR code column on the parent entity.
   - Do NOT invent reference sets that are not present in the inputs.

3. **Identify JUNCTION ENTITIES** for M:M relationships:
   - If any input shows a many-to-many relationship between entities, ALWAYS create a junction entity.
   - Junction entities MUST include foreign keys to both sides and SHOULD include effective_start_date and effective_end_date if sources show temporal validity (e.g., date ranges on assignments).
   - Junction entities MUST include audit columns (created_at, updated_at).

=============================================================================
ENTITY RULES
=============================================================================

For each entity, generate:
- entity_name: PascalCase (e.g., Carrier, GroupPlanAssignment, MemberCoverage)
- description: 1-2 sentence business description based on the inputs
- classification: Core | Reference | Junction
- attributes: List with name, type, pk, required, description
- relationships: List with to, type (1:1, 1:M, M:1), fk, description

Additional entity rules:
- If any lifecycle, status, approval, or publication status appears in the sources for that entity, you MUST include a status_code (or similarly named) attribute, and preferably link it to a Reference entity when a code set is present.
- If an entity participates in multiple relationships (e.g., appears as a parent or linkage point such as Organization, Carrier, Sponsor), it MUST include all basic business attributes available in the inputs (e.g., code, name, type, address/contact fields, or identifier fields) instead of reducing it to only foreign keys. Do NOT under-model such entities.
- If any timestamps appear for an entity in the inputs (created, updated, last_modified, etc.), you MUST include created_at and updated_at as DATETIME attributes.

=============================================================================
ATTRIBUTE RULES
=============================================================================

- Each entity MUST have a surrogate primary key: <entity_name>_id (INTEGER, pk=true, required=true).
- Use snake_case for all attribute names.
- Use standard SQL types: INTEGER, VARCHAR(n), DATE, DATETIME, BOOLEAN, DECIMAL(p,s).
- Combine duplicate/overlapping field concepts into a single CDM attribute with a clear description (e.g., multiple "name" variants can become one canonical name and one alias/marketing name, if supported by inputs).
- Move ingestion-specific or child-level attributes (such as detail_*, raw_*, or nested configuration elements) to the appropriate junction entity, not the parent entity.
- Do NOT include AI metadata (source_attribute, ai_reasoning, confidence, pruning_notes, etc.).
- Include created_at and updated_at DATETIME on ALL entities.
- If any effective or termination date appears for an entity or relationship in the source files, include BOTH effective_start_date and effective_end_date (or their domain-appropriate equivalents) on the relevant entity or junction.
- If identifiers (codes, external IDs, numbers) appear in the inputs, you MUST model them explicitly:
  - Either as attributes on the entity (e.g., entity_code, identifier_value) with clear descriptions and, where applicable, a corresponding identifier system/type.
  - Or via junction entities (e.g., EntityIdentifier with system_id + identifier_value) linked to a Reference IdentifierSystem entity if such a system list appears in the inputs.

=============================================================================
RELATIONSHIP RULES
=============================================================================

- The foreign key ALWAYS resides on the "many" side referencing the primary key of the "one" side.
- Foreign keys MUST reference the primary key of the target entity, even when the attribute names differ. Do NOT reference similarly named non-PK columns.
- In the relationships list for an entity:
  - type = M:1 means this entity has a foreign key (fk) pointing to the target entity's primary key.
  - type = 1:M describes the inverse from the parent perspective (this entity is referenced by many child records), but the actual FK still resides in the child.
- Model relationships only once from the child entity perspective; avoid redundant relationship definitions.
- For M:M relationships, create a dedicated Junction entity with FKs to both sides. Do NOT create direct M:M relationships between Core entities.
- If the sources indicate that a relationship has a validity period (e.g., start and end dates of an assignment or affiliation), place effective_start_date and effective_end_date on the junction entity (or on the child entity if that is how it is modeled).
- If the sources define roles (e.g., organization roles, plan-organization roles, participant types), you MUST model these as Reference entities (for the role codes) and use them via appropriate attributes or junction entities.

=============================================================================
NAMING RULES
=============================================================================

- Entities: PascalCase (e.g., Carrier, GroupPlanAssignment, OrganizationAffiliation)
- Attributes: snake_case (e.g., carrier_code, effective_start_date, status_code)
- Use terminology from the inputs; prefer business terms over technical or implementation-specific names (e.g., use sponsor_name instead of detail_clientname).
- When sources differ in naming for the same concept, choose the most widely recognized or neutral term and note that in the attribute description.

=============================================================================
CONSTRAINT RULES
=============================================================================
- Do NOT create entities for array-based source representations (e.g., clientorganizations, clientassociations, organizationassociations, or any nested collection that does NOT represent a first-class business entity). Only model entities that represent stable business objects reflected in multiple records.
- For FHIR resources: only include elements that represent conceptual business attributes. Do NOT flatten nested structures such as telemetry, serialized contact lists, period substructures, alias arrays, or references unless they map to a business entity.
- If multiple source files provide overlapping representations of the same concept (e.g., routing from NCPDP and routing from internal systems), unify the concept into a single CDM entity unless the business requires both to remain distinct.

=============================================================================
OUTPUT FORMAT
=============================================================================

Return ONLY valid JSON matching this structure:

{{
  "domain": "{config.cdm.domain}",
  "cdm_version": "1.0",
  "entities": [
    {{
      "entity_name": "EntityName",
      "description": "Business description from source files",
      "classification": "Core|Reference|Junction",
      "attributes": [
        {{"name": "entity_name_id", "type": "INTEGER", "pk": true, "required": true, "description": "Surrogate primary key"}},
        {{"name": "attr_name", "type": "VARCHAR(50)", "pk": false, "required": true, "description": "..."}}
      ],
      "relationships": [
        {{"to": "OtherEntity", "type": "M:1", "fk": "other_entity_id", "description": "..."}}
      ]
    }}
  ]
}}

=============================================================================
GENERATE THE CDM NOW
=============================================================================

Using ONLY the source files provided above, generate the complete {config.cdm.domain} CDM.

Before emitting the JSON, you MUST internally (without outputting it) check that:
- Every business entity from internal files and referenced standard resources is represented.
- All meaningful attributes appearing in the inputs are either present in the CDM or intentionally unified/omitted with a clear rationale.
- All many-to-many relationships have corresponding junction entities with appropriate effective dates and audit fields.
- All foreign keys are placed on the correct side and reference the primary key of the target entity.

Return ONLY the JSON. No explanation, no markdown code blocks."""

    return prompt


# =============================================================================
# FILE LOADING HELPERS
# =============================================================================

def find_latest_rationalized(outdir: Path, prefix: str) -> Optional[Path]:
    """Find the latest rationalized file matching prefix."""
    pattern = f"{prefix}*.json"
    matches = sorted(outdir.glob(pattern), reverse=True)
    return matches[0] if matches else None


def load_rationalized_file(file_path: Optional[Path]) -> Optional[Dict]:
    """Load a rationalized JSON file."""
    if not file_path or not file_path.exists():
        return None
    
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


# =============================================================================
# MAIN STEP FUNCTION
# =============================================================================

def run_step3a(
    config: AppConfig,
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False,
    rationalized_dir: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    """
    Step 3a: Generate foundational CDM from config and rationalized sources.
    
    Args:
        config: AppConfig with domain description and selected standards
        outdir: Output directory for CDM files
        llm: LLM client (None in dry run)
        dry_run: If True, save prompt without calling LLM
        rationalized_dir: Directory containing rationalized files (default: outdir/../rationalized)
    
    Returns:
        CDM dict (None in dry run)
    """
    
    print(f"\n🏗️  Building {config.cdm.domain} CDM...")
    
    # Find rationalized files
    if rationalized_dir is None:
        rationalized_dir = outdir.parent / "rationalized"
    
    domain_safe = config.cdm.domain.replace(' ', '_')
    
    # Load rationalized sources
    print(f"   📂 Loading rationalized sources from: {rationalized_dir}")
    
    guardrails_file = find_latest_rationalized(rationalized_dir, f"rationalized_guardrails_{domain_safe}")
    glue_file = find_latest_rationalized(rationalized_dir, f"rationalized_glue_{domain_safe}")
    fhir_file = find_latest_rationalized(rationalized_dir, f"rationalized_fhir_{domain_safe}")
    ncpdp_file = find_latest_rationalized(rationalized_dir, f"rationalized_ncpdp_{domain_safe}")
    
    guardrails_data = load_rationalized_file(guardrails_file)
    glue_data = load_rationalized_file(glue_file)
    fhir_data = load_rationalized_file(fhir_file)
    ncpdp_data = load_rationalized_file(ncpdp_file)
    
    # Helper to get entities from either key format
    def get_entity_count(data):
        if not data:
            return 0
        return len(data.get('entities', data.get('rationalized_entities', [])))
    
    # Report what we found
    gr_count = get_entity_count(guardrails_data)
    glue_count = get_entity_count(glue_data)
    fhir_count = get_entity_count(fhir_data)
    ncpdp_count = get_entity_count(ncpdp_data)
    
    print(f"   📊 Sources loaded:")
    print(f"      Guardrails: {gr_count} entities" + (f" ({guardrails_file.name})" if guardrails_file else " (not found)"))
    print(f"      Glue:       {glue_count} entities" + (f" ({glue_file.name})" if glue_file else " (not found)"))
    print(f"      FHIR:       {fhir_count} entities" + (f" ({fhir_file.name})" if fhir_file else " (not found)"))
    print(f"      NCPDP:      {ncpdp_count} entities" + (f" ({ncpdp_file.name})" if ncpdp_file else " (not found)"))
    
    total_entities = gr_count + glue_count + fhir_count + ncpdp_count
    if total_entities == 0:
        print(f"   ⚠️  WARNING: No rationalized sources found. Run Step 1 first.")
        if not dry_run:
            print(f"   ❌ Cannot generate CDM without source data.")
            return None
    
    # Build prompt with injected data
    prompt = build_prompt(
        config=config,
        guardrails_data=guardrails_data,
        glue_data=glue_data,
        fhir_data=fhir_data,
        ncpdp_data=ncpdp_data
    )
    
    # Ensure output directory exists
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"step3a_build_cdm_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"\n   ✓ Prompt saved: {output_file}")
        print(f"     Characters: {len(prompt):,}")
        print(f"     Tokens (est): {len(prompt) // 4:,}")
        return None
    
    # Live mode - call LLM
    print(f"\n   🤖 Calling LLM to generate CDM...")
    print(f"      Prompt size: {len(prompt):,} chars (~{len(prompt)//4:,} tokens)")
    
    messages = [
        {
            "role": "system",
            "content": "You are a senior data architect. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
        },
        {
            "role": "user",
            "content": prompt
        }
    ]
    
    response, token_usage = llm.chat(messages)
    
    # Parse response
    try:
        # Strip markdown if present
        response_clean = response.strip()
        if response_clean.startswith("```"):
            lines = response_clean.split("\n")
            if lines[0].strip().lower() in ("```json", "```"):
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
        
        cdm = json.loads(response_clean)
        
        # Validate structure
        if 'entities' not in cdm:
            raise ValueError("Response missing 'entities' key")
        
        # Add metadata
        cdm["generated_date"] = datetime.now().isoformat()
        cdm["generator"] = "step3a_build_foundational_cdm"
        cdm["model"] = llm.model if llm else "unknown"
        cdm["source_files"] = {
            "guardrails": guardrails_file.name if guardrails_file else None,
            "glue": glue_file.name if glue_file else None,
            "fhir": fhir_file.name if fhir_file else None,
            "ncpdp": ncpdp_file.name if ncpdp_file else None
        }
        
        # Save output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = config.cdm.domain.lower().replace(' ', '_')
        output_file = outdir / f"cdm_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(cdm, f, indent=2)
        
        # Report results
        entity_count = len(cdm.get('entities', []))
        attr_count = sum(len(e.get('attributes', [])) for e in cdm.get('entities', []))
        rel_count = sum(len(e.get('relationships', [])) for e in cdm.get('entities', []))
        
        # Classify entities
        core_count = sum(1 for e in cdm.get('entities', []) if e.get('classification') == 'Core')
        ref_count = sum(1 for e in cdm.get('entities', []) if e.get('classification') == 'Reference')
        junc_count = sum(1 for e in cdm.get('entities', []) if e.get('classification') == 'Junction')
        
        print(f"\n   ✅ CDM generated successfully!")
        print(f"      Entities: {entity_count} (Core: {core_count}, Reference: {ref_count}, Junction: {junc_count})")
        print(f"      Attributes: {attr_count}")
        print(f"      Relationships: {rel_count}")
        print(f"\n   📄 Saved to: {output_file}")
        
        return cdm
        
    except json.JSONDecodeError as e:
        print(f"\n   ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"   Response preview: {response[:500]}...")
        
        # Save failed response for debugging
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"step3a_error_response_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"   💾 Full response saved to: {error_file}")
        raise
        
    except ValueError as e:
        print(f"\n   ❌ ERROR: {e}")
        raise