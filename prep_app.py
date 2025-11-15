"""
Prep App - Input File Rationalization
Rationalizes entities and attributes WITHIN each file type for CDM generation.
Run this once when inputs change, then use app_cdm_gen.py for CDM generation.

Supports multiple models: GPT-5, GPT-4.1, and local models (70B, 33B, 8B).
"""
import argparse
import sys
import json
import os
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from src.config import load_config
from src.converters import (
    convert_fhir_to_json,
    convert_guardrails_to_json,
    convert_ddl_to_json,    
    convert_glue_to_json,
    convert_naming_standard_to_json
)
from src.core.llm_client import LLMClient
load_dotenv()
from src.core.model_selector import (
    MODEL_OPTIONS,
    select_model,
    get_model_config,
    prompt_user
)


def count_tokens(text: str) -> int:
    """Rough token count estimate (4 chars per token)"""
    return len(text) // 4


def save_prompt_to_file(prompt: str, filename: str, prompts_dir: Path) -> dict:
    """Save prompt to file and return stats"""
    output_file = prompts_dir / filename
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    char_count = len(prompt)
    token_count = count_tokens(prompt)
    
    return {
        'file': str(output_file),
        'characters': char_count,
        'tokens_estimate': token_count
    }


def build_fhir_rationalization_prompt(domain: str, fhir_files: list) -> tuple[str, list]:
    """Build FHIR rationalization prompt with full detail capture"""
    # Convert files to JSON
    fhir_json = []
    for fhir_file in fhir_files:
        fhir_json.append({
            'filename': Path(fhir_file).name,
            'content': convert_fhir_to_json(fhir_file)
        })
    
    # Build prompt
    prompt = f"""You are a FHIR expert rationalizing multiple FHIR resource profiles for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the {len(fhir_files)} FHIR profile files provided and rationalize them into a unified set of entities and attributes with FULL business context.

**Rationalization Goals:**
1. Identify all unique entities across all FHIR resources
2. Consolidate duplicate or overlapping attributes
3. Resolve conflicts between different FHIR resources
4. **Preserve ALL business context** (definitions, comments, requirements, constraints)
5. Create a clean, unified entity/attribute structure with rich descriptions

**CRITICAL - Capture Full FHIR Metadata:**

For each attribute, extract from FHIR StructureDefinition elements:
- **short**: Brief title/label
- **definition**: Full technical definition
- **comment**: Implementation guidance and business notes
- **requirements**: Why this field exists (business purpose)
- **constraint**: Validation rules and invariants
- **binding**: Value set information (if applicable)
- **mustSupport**: Whether implementations must support this element

This rich context is essential for:
- Accurate duplicate detection across sources in Step 2
- Understanding business intent and use cases
- Capturing implementation patterns and best practices
- Generating complete business descriptions in final CDM

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):

{{
  "domain": "{domain}",
  "source": "fhir",
  "rationalized_entities": [
    {{
      "entity_name": "Coverage",
      "short_description": "Insurance coverage",
      "description": "Detailed description of what this entity represents in PBM context",
      "source_resources": ["Coverage"],
      "attributes": [
        {{
          "attribute_name": "coverage_identifier",
          "fhir_path": "Coverage.identifier",
          "data_type": "Identifier",
          "cardinality": "0..*",
          "required": false,
          "must_support": true,
          
          "short_description": "Business identifier(s) for this coverage",
          "definition": "The identifier of the coverage as issued by the insurer.",
          "comment": "The main (and possibly only) identifier for the coverage - often referred to as a Member Id, Certificate number, Personal Health Number or Case ID. May be constructed as the concatenation of the Coverage.SubscriberID and the Coverage.dependent. Note that not all insurers issue unique member IDs therefore searches may result in multiple responses.",
          "requirements": "Allows coverages to be distinguished and referenced.",
          
          "constraints": [
            "ele-1: All FHIR elements must have a @value or children"
          ],
          "binding": null,
          
          "source_files": ["coverage.profile.json"]
        }},
        {{
          "attribute_name": "status",
          "fhir_path": "Coverage.status",
          "data_type": "code",
          "cardinality": "1..1",
          "required": true,
          
          "short_description": "Coverage status",
          "definition": "The status of the resource instance.",
          "comment": "This element is labeled as a modifier because the status contains codes that mark the resource as not currently valid.",
          "requirements": "Need to track the status of the resource as 'draft' resources may undergo further edits while 'active' resources are immutable and may only be retired.",
          
          "constraints": [],
          "binding": {{
            "strength": "required",
            "valueSet": "http://hl7.org/fhir/ValueSet/fm-status",
            "description": "A code specifying the state of the resource instance."
          }},
          
          "possible_values": ["active", "cancelled", "draft", "entered-in-error"],
          
          "source_files": ["coverage.profile.json"]
        }}
      ],
      "relationships": [
        {{
          "related_entity": "Patient",
          "relationship_type": "many-to-one",
          "fhir_reference": "Coverage.beneficiary",
          "description": "The party who benefits from the insurance coverage"
        }}
      ]
    }}
  ]
}}

**Field Mapping from FHIR StructureDefinition:**

When processing FHIR StructureDefinition snapshot elements:
1. `element.short` → `short_description`
2. `element.definition` → `definition`
3. `element.comment` → `comment`
4. `element.requirements` → `requirements`
5. `element.constraint` → `constraints` (array)
6. `element.binding` → `binding` (object with strength, valueSet, description)
7. `element.mustSupport` → `must_support` (boolean)
8. `element.min` and `element.max` → `cardinality` (e.g., "0..1", "1..1", "0..*")
9. `element.min > 0` → `required` (true/false)

**Handling Missing Fields:**
- If a field doesn't exist in FHIR, set to null (not omit)
- If comment/requirements/constraints are empty, set to null or []
- Always include short_description, definition at minimum

**For Nested/Complex Types:**
- If an element has sub-elements (e.g., Coverage.class), create a separate entity
- Link via relationships
- Preserve the hierarchical structure

**CRITICAL:** 
- Output ONLY valid JSON
- Include ALL textual fields from FHIR (short, definition, comment, requirements)
- Rationalize conflicts (don't duplicate), but preserve all context
- Focus on PBM passthrough model needs
- Never truncate definition, comment, or requirements text

---

## FHIR Profile Files

"""
    
    for i, fhir_data in enumerate(fhir_json, 1):
        prompt += f"### FHIR File {i}: {fhir_data['filename']}\n\n```json\n{fhir_data['content']}\n```\n\n"
    
    prompt += """
---

**Remember:** Extract and preserve ALL business context from FHIR definitions. This information is critical for downstream CDM generation and will prevent information loss.

Generate the rationalized JSON now.
"""
    
    return prompt, fhir_json


def build_guardrails_rationalization_prompt(domain: str, gr_files: list) -> tuple[str, list]:
    """Build Guardrails rationalization prompt with data governance and calculated field context"""
    # Convert files to JSON
    gr_json = []
    for gr_file in gr_files:
        gr_json.append({
            'filename': Path(gr_file).name,
            'content': convert_guardrails_to_json(gr_file)
        })
    
    # Build prompt
    prompt = f"""You are a business analyst rationalizing multiple API specifications for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the {len(gr_files)} Guardrails specification files and rationalize them into a unified set of business entities and attributes with complete data governance.

**Rationalization Goals:**
1. Identify all unique business entities across all API specifications
2. Consolidate duplicate or overlapping attributes across different APIs
3. Resolve conflicts between API versions and specifications
4. **Preserve business rules, validation requirements, AND data governance metadata**
5. **Capture calculated field information and API request/response context**
6. Create a clean, unified business entity/attribute structure

**CRITICAL - Enhanced Metadata Capture:**

From the Excel columns in Guardrails files, extract:

**Core Attribute Information:**
- Entity Name (table/object)
- Field Name (column/attribute)
- Logical Data Type
- Allow Null (Y/N) → convert to required and allow_null booleans

**Data Governance (CRITICAL for compliance):**
- **Data Classification** (Internal, Confidential, Restricted, etc.)
- **PII (Y/N)** → is_pii boolean - Personally Identifiable Information flag
- **PHI (Y/N)** → is_phi boolean - Protected Health Information flag (HIPAA)
- These are MANDATORY for data governance and must be captured when present

**Calculated Field Information:**
- **Calculated Field (Y/N)** → is_calculated boolean
- **Calculation Dependency** → what other fields this depends on
- This distinguishes derived fields from source fields

**Business Context:**
- Description (synthesize from any available description/definition columns)
- Business rules (entity-level constraints ONLY - not field-level constraints)
- Validation rules (field-level constraints and formats)

**Source File Tracking (CRITICAL - MANDATORY):**

For EVERY attribute you extract, you MUST track which Guardrails file AND tab it comes from:

1. **Check each file::tab provided below** - as you process each sheet in each Guardrails file, note which attributes it contains
2. **Track the filename::tabname** - when you extract an attribute, record the EXACT "filename::tabname" in source_files array
3. **List ALL file::tab combinations** - if the same attribute appears in multiple tabs or files, list ALL of them

**Format:** "filename.xlsx::TabName"

**Examples:**
- Attribute in 1 tab only: 
  source_files: ["GR_File_1.xlsx::V1 Handler Copay"]
  
- Attribute in 2 tabs same file: 
  source_files: ["GR_File_1.xlsx::V1 Handler Copay", "GR_File_1.xlsx::V1 Handler Rate"]
  
- Attribute in multiple files/tabs: 
  source_files: [
    "GR_File_1.xlsx::V1 Handler Copay",
    "GR_File_2.xlsx::Hierarchy",
    "GR_File_3.xlsx::Plan"
  ]

**When consolidating similar attributes:**
- If you merge "client_code" from File1::TabA and "clientCode" from File2::TabB into one attribute
- Then source_files: ["GR_File_1.xlsx::TabA", "GR_File_2.xlsx::TabB"]

**Important:**
- Even if attribute definition is identical across tabs/files, LIST ALL file::tab combinations where it appears
- source_files is at ATTRIBUTE level ONLY (do NOT add at entity level)
- Use exact tab names as shown in the file data below
- This is MANDATORY for complete provenance tracking

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):

{{
  "domain": "{domain}",
  "source": "guardrails",
  "rationalized_entities": [
    {{
      "entity_name": "EntityA",
      "description": "Business entity representing [concept] in the domain",
      "business_purpose": "Defines [business purpose and role]",
      
      "attributes": [
        {{
          "attribute_name": "entitya_id",
          "data_type": "string",
          "required": true,
          "allow_null": false,
          
          "description": "Unique identifier for the entity",
          "business_context": "Primary identifier used across all systems",
          
          "is_calculated": false,
          "calculation_dependency": null,
          
          "data_classification": "Internal",
          "is_pii": false,
          "is_phi": false,
          
          "validation_rules": [
            "Required",
            "Must be unique",
            "Alphanumeric only"
          ],
          
          "business_rules": [
            {{
              "rule_type": "uniqueness",
              "description": "Must be unique across the enterprise",
              "enforcement": "Database constraint + API validation"
            }}
          ],
          
          "source_files": ["GR_File_1.xlsx::V1 Handler Copay", "GR_File_2.xlsx::Hierarchy"]
        }},
        {{
          "attribute_name": "entitya_name",
          "data_type": "string",
          "required": true,
          "allow_null": false,
          
          "description": "Name of the entity",
          "business_context": "Used for identification and display",
          
          "is_calculated": false,
          "calculation_dependency": null,
          
          "data_classification": "Confidential",
          "is_pii": true,
          "is_phi": true,
          
          "validation_rules": [
            "Required",
            "Max length 200 characters"
          ],
          
          "business_rules": [],
          
          "source_files": ["GR_File_1.xlsx::Plan"]
        }},
        {{
          "attribute_name": "calculated_amount",
          "data_type": "decimal",
          "required": false,
          "allow_null": true,
          
          "description": "Calculated amount for the transaction",
          "business_context": "Final amount after applying all business rules",
          
          "is_calculated": true,
          "calculation_dependency": "base_amount + adjustment - discount",
          
          "data_classification": "Internal",
          "is_pii": false,
          "is_phi": false,
          
          "validation_rules": [
            "Must be >= 0 if present",
            "Maximum 2 decimal places"
          ],
          
          "business_rules": [
            {{
              "rule_type": "conditional_constraint",
              "condition": "entity_type = 'premium'",
              "constraint": "calculated_amount <= 10000",
              "description": "Premium type amounts cannot exceed $10,000",
              "enforcement": "API validation"
            }}
          ],
          
          "source_files": ["GR_File_2.xlsx::V1 Handler Rate", "GR_File_n.xlsx::Benefit"]
        }}
      ],
      "relationships": [
        {{
          "related_entity": "EntityB",
          "relationship_type": "many-to-one",
          "foreign_key": "entityb_code",
          "description": "EntityA belongs to EntityB"
        }}
      ]
    }}
  ]
}}

**Field Extraction Guidelines:**

1. **Entity Name**: Use the entity/table name from Excel. Normalize naming (e.g., "HandlerCopay" or "handler_copay")

2. **Description**: Synthesize from any description/definition columns. Keep concise but meaningful.

3. **Required**: 
   - If "Allow Null" = "N" → required: true, allow_null: false
   - If "Allow Null" = "Y" → required: false, allow_null: true

4. **Calculated Field**:
   - If "Calculated Field (Y/N)" = "Y" → is_calculated: true
   - If "Y" and "Calculation Dependency" has value → capture the dependency
   - If "N" or empty → is_calculated: false, calculation_dependency: null

5. **Data Governance** (CRITICAL):
   - **data_classification**: Extract from "Data Classification" column (Internal, Confidential, Restricted, etc.)
   - **is_pii**: Convert "PII (Y/N)" column to boolean (Y→true, N→false, empty→false)
   - **is_phi**: Convert "PHI (Y/N)" column to boolean (Y→true, N→false, empty→false)
   - These are MANDATORY for compliance - always include even if false/null

6. **Source Files** (CRITICAL):
   - **At attribute level ONLY**: Track which specific Excel file(s) this attribute comes from
   - If attribute appears in multiple Excel files, list ALL of them
   - This ensures complete provenance tracking

7. **Validation Rules**: Synthesize from:
   - "Allow Null" → "Required" if N
   - Any format notes or constraints mentioned
   - Range or pattern requirements
   - These are STRUCTURAL/FORMAT constraints

8. **Business Rules** (at attribute level):
   - **rule_type**: Type of business rule (conditional_constraint, range_limit, dependency, etc.)
   - **condition**: IF condition (e.g., "claim_type = 'specialty'")
   - **constraint**: THEN constraint (e.g., "max_payment <= 50000")
   - **description**: Plain English explanation
   - **enforcement**: How enforced (API validation, database trigger, etc.)
   - These are BUSINESS LOGIC constraints, not structural
   - Example: "If entity_type is 'premium', amount cannot exceed $10,000"
   - Empty array [] if no business rules for attribute

**Handling Multiple Sheets:**
- Each Excel file may have multiple sheets (e.g., "V1 Handler Copay", "V1 Handler Dispense Fee")
- Each sheet typically represents a different entity or API operation
- Rationalize across sheets and files to consolidate similar entities

**Handling Missing Data:**
- If a field is not present in the Excel, set to null (don't omit)
- If "Data Classification" column is empty → data_classification: null
- If PII/PHI columns don't exist → is_pii: false, is_phi: false
- Always include the governance fields even if false/null

**CRITICAL:**
- Output ONLY valid JSON
- This is HEAVY rationalization - conflicting definitions need resolution
- **Data governance fields (data_classification, is_pii, is_phi) are MANDATORY - always include them**
- Calculated field information is important for distinguishing source vs derived data
- Focus on business perspective (not just technical schema)

---

## Guardrails Files

"""
    
    for i, gr_data in enumerate(gr_json, 1):
        prompt += f"### Guardrails File {i}: {gr_data['filename']}\n\n```json\n{gr_data['content']}\n```\n\n"
    
    prompt += """
---

**Remember:** 
1. **Data governance metadata (PII/PHI) is CRITICAL** - always capture when available
2. **Source files tracking is MANDATORY** - use "filename.xlsx::TabName" format for every attribute
3. **During consolidation, track provenance** - if attribute appears in 3 file::tab combinations, source_files must have 3 entries
4. **Business rules at ATTRIBUTE level** - not entity level (no entity business_rules field)
5. **Validation rules vs Business rules** - validation is structural, business rules are conditional logic
6. Calculated field info distinguishes source data from derived data
7. Rationalize across files to consolidate similar entities
8. Output ONLY valid JSON

Generate the rationalized JSON now.
"""
    return prompt, gr_json



def build_ddl_rationalization_prompt(domain: str, ddl_files: list) -> tuple[str, list]:
    """
    Build DDL rationalization prompt for consolidated AWS Glue columns.
    
    NOTE: This now uses convert_glue_to_json() which consolidates columns
    across all Glue jobs, showing which jobs each column appears in.
    """
    # Import at function level to avoid circular dependency
    from src.converters import convert_glue_to_json
    
    # Convert files to consolidated JSON
    glue_json = []
    for glue_file in ddl_files:
        glue_json.append({
            'filename': Path(glue_file).name,
            'content': convert_glue_to_json(glue_file)  # Uses new converter
        })
    
    # Build prompt
    prompt = f"""You are a database architect rationalizing AWS Glue columns for a PBM CDM.

**Domain:** {domain}

**CRITICAL CONTEXT:**
The input has been PRE-CONSOLIDATED. Each column shows:
- **Name**: Column name
- **Type**: Data type
- **GJSources**: List of ALL Glue jobs where this column appears

This means columns that appear in multiple Glue jobs have already been consolidated.
You don't need to find duplicates - they're already grouped.

**Your Task:**
Create rationalized entities and attributes from the consolidated columns:

1. **Group related columns into logical business entities**
   - Use column name patterns (e.g., detail_client_*, detail_account_*)
   - Consider business concepts (client, account, plan, benefit, etc.)
   - Don't over-rationalize - keep related columns together

2. **Track ALL Glue job sources for each attribute**
   - Use the GJSources array to populate source_tables
   - Format: "DatabaseName.GlueJobName.ColumnName"
   - If a column appears in 3 Glue jobs, source_tables should have 3 entries

3. **Infer logical primary keys** 
   - Mark as "inferred": true (event data has no actual PKs)
   - Look for columns with "_id" suffix matching entity name

4. **Infer relationships between entities**
   - Mark as "inferred": true (no FKs in Glue)
   - Look for foreign key patterns (e.g., client_id in account entity)

5. **Handle array columns properly**
   - Columns like "resources[0]" represent arrays
   - Don't split indices into separate attributes
   - Rationalize as single array attribute "resources"

**CRITICAL - DO NOT OVER-RATIONALIZE:**
- **Keep ALL columns** - don't drop any
- **Don't create bridge/junction entities** unnecessarily
- **Include event envelope fields** (version, id, detail-type, source, account, time, region)
- These are metadata but important for event processing
- Goal: preserve all source data while normalizing naming

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):

{{
  "domain": "{domain}",
  "source": "ddl_glue",
  "database_name": "/path/from/input",
  "rationalized_entities": [
    {{
      "entity_name": "Client",
      "description": "Client organization in the benefits plan hierarchy",
      
      "glue_sources": [
        "source_navitus_bpm_client_event",
        "source_navitus_bpm_account_event"
      ],
      
      "attributes": [
        {{
          "attribute_name": "client_id",
          "data_type": "int",
          "nullable": false,
          "nullable_inferred": true,
          "primary_key": true,
          "primary_key_inferred": true,
          "description": "Unique identifier for the client organization",
          
          "source_tables": [
            "/navitus/bpm/benefitsplanmanagement-analytics.source_navitus_bpm_client_event.detail_clientid",
            "/navitus/bpm/benefitsplanmanagement-analytics.source_navitus_bpm_account_event.detail_clientid"
          ]
        }},
        {{
          "attribute_name": "client_name",
          "data_type": "string",
          "nullable": true,
          "nullable_inferred": true,
          "primary_key": false,
          "primary_key_inferred": false,
          "description": "Name of the client organization",
          
          "source_tables": [
            "/navitus/bpm/benefitsplanmanagement-analytics.source_navitus_bpm_client_event.detail_name"
          ]
        }},
        {{
          "attribute_name": "effective_date",
          "data_type": "date",
          "nullable": true,
          "nullable_inferred": true,
          "primary_key": false,
          "primary_key_inferred": false,
          "description": "Date when client becomes effective",
          
          "source_tables": [
            "/navitus/bpm/benefitsplanmanagement-analytics.source_navitus_bpm_client_event.detail_effectivedate"
          ]
        }}
      ],
      
      "keys": [
        {{
          "key_type": "primary",
          "key_name": "PK_client",
          "columns": ["client_id"],
          "inferred": true
        }}
      ],
      
      "relationships": [
        {{
          "related_entity": "Account",
          "relationship_type": "one-to-many",
          "description": "Client has multiple accounts",
          "foreign_key_attribute": "client_id",
          "inferred": true
        }}
      ]
    }}
  ]
}}

**Field Extraction Guidelines:**

1. **Entity Name**: 
   - Derive from column name patterns
   - Remove prefixes: detail_, detail_mutation_, etc.
   - Group by business concept (client, account, carrier, plan, etc.)
   - Examples:
     * detail_clientid, detail_clientname → "Client" entity
     * detail_accountid, detail_accountname → "Account" entity
     * detail_planid, detail_planname → "Plan" entity

2. **Attribute Name**:
   - Clean up column names (remove detail_, normalize casing)
   - Use consistent naming (e.g., _id suffix for identifiers)
   - Keep semantic meaning clear

3. **Data Type**:
   - Map from Glue types to logical types
   - string → string (or varchar if length known)
   - int → int
   - bigint → bigint
   - double → decimal
   - timestamp → datetime
   - date → date
   - boolean → boolean
   - array<T> → array<T>
   - struct<...> → object or JSON

4. **nullable (INFERRED)**:
   - Always set nullable_inferred: true
   - Heuristics:
     * Fields with "_id" in name → nullable: false
     * Fields with "name" in name → nullable: true
     * Date fields → nullable: true
     * Status fields → nullable: false

5. **primary_key (INFERRED)**:
   - Always set primary_key_inferred: true
   - Heuristics:
     * Field name matches entity name + "_id" → primary_key: true
     * Example: client_id in Client entity → true
     * Multiple PKs possible (composite keys)

6. **description (GENERATED)**:
   - Generate based on column name and business context
   - Keep concise (1-2 sentences)
   - Focus on business meaning in PBM domain

7. **source_tables (CRITICAL)**:
   - Format: "DatabaseName.GlueJobName.ColumnName"
   - Use GJSources array from input to build this
   - If column appears in 3 jobs, list all 3 paths
   - Example:
     * Input GJSources: ["job1", "job2", "job3"]
     * Column: detail_clientid
     * Output source_tables: [
         "/navitus/bpm/db.job1.detail_clientid",
         "/navitus/bpm/db.job2.detail_clientid",
         "/navitus/bpm/db.job3.detail_clientid"
       ]

**Source Tracking (CRITICAL):**
- **Entity level**: glue_sources lists ALL Glue jobs that contribute any attributes
- **Attribute level**: source_tables lists ALL DatabaseName.GlueJob.Column paths
- This ensures complete provenance tracking across all event sources

**Relationship Inference:**
- Look for foreign key patterns in column names
- Example: client_id in Account entity → FK to Client
- Mark all relationships as "inferred": true
- Include foreign_key_attribute to show which column is the FK

**Array Column Handling:**
- Input may have: resources[0], resources[1], resources[2]
- Rationalize to single: resources (type: array<string>)
- Note in description that it's an array

**Event Envelope Fields:**
Keep these standard AWS event fields:
- version (string)
- id (string) - event ID
- detail-type (string)
- source (string)
- account (string)
- time (string/datetime)
- region (string)
- resources (array<string>)

These can be grouped into an "EventMetadata" entity or kept with each domain entity.

**CRITICAL REQUIREMENTS:**

1. **ALWAYS set _inferred flags** - transparency about data provenance
2. **Use consolidated GJSources** - don't duplicate work, use what's given
3. **Generate complete source_tables paths** - DatabaseName.GlueJob.Column format
4. **Keep ALL columns** - don't drop any from source
5. **Output ONLY valid JSON** - no markdown, no code blocks
6. **Don't over-normalize** - preserve event structure where appropriate

---

## Consolidated Glue Files

"""
    
    for i, glue_data in enumerate(glue_json, 1):
        prompt += f"### Glue File {i}: {glue_data['filename']}\n\n```json\n{glue_data['content']}\n```\n\n"
    
    prompt += """
---

**Remember:** 
1. Input is PRE-CONSOLIDATED - columns already grouped by name+type with GJSources
2. Use GJSources array to build complete source_tables paths
3. Format: "DatabaseName.GlueJobName.ColumnName"
4. Don't over-rationalize - keep event structure where appropriate
5. Mark everything as inferred (no actual PKs/FKs in Glue)
6. Include ALL columns - don't drop any
7. Output ONLY valid JSON

Generate the rationalized JSON now.
"""
    
    return prompt, glue_json


def build_naming_rationalization_prompt(domain: str, naming_files: list) -> tuple[str, list]:
    """Build Naming Standards rationalization prompt"""
    # Convert files to JSON
    naming_json = []
    for naming_file in naming_files:
        naming_json.append({
            'filename': Path(naming_file).name,
            'content': convert_naming_standard_to_json(naming_file)
        })
    
    # Build prompt
    prompt = f"""You are a data governance specialist rationalizing naming standards for a PBM CDM.

**Domain:** {domain}

**Your Task:**
Analyze the {len(naming_files)} naming standard files and rationalize them into a unified set of naming conventions and rules.

**Rationalization Goals:**
1. Identify all naming rules and conventions
2. Consolidate duplicate or overlapping rules
3. Resolve conflicts between different standards (note conflicts for user review if needed)
4. Create a clean, unified naming standard

**Output Format:**
Return ONLY valid JSON (no markdown, no code blocks):
```json
{{
  "domain": "{domain}",
  "source": "naming_standards",
  "rationalized_rules": [
    {{
      "rule_category": "field_naming",
      "rule": "Use snake_case for all field names",
      "examples": ["plan_id", "member_first_name", "effective_date"],
      "source_files": ["Standard_2024.xlsx"]
    }},
    {{
      "rule_category": "data_types",
      "rule": "Use VARCHAR for text fields, specify max length",
      "examples": ["VARCHAR(50)", "VARCHAR(200)"],
      "source_files": ["Standard_2024.xlsx"]
    }},
    {{
      "rule_category": "prefixes",
      "rule": "Use 'is_' prefix for boolean fields",
      "examples": ["is_active", "is_primary", "is_deleted"],
      "source_files": ["Standard_2024.xlsx"]
    }}
  ],
  "rationalized_patterns": [
    {{
      "pattern_name": "identifier_fields",
      "pattern": "{{entity_name}}_id",
      "data_type": "VARCHAR(50)",
      "examples": ["plan_id", "member_id", "claim_id"]
    }},
    {{
      "pattern_name": "date_fields",
      "pattern": "{{context}}_date",
      "data_type": "DATE",
      "examples": ["effective_date", "termination_date", "created_date"]
    }}
  ]
}}
```

**CRITICAL:**
- Output ONLY valid JSON
- Resolve conflicts where possible
- If standards directly conflict, note for user review
- Focus on consistency and clarity

---

## Naming Standard Files

"""
    
    for i, naming_data in enumerate(naming_json, 1):
        prompt += f"### Naming Standard File {i}: {naming_data['filename']}\n\n```json\n{naming_data['content']}\n```\n\n"
    
    return prompt, naming_json


def rationalize_with_llm(prompt: str, llm: LLMClient, output_file: Path) -> dict:
    """Call LLM and save rationalized output"""
    print(f"  Calling {llm.model}...")
    
    start_time = time.time()
    response = llm.call(prompt)
    elapsed_time = time.time() - start_time
    
    print(f"  ⏱️  Response received in {elapsed_time:.1f} seconds")
    # Parse JSON response
    try:
        rationalized_json = json.loads(response)
    except json.JSONDecodeError:
        # Try to extract JSON from response
        import re
        json_match = re.search(r'\{.*\}', response, re.DOTALL)
        if json_match:
            rationalized_json = json.loads(json_match.group())
        else:
            raise ValueError("LLM did not return valid JSON")
    
    # Save output
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(rationalized_json, f, indent=2)
    
    return rationalized_json


def main():
    """Main entry point for prep app"""
    load_dotenv()
    
    ap = argparse.ArgumentParser(
        description="Prep App - Rationalize input files for CDM generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python prep_app.py --config config/plan_and_benefit.json
        """
    )
    
    ap.add_argument("--config", required=True, help="Path to JSON config file")
    
    args = ap.parse_args()
    
    try:
        # Load configuration
        print(f"Loading configuration from: {args.config}")
        config = load_config(args.config)
        print(f"✓ Configuration loaded for domain: {config.cdm.domain}")
        
        # Create prep output directory
        prep_outdir = Path(config.output.directory) / "prep"
        prep_outdir.mkdir(parents=True, exist_ok=True)
        
        prompts_dir = prep_outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'='*60}")
        print(f"PREP APP - Input Rationalization")
        print(f"Domain: {config.cdm.domain}")
        print(f"Output: {prep_outdir}")
        print(f"{'='*60}")
        
        # === MODEL SELECTION ===
        selected_model = select_model()
        model_config = MODEL_OPTIONS[selected_model]
        print(f"\n✓ Selected model: {model_config['name']}")
        
        # === DRY RUN PROMPT (default: Y) ===
        dry_run = prompt_user("\nDry run mode (review prompts without calling LLM)?", default="Y")
        
        if dry_run:
            print("\n🔍 DRY RUN MODE - Prompts will be saved for review, no LLM calls made")
        else:
            print("\n🚀 LIVE MODE - LLM will be called for rationalization")
        
        # Collect what to process
        process_fhir = False
        process_guardrails = False
        process_ddl = False
        process_naming = False
        
        if config.inputs.fhir:
            print(f"\nFound {len(config.inputs.fhir)} FHIR file(s)")
            process_fhir = prompt_user("Rationalize FHIR files?", default="N")
        
        if config.inputs.guardrails:
            print(f"\nFound {len(config.inputs.guardrails)} Guardrails file(s)")
            process_guardrails = prompt_user("Rationalize Guardrails files?", default="N")
        
        if config.inputs.ddl:
            print(f"\nFound {len(config.inputs.ddl)} DDL file(s)")
            process_ddl = prompt_user("Rationalize DDL files?", default="N")
        
        if config.inputs.naming_standard:
            print(f"\nFound {len(config.inputs.naming_standard)} Naming Standard file(s)")
            process_naming = prompt_user("Rationalize Naming Standard files?", default="N")
        
        # === DRY RUN MODE ===
        if dry_run:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"\n{'='*60}")
            print("DRY RUN - Generating prompts...")
            print(f"{'='*60}")
            
            dry_run_results = {
                'domain': config.cdm.domain,
                'mode': 'dry_run',
                'selected_model': selected_model,
                'prompts': {}
            }
            
            # FHIR
            if process_fhir:
                print(f"\n📝 Generating FHIR rationalization prompt...")
                prompt, _ = build_fhir_rationalization_prompt(config.cdm.domain, config.inputs.fhir)
                stats = save_prompt_to_file(prompt, f"fhir_rationalization_prompt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['fhir'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # Guardrails
            if process_guardrails:
                print(f"\n📝 Generating Guardrails rationalization prompt...")
                prompt, _ = build_guardrails_rationalization_prompt(config.cdm.domain, config.inputs.guardrails)
                stats = save_prompt_to_file(prompt, f"guardrails_rationalization_prompt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['guardrails'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # DDL
            if process_ddl:
                print(f"\n📝 Generating DDL rationalization prompt...")
                prompt, _ = build_ddl_rationalization_prompt(config.cdm.domain, config.inputs.ddl)
                stats = save_prompt_to_file(prompt, f"ddl_rationalization_prompt.txt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['ddl'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # Naming
            if process_naming:
                print(f"\n📝 Generating Naming Standards rationalization prompt...")
                prompt, _ = build_naming_rationalization_prompt(config.cdm.domain, config.inputs.naming_standard)
                stats = save_prompt_to_file(prompt, f"naming_rationalization_prompt.txt_{timestamp}.txt", prompts_dir)
                dry_run_results['prompts']['naming'] = stats
                print(f"  ✓ Saved: {stats['file']}")
                print(f"    Characters: {stats['characters']:,}")
                print(f"    Tokens (est): {stats['tokens_estimate']:,}")
            
            # Save dry run manifest
            manifest_file = prompts_dir / f"dry_run_manifest_{timestamp}.json"  # dry run
            with open(manifest_file, 'w', encoding='utf-8') as f:
                json.dump(dry_run_results, f, indent=2)
            
            print(f"\n{'='*60}")
            print(f"✓ DRY RUN COMPLETE")
            print(f"  Prompts saved to: {prompts_dir}")
            print(f"  Manifest: {manifest_file}")
            print(f"\nReview the prompts, then run again without dry run to execute.")
            print(f"{'='*60}")
            
            return
        
        # === LIVE MODE ===
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"\n{'='*60}")
        print(f"LIVE MODE - Calling {model_config['name']}...")
        print(f"{'='*60}")
        
        results = {
            'domain': config.cdm.domain,
            'mode': 'live',
            'selected_model': selected_model,
            'rationalized_files': {}
        }
        
        # Initialize LLM
        print(f"\n=== Initializing LLM ===")
        print(f"DEBUG: model_config = {model_config}")
        print(f"DEBUG: model = {model_config['model']}")
        print(f"DEBUG: base_url = {model_config['base_url']()}")

        llm = LLMClient(
            model=model_config['model'],
            base_url=model_config['base_url'](),
            temperature=float(os.getenv("TEMP_DEFAULT", "0.2")),
            timeout=1800
        )
        print(f"✓ LLM initialized: {llm.model}")
        
        # FHIR
        if process_fhir:
            print(f"\n=== Rationalizing FHIR files ===")
            prompt, _ = build_fhir_rationalization_prompt(config.cdm.domain, config.inputs.fhir)
            output_file = prep_outdir / f"rationalized_fhir_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['fhir'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        # Guardrails
        if process_guardrails:
            print(f"\n=== Rationalizing Guardrails files ===")
            prompt, _ = build_guardrails_rationalization_prompt(config.cdm.domain, config.inputs.guardrails)
            output_file = prep_outdir / f"rationalized_guardrails_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['guardrails'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        # DDL
        if process_ddl:
            print(f"\n=== Rationalizing DDL files ===")
            prompt, _ = build_ddl_rationalization_prompt(config.cdm.domain, config.inputs.ddl)
            output_file = prep_outdir / f"rationalized_ddl_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['ddl'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Entities: {len(rationalized.get('rationalized_entities', []))}")
        
        # Naming
        if process_naming:
            print(f"\n=== Rationalizing Naming Standards ===")
            prompt, _ = build_naming_rationalization_prompt(config.cdm.domain, config.inputs.naming_standard)
            output_file = prep_outdir / f"rationalized_naming_{config.cdm.domain.replace(' ', '_')}_{timestamp}.json"
            rationalized = rationalize_with_llm(prompt, llm, output_file)
            results['rationalized_files']['naming'] = str(output_file)
            print(f"  ✓ Output: {output_file}")
            print(f"  Rules: {len(rationalized.get('rationalized_rules', []))}")
        
        # Save manifest
        manifest_file = prep_outdir / f"prep_manifest_{timestamp}.json"   
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n{'='*60}")
        print(f"✓ PREP COMPLETE")
        print(f"  Manifest: {manifest_file}")
        print(f"  Rationalized files saved to: {prep_outdir}")
        print(f"\nNext step: Run app_cdm_gen.py to generate CDM")
        print(f"{'='*60}")
        
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()