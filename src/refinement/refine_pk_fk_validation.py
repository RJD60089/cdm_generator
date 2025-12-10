# src/refinement/refine_pk_fk_validation.py
"""
Refinement - PK/FK Validation

Analyzes CDM for primary key, foreign key, and relationship cardinality issues,
presents recommendations for user approval, then applies approved fixes.

Validates:
  - Every entity has exactly one PK
  - PK naming follows convention ({entity}_id)
  - All FK references resolve to existing entities
  - FK naming matches target entity PK
  - No orphan FKs (pointing to non-existent entities)
  - No circular FK references (optional warning)
  - Relationship consistency (FK exists for each relationship)
  - Cardinality correctness (M:1, 1:M, M:N, 1:1)
  - Ordinality correctness (required vs optional FK)

Three phases:
  Phase 1: AI analyzes CDM → generates validation findings (fix or flag_for_review)
  Phase 2: User reviews each finding via console
  Phase 3: AI applies approved fixes → outputs refined CDM

Input: CDM JSON (foundational or post-consolidation)
Output: Refined CDM JSON with PK/FK issues resolved, SME review items flagged

Usage via orchestrator:
    python cdm_orchestrator.py plan  # Select Refinement - PK/FK Validation
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


# =============================================================================
# PHASE 1: ANALYSIS PROMPT
# =============================================================================

def build_analysis_prompt(cdm: Dict) -> str:
    """
    Build prompt for AI to analyze CDM and identify PK/FK issues.
    """
    
    # Build entity summary with PK/FK details
    entity_details = []
    all_entity_names = set()
    
    for entity in cdm.get('entities', []):
        entity_name = entity.get('entity_name')
        all_entity_names.add(entity_name)
        
        attrs = entity.get('attributes', [])
        rels = entity.get('relationships', [])
        
        # Find PKs
        pks = [a for a in attrs if a.get('pk', False)]
        
        # Find potential FKs (fields ending in _id that aren't PK)
        fks = [a for a in attrs if a.get('name', '').endswith('_id') and not a.get('pk', False)]
        
        entity_details.append({
            'entity_name': entity_name,
            'classification': entity.get('classification'),
            'primary_keys': [{'name': pk.get('name'), 'type': pk.get('type')} for pk in pks],
            'foreign_key_candidates': [{'name': fk.get('name'), 'type': fk.get('type')} for fk in fks],
            'relationships': [{'to': r.get('to'), 'type': r.get('type'), 'fk': r.get('fk')} for r in rels],
            'total_attributes': len(attrs)
        })
    
    prompt = f"""You are a senior data architect validating Primary Keys (PK), Foreign Keys (FK), relationship cardinality, and cross-entity consistency in a Canonical Data Model (CDM).

=============================================================================
TASK
=============================================================================

Analyze the provided CDM and identify PK/FK issues, cardinality/ordinality concerns, and cross-entity inconsistencies.
Your findings will be reviewed by SME analysts before implementation.

=============================================================================
VALIDATION RULES
=============================================================================

**Primary Key Rules:**
1. Every entity MUST have exactly one primary key
2. PK should be named {{entity_name_singular}}_id (e.g., carrier_id for Carrier)
3. PK should be INTEGER type (surrogate key pattern)
4. PK must have pk: true, required: true

**Foreign Key Rules:**
1. Every FK field (ending in _id, not a PK) should reference an existing entity
2. FK name should match the target entity's PK name (e.g., carrier_id references Carrier.carrier_id)
3. Every relationship defined should have a corresponding FK attribute
4. FK should NOT be marked as pk: true

**Relationship Consistency:**
1. If entity A has relationship to entity B, entity A should have FK to B
2. FK field name in relationship should exist in attributes
3. Relationship "to" entity must exist in CDM

**Cardinality and Ordinality:**
1. Verify relationship cardinality notation (M:1, 1:M, M:N, 1:1) is appropriate
2. Check for implied cardinality based on FK placement:
   - FK on child entity = M:1 (many children to one parent)
   - Junction table with two FKs = M:N relationship
   - 1:1 relationships should have unique constraint consideration
3. Ordinality (optional vs required):
   - Required FK = mandatory relationship (1 minimum)
   - Optional FK = optional relationship (0 minimum)
4. Flag relationships where cardinality may be incorrectly specified
5. Compare cardinality/ordinality across SIMILAR entities:
   - If Entity A and Entity B both reference Entity C, their cardinality to C should be consistent 
     unless there's a clear business reason for difference
   - Junction tables should consistently use M:1 to both parent entities

**Cross-Entity Consistency (IMPORTANT):**
Look for inconsistencies across similar entities that may indicate modeling errors:

1. Similar entities with different patterns:
   - Entities with similar names or roles should have similar structural patterns
   - If GroupPlanAssignment has both group_id and plan_id as FKs, check if SubgroupPlanAssignment 
     follows the same pattern
   - If one junction table has effective_date/termination_date, similar junction tables should too

2. Attribute naming inconsistencies:
   - Same concept should have same attribute name across entities
   - e.g., if one entity uses "effective_date" and another uses "start_date" for the same concept, flag it

3. Relationship pattern inconsistencies:
   - If Carrier→Group is M:1, verify similar parent-child relationships follow same pattern
   - If one entity tracks audit fields (created_date, modified_date), similar entities should too

4. Missing parallel structures:
   - If SponsorAssociation exists, check if parallel structures (SponsorOrganization, etc.) exist 
     and are consistently modeled

=============================================================================
ACTION GUIDANCE
=============================================================================

Use "fix" action ONLY when you are HIGHLY CONFIDENT that:
  - The issue is clearly a structural error (missing PK, orphan FK, broken reference)
  - The fix is unambiguous and will not change business meaning
  - Confidence is 0.85 or higher

Use "flag_for_review" when:
  - Cardinality MIGHT be incorrect but business rules are unclear
  - FK exists but target entity relationship is ambiguous
  - Naming convention differs but may be intentional
  - Ordinality (required vs optional) needs business validation
  - Cross-entity patterns are inconsistent and need SME clarification
  - Similar entities have different structural patterns
  - SME input would help clarify the appropriate action
  - This is the PREFERRED action when there is ANY uncertainty

It is better to flag a potential issue for review than to incorrectly modify 
the data model structure.

=============================================================================
CDM TO ANALYZE
=============================================================================

Domain: {cdm.get('domain', 'Unknown')}
Total Entities: {len(cdm.get('entities', []))}

All Entity Names: {sorted(all_entity_names)}

Entity Details:
{json.dumps(entity_details, indent=2)}

Full CDM:
{json.dumps(cdm, indent=2)}

=============================================================================
OUTPUT FORMAT
=============================================================================

Return ONLY valid JSON matching this exact schema:

{{
  "analysis_summary": {{
    "total_entities_analyzed": <int>,
    "entities_with_pk_issues": <int>,
    "entities_with_fk_issues": <int>,
    "entities_with_cardinality_issues": <int>,
    "entities_with_consistency_issues": <int>,
    "total_issues_found": <int>,
    "fixes": <int>,
    "flags_for_review": <int>
  }},
  "validation_findings": [
    {{
      "id": "<unique finding id, e.g., PKV-001>",
      "action": "<fix|flag_for_review>",
      "category": "<missing_pk|invalid_pk_name|missing_fk|orphan_fk|broken_relationship|type_mismatch|naming_convention|cardinality_issue|ordinality_issue|pattern_inconsistency|attribute_naming_inconsistency|missing_parallel_structure>",
      "entity_name": "<affected entity>",
      "related_entities": ["<other entities involved, for cross-entity issues>"],
      "field_name": "<affected field, if applicable>",
      "current_value": "<current state>",
      "expected_value": "<expected/correct state, or null for flag_for_review>",
      "description": "<clear description of the issue>",
      "confidence": <0.0-1.0>,
      "questions_for_sme": ["<specific questions if flag_for_review>"],
      "recommended_fix": {{
        "action": "<add_pk|rename_pk|add_fk|remove_fk|rename_fk|add_relationship|fix_relationship|change_type|update_cardinality|update_ordinality|align_pattern|rename_attribute|add_structure>",
        "details": {{<action-specific details>}}
      }}
    }}
  ]
}}

Categories explained:
- missing_pk: Entity has no primary key defined
- invalid_pk_name: PK exists but name doesn't follow convention
- missing_fk: Relationship exists but no FK attribute
- orphan_fk: FK attribute exists but references non-existent entity
- broken_relationship: Relationship references non-existent entity or FK
- type_mismatch: PK/FK type inconsistency
- naming_convention: FK name doesn't match target PK name
- cardinality_issue: Relationship cardinality (M:1, 1:M, M:N, 1:1) may be incorrect
- ordinality_issue: Required/optional status of FK may be incorrect
- pattern_inconsistency: Similar entities have different structural patterns (FKs, relationships, audit fields)
- attribute_naming_inconsistency: Same concept has different attribute names across entities
- missing_parallel_structure: Expected parallel structure (like junction table) is missing

VALIDATION CHECKLIST (verify before responding):
[ ] fix action used ONLY when confidence >= 0.85
[ ] flag_for_review used when any uncertainty exists about business meaning
[ ] questions_for_sme provided for all flag_for_review findings
[ ] Cardinality reviewed for all relationships
[ ] Cross-entity consistency reviewed for similar entities

If NO issues are found, return empty validation_findings array.

Return ONLY the JSON object. No markdown, no code blocks, no commentary.
"""
    return prompt


# =============================================================================
# PHASE 1: ANALYZE PK/FK
# =============================================================================

def analyze_pk_fk(
    cdm: Dict,
    llm: LLMClient,
    outdir: Path,
    dry_run: bool = False
) -> Optional[Dict]:
    """
    Phase 1: AI analyzes CDM and generates PK/FK validation findings.
    
    Args:
        cdm: CDM dict to validate
        llm: LLM client (None if dry_run)
        outdir: Output directory
        dry_run: If True, save prompt only
        
    Returns:
        Findings dict (None if dry_run)
    """
    
    print(f"\n{'='*60}")
    print(f"PHASE 1: ANALYZE PK/FK STRUCTURE")
    print(f"{'='*60}")
    
    entity_count = len(cdm.get('entities', []))
    print(f"   Analyzing {entity_count} entities...")
    
    # Build prompt
    prompt = build_analysis_prompt(cdm)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"pk_fk_analysis_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"\n   ✓ Analysis prompt saved: {output_file}")
        print(f"     Characters: {len(prompt):,}")
        return None
    
    # Live mode - call LLM
    print(f"   🤖 Calling LLM for PK/FK analysis...")
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
        response_clean = response.strip()
        if response_clean.startswith("```"):
            lines = response_clean.split("\n")
            if lines[0].strip().lower() in ("```json", "```"):
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
        
        findings = json.loads(response_clean)
        
        # Validate structure
        if 'validation_findings' not in findings:
            raise ValueError("Response missing 'validation_findings' key")
        
        # Add metadata
        findings["generated_date"] = datetime.now().isoformat()
        findings["phase"] = "pk_fk_analysis"
        findings["source_cdm"] = cdm.get('domain', 'Unknown')
        
        # Save findings
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = cdm.get('domain', 'unknown').lower().replace(' ', '_')
        output_file = outdir / f"pk_fk_findings_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(findings, f, indent=2)
        
        # Report results
        finding_count = len(findings.get('validation_findings', []))
        summary = findings.get('analysis_summary', {})
        
        print(f"\n   ✅ Analysis complete!")
        print(f"      Total findings: {finding_count}")
        print(f"      Errors:   {summary.get('errors', 0)}")
        print(f"      Warnings: {summary.get('warnings', 0)}")
        print(f"      Info:     {summary.get('info', 0)}")
        print(f"\n   📄 Saved to: {output_file}")
        
        return findings
        
    except json.JSONDecodeError as e:
        print(f"\n   ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"   Response preview: {response[:500]}...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"pk_fk_analysis_error_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"   💾 Full response saved to: {error_file}")
        raise


# =============================================================================
# PHASE 2: REVIEW FINDINGS
# =============================================================================

def review_findings(findings: Dict) -> Dict:
    """
    Phase 2: Present each finding to user for approval via console.
    
    Args:
        findings: Output from analyze_pk_fk
        
    Returns:
        Dict with approved_fixes list
    """
    
    print(f"\n{'='*60}")
    print(f"PHASE 2: REVIEW PK/FK FINDINGS")
    print(f"{'='*60}")
    
    items = findings.get('validation_findings', [])
    
    if not items:
        print("\n   No PK/FK issues found. CDM structure is valid.")
        return {
            "approved_fixes": [],
            "rejected_fixes": [],
            "flagged_for_sme": [],
            "review_date": datetime.now().isoformat()
        }
    
    # Group by action type for display
    fixes = [f for f in items if f.get('action') == 'fix']
    flags = [f for f in items if f.get('action') == 'flag_for_review']
    
    print(f"\n   {len(items)} finding(s) to review:")
    print(f"      🔧 Fixes:           {len(fixes)}")
    print(f"      🔍 Flags for Review: {len(flags)}")
    
    print("\n   For each finding:")
    print("     [A] Approve - Apply this fix / Accept flag for SME")
    print("     [R] Reject  - Do not apply")
    print("     [S] Skip    - Decide later (treated as reject)")
    print("     [Q] Quit    - Stop review, process approved so far")
    print("     [AA] Approve All remaining")
    print("     [RA] Reject All remaining")
    
    approved = []
    rejected = []
    flagged_for_sme = []
    approve_all = False
    reject_all = False
    
    # Sort: Fixes first, then Flags for review
    sorted_items = fixes + flags
    
    for i, finding in enumerate(sorted_items, 1):
        if reject_all:
            rejected.append(finding)
            continue
        if approve_all:
            if finding.get('action') == 'flag_for_review':
                flagged_for_sme.append(finding)
            else:
                approved.append(finding)
            continue
            
        action_icon = {'fix': '🔧', 'flag_for_review': '🔍'}.get(finding.get('action'), '⚪')
        
        print(f"\n{'─'*60}")
        print(f"FINDING {i}/{len(sorted_items)}: {finding.get('id', f'PKV-{i:03d}')}")
        print(f"{'─'*60}")
        print(f"Action:      {action_icon} {finding.get('action')}")
        print(f"Category:    {finding.get('category')}")
        print(f"Entity:      {finding.get('entity_name')}")
        
        # Show related entities for cross-entity issues
        related = finding.get('related_entities', [])
        if related:
            print(f"Related:     {', '.join(related)}")
        
        if finding.get('field_name'):
            print(f"Field:       {finding.get('field_name')}")
        print(f"Confidence:  {finding.get('confidence', 'N/A')}")
        
        print(f"\nIssue:")
        print(f"  {finding.get('description', 'No description')}")
        
        if finding.get('current_value'):
            print(f"\nCurrent:  {finding.get('current_value')}")
        if finding.get('expected_value'):
            print(f"Expected: {finding.get('expected_value')}")
        
        # Show SME questions if flag_for_review
        questions = finding.get('questions_for_sme', [])
        if questions:
            print(f"\nQuestions for SME:")
            for q in questions:
                print(f"  • {q}")
        
        fix = finding.get('recommended_fix', {})
        if fix:
            print(f"\nRecommended Fix:")
            print(f"  Action: {fix.get('action')}")
            if fix.get('details'):
                print(f"  Details: {json.dumps(fix.get('details'), indent=4)}")
        
        # Get user input
        while True:
            choice = input(f"\n[A]pprove / [R]eject / [S]kip / [Q]uit / [AA] All / [RA] Reject All? ").strip().upper()
            
            if choice == 'A':
                if finding.get('action') == 'flag_for_review':
                    flagged_for_sme.append(finding)
                    print(f"   ✓ Flagged for SME review")
                else:
                    approved.append(finding)
                    print(f"   ✓ Approved")
                break
            elif choice == 'R':
                rejected.append(finding)
                print(f"   ✗ Rejected")
                break
            elif choice == 'S':
                rejected.append(finding)
                print(f"   ○ Skipped")
                break
            elif choice == 'Q':
                print(f"\n   Stopping review. {len(approved)} approved, {len(sorted_items) - i} not reviewed.")
                rejected.extend(sorted_items[i:])
                break
            elif choice == 'AA':
                if finding.get('action') == 'flag_for_review':
                    flagged_for_sme.append(finding)
                else:
                    approved.append(finding)
                approve_all = True
                print(f"   ✓ Approved (and all remaining)")
                break
            elif choice == 'RA':
                rejected.append(finding)
                reject_all = True
                print(f"   ✗ Rejected (and all remaining)")
                break
            else:
                print("   Invalid choice. Please enter A, R, S, Q, AA, or RA.")
        
        if choice == 'Q':
            break
    
    result = {
        "approved_fixes": approved,
        "rejected_fixes": rejected,
        "flagged_for_sme": flagged_for_sme,
        "review_date": datetime.now().isoformat(),
        "total_reviewed": len(approved) + len(rejected) + len(flagged_for_sme),
        "total_approved": len(approved),
        "total_rejected": len(rejected),
        "total_flagged": len(flagged_for_sme)
    }
    
    print(f"\n{'─'*60}")
    print(f"REVIEW COMPLETE")
    print(f"{'─'*60}")
    print(f"   Approved:        {len(approved)}")
    print(f"   Flagged for SME: {len(flagged_for_sme)}")
    print(f"   Rejected:        {len(rejected)}")
    
    return result


# =============================================================================
# PHASE 3: APPLY PROMPT
# =============================================================================

def build_apply_prompt(cdm: Dict, approved_fixes: List[Dict]) -> str:
    """
    Build prompt for AI to apply approved PK/FK fixes.
    """
    
    prompt = f"""You are a CDM refactoring engine. Apply the approved PK/FK fixes to the Canonical Data Model exactly as specified.

=============================================================================
RULES
=============================================================================

1. Apply ONLY the approved fixes - do not add anything not specified
2. Do not reinterpret the domain or make additional changes
3. When adding a PK:
   - Add the attribute with pk: true, required: true
   - Use INTEGER type
   - Position it first in the attributes array
4. When renaming a PK/FK:
   - Update the attribute name
   - Update any relationships that reference it
5. When adding a FK:
   - Add the attribute with appropriate type (INTEGER)
   - Ensure it matches the target entity's PK type
6. When fixing relationships:
   - Update the fk field to match the actual attribute name
   - Ensure "to" references an existing entity
7. When updating cardinality:
   - Update the relationship "type" field (e.g., M:1, 1:M, M:N, 1:1)
   - Ensure FK placement matches cardinality (FK on "many" side)
8. When updating ordinality:
   - Update the FK attribute "required" field (true = mandatory, false = optional)
9. When aligning patterns across entities:
   - Add missing attributes to achieve consistency
   - Ensure similar entities have similar structural patterns
10. When renaming attributes for consistency:
    - Update the attribute name to match the standard name
    - Update any relationships referencing the old name
11. Preserve all attributes and relationships not affected by fixes

=============================================================================
ORIGINAL CDM
=============================================================================

Domain: {cdm.get('domain')}
Entities: {len(cdm.get('entities', []))}

{json.dumps(cdm, indent=2)}

=============================================================================
APPROVED FIXES TO APPLY
=============================================================================

{json.dumps(approved_fixes, indent=2)}

=============================================================================
OUTPUT FORMAT
=============================================================================

Return the COMPLETE updated CDM as valid JSON with:
- All original metadata (domain, cdm_version, etc.)
- Updated entities array with fixes applied
- A new "pk_fk_fix_log" array documenting what was changed

{{
  "domain": "{cdm.get('domain')}",
  "cdm_version": "{cdm.get('cdm_version', '1.0')}",
  "entities": [...],
  "pk_fk_fix_log": [
    {{
      "finding_id": "<original finding id>",
      "action": "<what was done>",
      "entity": "<affected entity>",
      "field": "<affected field>",
      "change": "<description of change>"
    }}
  ]
}}

Return ONLY the JSON object. No markdown, no code blocks, no commentary.
"""
    return prompt


# =============================================================================
# PHASE 3: APPLY FIXES
# =============================================================================

def apply_fixes(
    cdm: Dict,
    approved: Dict,
    llm: LLMClient,
    outdir: Path,
    dry_run: bool = False
) -> Optional[Dict]:
    """
    Phase 3: AI applies approved PK/FK fixes to CDM.
    
    Args:
        cdm: Original CDM
        approved: Output from review_findings
        llm: LLM client
        outdir: Output directory
        dry_run: If True, save prompt only
        
    Returns:
        Refined CDM dict (None if dry_run or no changes)
    """
    
    print(f"\n{'='*60}")
    print(f"PHASE 3: APPLY PK/FK FIXES")
    print(f"{'='*60}")
    
    approved_fixes = approved.get('approved_fixes', [])
    
    if not approved_fixes:
        print("\n   No approved fixes to apply. CDM unchanged.")
        return cdm
    
    print(f"   Applying {len(approved_fixes)} approved fix(es)...")
    
    # Build prompt
    prompt = build_apply_prompt(cdm, approved_fixes)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"pk_fk_apply_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"\n   ✓ Apply prompt saved: {output_file}")
        print(f"     Characters: {len(prompt):,}")
        return None
    
    # Live mode - call LLM
    print(f"   🤖 Calling LLM to apply fixes...")
    print(f"      Prompt size: {len(prompt):,} chars (~{len(prompt)//4:,} tokens)")
    
    messages = [
        {
            "role": "system",
            "content": "You are a CDM refactoring engine. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
        },
        {
            "role": "user",
            "content": prompt
        }
    ]
    
    response, token_usage = llm.chat(messages)
    
    # Parse response
    try:
        response_clean = response.strip()
        if response_clean.startswith("```"):
            lines = response_clean.split("\n")
            if lines[0].strip().lower() in ("```json", "```"):
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
        
        refined_cdm = json.loads(response_clean)
        
        # Validate structure
        if 'entities' not in refined_cdm:
            raise ValueError("Response missing 'entities' key")
        
        # Add/update metadata
        refined_cdm["refined_date"] = datetime.now().isoformat()
        refined_cdm["refinement_type"] = "pk_fk_validation"
        refined_cdm["refinement_model"] = llm.model if llm else "unknown"
        refined_cdm["fixes_applied"] = len(approved_fixes)
        refined_cdm["flagged_for_sme_review"] = approved.get('flagged_for_sme', [])
        
        # Save refined CDM
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = cdm.get('domain', 'unknown').lower().replace(' ', '_')
        output_file = outdir / f"cdm_{domain_safe}_pk_fk_validated_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(refined_cdm, f, indent=2)
        
        # Save approved fixes log
        approved_file = outdir / f"pk_fk_approved_{domain_safe}_{timestamp}.json"
        with open(approved_file, 'w', encoding='utf-8') as f:
            json.dump(approved, f, indent=2)
        
        # Report results
        log = refined_cdm.get('pk_fk_fix_log', [])
        flagged = approved.get('flagged_for_sme', [])
        
        print(f"\n   ✅ PK/FK fixes applied!")
        print(f"      Fixes applied: {len(log)}")
        if flagged:
            print(f"      Flagged for SME: {len(flagged)}")
        print(f"\n   📄 Refined CDM: {output_file}")
        print(f"   📄 Fix log:     {approved_file}")
        
        return refined_cdm
        
    except json.JSONDecodeError as e:
        print(f"\n   ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"   Response preview: {response[:500]}...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"pk_fk_apply_error_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"   💾 Full response saved to: {error_file}")
        raise


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def find_latest_cdm(outdir: Path, domain: str) -> Optional[Path]:
    """
    Find latest CDM file for a domain.
    
    Naming convention: cdm_{domain}_{module}_{YYYYMMDD}_{HHMMSS}.json
    Examples:
        cdm_plan_foundational_20251203_100000.json
        cdm_plan_consolidated_20251203_142244.json
        cdm_plan_pk_fk_validated_20251203_150000.json
    
    For PK/FK validation, finds the latest CDM (including consolidated).
    Returns the file with the latest timestamp.
    """
    domain_safe = domain.lower().replace(' ', '_')
    pattern = f"cdm_{domain_safe}_*.json"
    matches = list(outdir.glob(pattern))
    
    # Filter out non-CDM files (reports, findings, etc.)
    cdm_files = [f for f in matches 
                 if 'recommendations' not in f.name 
                 and 'findings' not in f.name
                 and 'approved' not in f.name
                 and 'disposition' not in f.name
                 and 'gaps' not in f.name
                 and '_full_' not in f.name]
    
    if not cdm_files:
        return None
    
    def extract_timestamp(filepath: Path) -> str:
        """Extract timestamp from cdm_{domain}_{module}_{date}_{time}.json"""
        stem = filepath.stem
        parts = stem.split('_')
        # Timestamp is last 2 parts: YYYYMMDD_HHMMSS
        if len(parts) >= 2:
            return f"{parts[-2]}_{parts[-1]}"
        return "0"
    
    # Sort by timestamp (descending), pick latest
    cdm_files.sort(key=extract_timestamp, reverse=True)
    return cdm_files[0]


def run_pk_fk_validation(
    config: AppConfig,
    cdm_file: Optional[Path],
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Optional[Dict]:
    """
    Main entry point for PK/FK validation refinement.
    
    Orchestrates all three phases:
      Phase 1: Analyze → findings
      Phase 2: Review → approved fixes
      Phase 3: Apply → refined CDM
    
    Args:
        config: App configuration
        cdm_file: Path to CDM (None to auto-find latest)
        outdir: Output directory
        llm: LLM client (None if dry_run)
        dry_run: If True, save prompts only
        
    Returns:
        Refined CDM dict (None if dry_run or no changes)
    """
    
    print(f"\n{'='*60}")
    print(f"REFINEMENT - PK/FK VALIDATION")
    print(f"{'='*60}")
    print(f"   Domain: {config.cdm.domain}")
    
    # Find CDM file if not provided
    if cdm_file is None:
        cdm_file = find_latest_cdm(outdir, config.cdm.domain)
        if cdm_file is None:
            print(f"\n   ❌ ERROR: No CDM file found in {outdir}")
            print(f"      Run 'Build Foundational CDM' first.")
            return None
    
    print(f"   CDM File: {cdm_file.name}")
    
    # Load CDM
    with open(cdm_file, 'r', encoding='utf-8') as f:
        cdm = json.load(f)
    
    entity_count = len(cdm.get('entities', []))
    print(f"   Entities: {entity_count}")
    
    # Ensure output directory exists
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Phase 1: Analyze
    findings = analyze_pk_fk(cdm, llm, outdir, dry_run)
    
    if dry_run:
        print(f"\n   🔍 DRY RUN complete. Review prompts in {outdir / 'prompts'}")
        return None
    
    if findings is None:
        return None
    
    # Phase 2: Review
    approved = review_findings(findings)
    
    if not approved.get('approved_fixes'):
        print(f"\n   No fixes approved. CDM unchanged.")
        return cdm
    
    # Phase 3: Apply
    refined_cdm = apply_fixes(cdm, approved, llm, outdir, dry_run)
    
    print(f"\n{'='*60}")
    print(f"PK/FK VALIDATION REFINEMENT COMPLETE")
    print(f"{'='*60}")
    
    return refined_cdm


# =============================================================================
# STANDALONE EXECUTION (for testing)
# =============================================================================

if __name__ == "__main__":
    import sys
    
    print("This module should be run via cdm_orchestrator.py")
    print("Usage: python cdm_orchestrator.py plan")
    sys.exit(1)