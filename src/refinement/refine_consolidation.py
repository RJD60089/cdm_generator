# src/refinement/refine_consolidation.py
"""
Refinement - Consolidation

Analyzes CDM for entity consolidation opportunities (overlapping/redundant entities),
presents recommendations for user approval, then applies approved changes.

Three phases:
  Phase 1: AI analyzes CDM → generates recommendations
  Phase 2: User reviews each recommendation via console
  Phase 3: AI applies approved changes → outputs refined CDM

Input: Foundational CDM JSON
Output: Refined CDM JSON with consolidation applied

Usage via orchestrator:
    python cdm_orchestrator.py plan  # Select Refinement - Consolidation
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

def build_analysis_prompt(cdm: Dict, domain_name: str = None, domain_description: str = None) -> str:
    """
    Build prompt for AI to analyze CDM and recommend consolidations.
    
    Args:
        cdm: The CDM dictionary
        domain_name: Domain name from config (e.g., "Plan and Benefit")
        domain_description: Domain description from config
    """
    
    # Use CDM domain if not provided
    if domain_name is None:
        domain_name = cdm.get('domain', 'Unknown')
    if domain_description is None:
        domain_description = cdm.get('description', 'No description provided')
    
    entity_count = len(cdm.get('entities', []))
    
    prompt = f"""You are a senior data architect who is an expert in analyzing a newly formed Canonical Data Model (CDM) for the specified domain and description below.

CDM Domain: {domain_name}
CDM Description: {domain_description}

===============================================================================
TASK
===============================================================================

Analyze the provided, newly formed CDM and identify opportunities to consolidate entities that may not have been fully consolidated in the formation of the CDM. 

This CDM, along with other domain CDMs, will collectively form the enterprise canonical data model. Your recommendations will be reviewed by SME analysts before final implementation.

===============================================================================
CONSOLIDATION PHILOSOPHY
===============================================================================

1. **Consolidate when entities represent the SAME real-world thing from different standards.**
   Example: If two sources both define the same concept, merge them.

2. **Keep entities DISTINCT when they represent different ROLES in the business process,
   even if they share similar attributes.**
   Different actors or different sides of a business relationship should remain separate entities.

3. **Prefer LINKING over MERGING when relationships exist but roles differ.**
   FK relationships between entities often indicate they should remain separate.

4. **Consolidate ONLY when the benefit is clear:**
   - Eliminates true redundancy (same thing, different source names)
   - Simplifies the model without losing domain meaning

===============================================================================
ACTION GUIDANCE
===============================================================================

Use "merge_entities" ONLY when you are HIGHLY CONFIDENT that:
  - Entities clearly represent the SAME real-world concept from different sources
  - There is no 1:N (one-to-many) relationship between them
  - Merging does NOT lose important domain distinctions
  - Confidence is 0.85 or higher

Use "flag_for_review" when:
  - Entities MIGHT represent the same concept but you are not certain
  - Similar attributes exist but business roles may differ  
  - Cardinality or FK relationships suggest they might be parent-child
  - SME input would help clarify the appropriate action
  - This is the PREFERRED action when there is ANY uncertainty

The output will be reviewed by SME analysts. It is better to flag a potential 
consolidation for review than to incorrectly merge entities that should remain separate.

===============================================================================
CRITICAL CONSTRAINTS
===============================================================================

1. CROSS-SOURCE FIRST: Always consider entities from different sources for consolidation first.
   Then consider entities from the same source.

2. REVIEW ALL ENTITIES: Read and consider the description, attributes, and relationships 
   for EACH AND EVERY entity before making recommendations.

3. MUTUAL EXCLUSIVITY: Each entity can appear in ONE AND ONLY ONE recommendation.
   - If an entity could fit in multiple recommendations, combine into ONE broader recommendation
   - NEVER list the same entity in multiple recommendations

4. NO CIRCULAR REFERENCES: 
   - The resulting_entity_name must NOT appear as a target in another recommendation

5. RESPECT HIERARCHY:
   - Do NOT consolidate entities at different hierarchy levels
   - Do NOT consolidate junction/association tables with their parent entities
   - Do NOT consolidate junction tables with each other (they represent distinct relationships)

6. CHECK CARDINALITY:
   - If Entity A can have MULTIPLE Entity B records (1:N), they are parent-child, NOT duplicates
   - FK relationships between potential merge targets often mean they should stay separate

===============================================================================
CDM TO ANALYZE (FULL JSON)
===============================================================================

Total Entities: {entity_count}

{json.dumps(cdm, indent=2)}

===============================================================================
SUPPORTED ACTIONS
===============================================================================

- "merge_entities"
    - Combine multiple entities into ONE new entity
    - Use ONLY when highly confident entities represent the SAME real-world concept
    - Confidence must be 0.85 or higher
    - All targets are removed; new entity is created
    - When naming: prefer dominant entity name, or industry-standard name if appropriate

- "flag_for_review"
    - Flag potential consolidation candidates for SME analyst review
    - Use when entities MIGHT be candidates but you are not certain
    - PREFERRED action when there is any uncertainty about business meaning
    - Include clear justification and questions for the SME

===============================================================================
OUTPUT FORMAT
===============================================================================

Return ONLY valid JSON matching this schema:

{{
  "analysis_summary": {{
    "total_entities_analyzed": <int>,
    "consolidation_candidates_found": <int>,
    "entities_in_recommendations": ["<list of all entities appearing in any recommendation>"],
    "analysis_notes": "<summary including any entities you considered but chose NOT to consolidate and why>"
  }},
  "consolidation_recommendations": [
    {{
      "id": "REC-001",
      "action": "<merge_entities|flag_for_review>",
      "targets": ["<entity_1>", "<entity_2>", ...],
      "resulting_entity_name": "<name for merged/resulting entity, or primary entity name for flag_for_review>",
      "justification": "<explain why these entities represent the SAME thing, or why they need SME review>",
      "questions_for_sme": ["<specific questions for SME if flag_for_review>"],
      "attribute_overlap": {{
        "shared_concepts": ["<conceptually similar attributes>"],
        "unique_to_each": {{
          "<entity_name>": ["<unique attrs>"]
        }}
      }},
      "relationship_impact": [
        {{
          "affected_entity": "<entity with FK to target>",
          "current_fk": "<current FK field>",
          "proposed_change": "<how FK should be updated>"
        }}
      ],
      "risk_level": "<low|medium|high>",
      "confidence": <0.0-1.0>
    }}
  ]
}}

VALIDATION CHECKLIST (verify before responding):
[ ] Each entity appears in AT MOST ONE recommendation
[ ] merge_entities used ONLY when confidence >= 0.85
[ ] flag_for_review used when any uncertainty exists
[ ] No junction tables consolidated with each other or parent entities
[ ] No entities with FK relationships to each other merged without strong justification

Return ONLY the JSON object. No markdown, no code blocks, no commentary.
"""
    return prompt


# =============================================================================
# PHASE 1: ANALYZE CONSOLIDATION
# =============================================================================

def analyze_consolidation(
    cdm: Dict,
    llm: LLMClient,
    outdir: Path,
    dry_run: bool = False,
    domain_name: str = None,
    domain_description: str = None
) -> Optional[Dict]:
    """
    Phase 1: AI analyzes CDM and generates consolidation recommendations.
    
    Args:
        cdm: Foundational CDM dict
        llm: LLM client (None if dry_run)
        outdir: Output directory
        dry_run: If True, save prompt only
        domain_name: Domain name from config
        domain_description: Domain description from config
        
    Returns:
        Recommendations dict (None if dry_run)
    """
    
    print(f"\n{'='*60}")
    print(f"PHASE 1: ANALYZE CONSOLIDATION OPPORTUNITIES")
    print(f"{'='*60}")
    
    entity_count = len(cdm.get('entities', []))
    print(f"   Domain: {domain_name or cdm.get('domain', 'Unknown')}")
    print(f"   Analyzing {entity_count} entities...")
    
    # Build prompt
    prompt = build_analysis_prompt(cdm, domain_name, domain_description)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"consolidation_analysis_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"\n   ✓ Analysis prompt saved: {output_file}")
        print(f"     Characters: {len(prompt):,}")
        return None
    
    # Live mode - call LLM
    print(f"   🤖 Calling LLM for analysis...")
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
        
        recommendations = json.loads(response_clean)
        
        # Validate structure
        if 'consolidation_recommendations' not in recommendations:
            raise ValueError("Response missing 'consolidation_recommendations' key")
        
        # Add metadata
        recommendations["generated_date"] = datetime.now().isoformat()
        recommendations["phase"] = "analysis"
        recommendations["source_cdm"] = cdm.get('domain', 'Unknown')
        
        # Save recommendations
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = cdm.get('domain', 'unknown').lower().replace(' ', '_')
        output_file = outdir / f"consolidation_recommendations_{domain_safe}_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(recommendations, f, indent=2)
        
        # Report results
        rec_count = len(recommendations.get('consolidation_recommendations', []))
        summary = recommendations.get('analysis_summary', {})
        
        print(f"\n   ✅ Analysis complete!")
        print(f"      Recommendations: {rec_count}")
        print(f"      Notes: {summary.get('analysis_notes', 'N/A')}")
        print(f"\n   📄 Saved to: {output_file}")
        
        return recommendations
        
    except json.JSONDecodeError as e:
        print(f"\n   ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"   Response preview: {response[:500]}...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"consolidation_analysis_error_{timestamp}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(response)
        print(f"   💾 Full response saved to: {error_file}")
        raise


# =============================================================================
# PHASE 2: REVIEW RECOMMENDATIONS
# =============================================================================

def review_recommendations(recommendations: Dict) -> Dict:
    """
    Phase 2: Present each recommendation to user for approval via console.
    
    Args:
        recommendations: Output from analyze_consolidation
        
    Returns:
        Dict with approved_changes list
    """
    
    print(f"\n{'='*60}")
    print(f"PHASE 2: REVIEW CONSOLIDATION RECOMMENDATIONS")
    print(f"{'='*60}")
    
    recs = recommendations.get('consolidation_recommendations', [])
    
    if not recs:
        print("\n   No consolidation recommendations to review.")
        return {
            "approved_changes": [],
            "rejected_changes": [],
            "review_date": datetime.now().isoformat()
        }
    
    print(f"\n   {len(recs)} recommendation(s) to review.\n")
    print("   For each recommendation:")
    print("     [A] Approve - Apply this consolidation")
    print("     [R] Reject  - Do not apply")
    print("     [M] Modify  - Change the resulting entity name")
    print("     [S] Skip    - Decide later (treated as reject for this run)")
    print("     [Q] Quit    - Stop review, process approved so far")
    
    approved = []
    rejected = []
    
    for i, rec in enumerate(recs, 1):
        print(f"\n{'─'*60}")
        print(f"RECOMMENDATION {i}/{len(recs)}: {rec.get('id', f'REC-{i:03d}')}")
        print(f"{'─'*60}")
        print(f"Action:      {rec.get('action')}")
        print(f"Targets:     {', '.join(rec.get('targets', []))}")
        print(f"Result:      {rec.get('resulting_entity_name')}")
        print(f"Confidence:  {rec.get('confidence', 'N/A')}")
        print(f"Risk:        {rec.get('risk_level', 'N/A')}")
        print(f"\nJustification:")
        print(f"  {rec.get('justification', 'No justification provided')}")
        
        # Show SME questions if flag_for_review
        questions = rec.get('questions_for_sme', [])
        if questions:
            print(f"\nQuestions for SME:")
            for q in questions:
                print(f"  • {q}")
        
        # Show attribute overlap if present
        overlap = rec.get('attribute_overlap', {})
        if overlap.get('shared_attributes'):
            print(f"\nShared Attributes ({len(overlap['shared_attributes'])}):")
            print(f"  {', '.join(overlap['shared_attributes'][:10])}")
            if len(overlap['shared_attributes']) > 10:
                print(f"  ... and {len(overlap['shared_attributes']) - 10} more")
        
        # Show relationship impact if present
        impacts = rec.get('relationship_impact', [])
        if impacts:
            print(f"\nRelationship Impact ({len(impacts)} affected):")
            for impact in impacts[:3]:
                print(f"  - {impact.get('affected_entity')}: {impact.get('proposed_change', 'N/A')}")
            if len(impacts) > 3:
                print(f"  ... and {len(impacts) - 3} more")
        
        # Get user input
        while True:
            choice = input(f"\n[A]pprove / [R]eject / [M]odify / [S]kip / [Q]uit? ").strip().upper()
            
            if choice == 'A':
                approved.append(rec)
                print(f"   ✓ Approved")
                break
            elif choice == 'R':
                rejected.append(rec)
                print(f"   ✗ Rejected")
                break
            elif choice == 'M':
                new_name = input(f"   Enter new resulting entity name [{rec.get('resulting_entity_name')}]: ").strip()
                if new_name:
                    rec['resulting_entity_name'] = new_name
                    rec['modified_by_user'] = True
                approved.append(rec)
                print(f"   ✓ Approved (modified: {new_name})")
                break
            elif choice == 'S':
                rejected.append(rec)
                print(f"   ○ Skipped (treated as reject)")
                break
            elif choice == 'Q':
                print(f"\n   Stopping review. {len(approved)} approved, {len(recs) - i} not reviewed.")
                # Remaining go to rejected
                rejected.extend(recs[i:])
                break
            else:
                print("   Invalid choice. Please enter A, R, M, S, or Q.")
        
        if choice == 'Q':
            break
    
    result = {
        "approved_changes": approved,
        "rejected_changes": rejected,
        "review_date": datetime.now().isoformat(),
        "total_reviewed": len(approved) + len(rejected),
        "total_approved": len(approved),
        "total_rejected": len(rejected)
    }
    
    print(f"\n{'─'*60}")
    print(f"REVIEW COMPLETE")
    print(f"{'─'*60}")
    print(f"   Approved: {len(approved)}")
    print(f"   Rejected: {len(rejected)}")
    
    return result


def build_apply_prompt(cdm: Dict, approved_changes: List[Dict]) -> str:
    """
    Build prompt for AI to apply approved consolidation changes.
    Dynamically generates TRANSFORMATION METHOD section based on action types.
    """
    
    # Generate transformation instructions for each approved change
    transformation_instructions = []
    
    for change in approved_changes:
        rec_id = change.get('id', 'REC-???')
        action = change.get('action', 'unknown')
        
        if action == 'merge_entities':
            targets = change.get('targets', [])
            resulting = change.get('resulting_entity_name', targets[0] if targets else 'Merged')
            
            instructions = [f"For {rec_id} ({action}):"]
            instructions.append(f"- Create merged entity {resulting}.")
            instructions.append(f"- Combine all unique attributes from {targets}.")
            instructions.append(f"- Deduplicate similar attributes (keep most descriptive).")
            instructions.append(f"- Update all FK references from {targets} to point to {resulting.lower()}_id.")
            instructions.append(f"- Update all relationships referencing {targets}.")
            instructions.append(f"- Remove original entities: {', '.join(targets)}.")
            transformation_instructions.append('\n'.join(instructions))
            
        elif action == 'flag_for_review':
            instructions = [f"For {rec_id} ({action}):"]
            instructions.append(f"- No changes required (flagged for SME review).")
            transformation_instructions.append('\n'.join(instructions))
            
        else:
            # Generic fallback
            instructions = [f"For {rec_id} ({action}):"]
            instructions.append(f"- Apply action as specified in the change details.")
            transformation_instructions.append('\n'.join(instructions))
    
    transformation_section = '\n\n'.join(transformation_instructions)
    
    prompt = f"""You are a senior data architect and CDM refactoring engine. 
Your task is to APPLY APPROVED CONSOLIDATION CHANGES to an existing Canonical Data Model (CDM).

===============================================================================
INPUTS
===============================================================================

1. CURRENT CDM (JSON):
{json.dumps(cdm, indent=2)}

2. APPROVED CONSOLIDATION ACTIONS (JSON):
{json.dumps(approved_changes, indent=2)}

===============================================================================
CRITICAL RULES
===============================================================================

1. APPLY ONLY THE APPROVED ACTIONS.
   Do not merge or modify any entities not listed above.

2. DO NOT INVENT ANYTHING.
   Only introduce structures explicitly described in the approved changes.

3. PRESERVE ALL BUSINESS MEANING.
   Move attributes faithfully.
   Move relationships faithfully.
   Assign roles exactly as specified.

4. PRESERVE IDENTIFIERS.
   Keep original PKs where entities survive.
   Update FKs only as required by consolidations.

5. OUTPUT MUST BE VALID JSON matching the CDM schema.

===============================================================================
TRANSFORMATION METHOD
===============================================================================

{transformation_section}

===============================================================================
OUTPUT
===============================================================================

Return ONLY the updated CDM JSON using the same structure as the input CDM.

Additionally, include a "consolidation_log" array documenting what was done:
{{
  "domain": "...",
  "cdm_version": "...",
  "entities": [...],
  "consolidation_log": [
    {{
      "id": "REC-001",
      "action": "merge_entities",
      "source_entities": [...],
      "resulting_entity": "...",
      "entities_removed": [...],
      "entities_created": [...],
      "summary": "Brief description of what was done"
    }}
  ]
}}

No explanation, no markdown.
"""
    return prompt


# =============================================================================
# PHASE 3: APPLY CONSOLIDATION
# =============================================================================

def apply_consolidation(
    cdm: Dict,
    approved: Dict,
    llm: LLMClient,
    outdir: Path,
    dry_run: bool = False
) -> Optional[Dict]:
    """
    Phase 3: Apply approved consolidation changes to CDM.
    
    Args:
        cdm: Original foundational CDM
        approved: Output from review_recommendations
        llm: LLM client
        outdir: Output directory
        dry_run: If True, save prompt only
        
    Returns:
        Refined CDM dict (None if dry_run or no changes)
    """
    
    print(f"\n{'='*60}")
    print(f"PHASE 3: APPLY CONSOLIDATION CHANGES")
    print(f"{'='*60}")
    
    approved_changes = approved.get('approved_changes', [])
    
    if not approved_changes:
        print("\n   No approved changes to apply. CDM unchanged.")
        return cdm
    
    original_count = len(cdm.get('entities', []))
    print(f"   Approved changes: {len(approved_changes)}")
    print(f"   Current entity count: {original_count}")
    
    # Build prompt
    prompt = build_apply_prompt(cdm, approved_changes)
    
    # Dry run - save prompt and exit
    if dry_run:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = prompts_dir / f"consolidation_apply_{timestamp}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"\n   ✓ Apply prompt saved: {output_file}")
        print(f"     Characters: {len(prompt):,}")
        return None
    
    # Live mode - call LLM
    print(f"\n   🤖 Calling LLM to apply changes...")
    print(f"      Prompt size: {len(prompt):,} chars (~{len(prompt)//4:,} tokens)")
    
    messages = [
        {
            "role": "system",
            "content": "You are a senior data architect. Return ONLY valid JSON with no markdown."
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
        
        final_count = len(refined_cdm.get('entities', []))
        
        # Report consolidation log
        log = refined_cdm.get('consolidation_log', [])
        if log:
            print(f"\n   📋 Consolidation Log:")
            for entry in log:
                action = entry.get('action', 'unknown')
                source = entry.get('source_entities', [])
                result = entry.get('resulting_entity', 'N/A')
                summary = entry.get('summary', '')
                print(f"      • {action}: {source} → {result}")
                if summary:
                    print(f"        {summary}")
        
        # Basic validation
        if final_count == original_count and approved_changes:
            print(f"\n   ⚠️  WARNING: Entity count unchanged ({original_count})")
            print(f"      Expected consolidation to reduce entity count.")
            print(f"      Review consolidation_log for details.")
        
        # Add/update metadata
        refined_cdm["refined_date"] = datetime.now().isoformat()
        refined_cdm["refinement_type"] = "consolidation"
        refined_cdm["refinement_model"] = llm.model if llm else "unknown"
        refined_cdm["pre_consolidation_entity_count"] = original_count
        refined_cdm["post_consolidation_entity_count"] = final_count
        
        # Save refined CDM
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = cdm.get('domain', 'unknown').lower().replace(' ', '_')
        output_file = outdir / f"cdm_{domain_safe}_consolidated_{timestamp}.json"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(refined_cdm, f, indent=2)
        
        # Save approved changes log
        approved_file = outdir / f"consolidation_approved_{domain_safe}_{timestamp}.json"
        with open(approved_file, 'w', encoding='utf-8') as f:
            json.dump(approved, f, indent=2)
        
        print(f"\n   ✅ Consolidation complete!")
        print(f"      Entities: {original_count} → {final_count}")
        print(f"      Changes logged: {len(log)}")
        print(f"\n   📄 Refined CDM: {output_file}")
        print(f"   📄 Change log:  {approved_file}")
        
        return refined_cdm
        
    except json.JSONDecodeError as e:
        print(f"\n   ❌ ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"   Response preview: {response[:500]}...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_file = outdir / f"consolidation_apply_error_{timestamp}.txt"
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
    
    For consolidation, finds the latest non-consolidated CDM to use as input.
    Returns the file with the latest timestamp.
    """
    domain_safe = domain.lower().replace(' ', '_')
    pattern = f"cdm_{domain_safe}_*.json"
    matches = list(outdir.glob(pattern))
    
    # Exclude consolidated versions, recommendations, and other non-CDM files
    cdm_files = [f for f in matches 
                 if 'consolidated' not in f.name 
                 and 'recommendations' not in f.name
                 and 'approved' not in f.name
                 and 'disposition' not in f.name
                 and 'findings' not in f.name
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


def run_consolidation_refinement(
    config: AppConfig,
    cdm_file: Optional[Path],
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Optional[Dict]:
    """
    Main entry point for consolidation refinement.
    
    Orchestrates all three phases:
      Phase 1: Analyze → recommendations
      Phase 2: Review → approved changes
      Phase 3: Apply → refined CDM
    
    Args:
        config: App configuration
        cdm_file: Path to foundational CDM (None to auto-find latest)
        outdir: Output directory
        llm: LLM client (None if dry_run)
        dry_run: If True, save prompts only
        
    Returns:
        Refined CDM dict (None if dry_run or no changes)
    """
    
    print(f"\n{'='*60}")
    print(f"REFINEMENT - CONSOLIDATION")
    print(f"{'='*60}")
    print(f"   Domain: {config.cdm.domain}")
    
    # Find CDM file if not provided
    if cdm_file is None:
        cdm_file = find_latest_cdm(outdir, config.cdm.domain)
        if cdm_file is None:
            print(f"\n   ❌ ERROR: No CDM file found in {outdir}")
            print(f"      Run 'Build CDM Artifacts' first.")
            return None
    
    print(f"   CDM File: {cdm_file.name}")
    
    # Load CDM
    with open(cdm_file, 'r', encoding='utf-8') as f:
        cdm = json.load(f)
    
    entity_count = len(cdm.get('entities', []))
    print(f"   Entities: {entity_count}")
    
    # Ensure output directory exists
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Get domain info from config
    domain_name = config.cdm.domain
    domain_description = getattr(config.cdm, 'description', None)
    
    # Phase 1: Analyze
    recommendations = analyze_consolidation(
        cdm, llm, outdir, dry_run,
        domain_name=domain_name,
        domain_description=domain_description
    )
    
    if dry_run:
        print(f"\n   🔍 DRY RUN complete. Review prompts in {outdir / 'prompts'}")
        return None
    
    if recommendations is None:
        return None
    
    # Phase 2: Review
    approved = review_recommendations(recommendations)
    
    if not approved.get('approved_changes'):
        print(f"\n   No changes approved. CDM unchanged.")
        return cdm
    
    # Phase 3: Apply
    refined_cdm = apply_consolidation(cdm, approved, llm, outdir, dry_run)
    
    print(f"\n{'='*60}")
    print(f"CONSOLIDATION REFINEMENT COMPLETE")
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