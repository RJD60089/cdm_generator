# src/cdm_builder/id_foundational_model.py
"""
Identify Foundational Model

Determines whether the CDM foundation should be based on:
- FHIR (standards-first)
- NCPDP (standards-first)  
- Hybrid (business-first, using Guardrails or Glue)

Process:
1. MATCHING: For each standards entity, compare ALL business attributes (single call per entity)
2. EVALUATION: AI assesses overall match quality and recommends foundation

Comparison sequence:
- Guardrails → FHIR
- Guardrails → NCPDP
- Glue → FHIR
- Glue → NCPDP
- All fail → Hybrid

Input: Config, 4 rationalized files
Output: Foundation selection + disposition file
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field

from src.config.config_parser import AppConfig
from src.core.llm_client import LLMClient


@dataclass
class EntityMatchResult:
    """Result from matching all business attrs against one standards entity"""
    standards_entity: str
    standards_source: str
    business_source: str
    total_business_attrs: int
    total_standards_attrs: int
    matched_pairs: List[Dict[str, Any]]  # [{business_attr, business_entity, standards_attr, confidence, reasoning}]
    unmatched_business: List[Dict[str, str]]  # [{attr, entity}]
    unmatched_standards: List[str]
    match_summary: str


@dataclass 
class ComparisonResult:
    """Full result of one business→standards comparison"""
    business_source: str
    standards_source: str
    entity_results: List[EntityMatchResult]
    total_business_attrs: int
    total_standards_attrs: int
    matched_business_attrs: int
    matched_standards_attrs: int
    business_coverage_pct: float  # matched / total business
    standards_coverage_pct: float  # matched / total standards
    ai_recommendation: str  # USE_AS_FOUNDATION | TRY_NEXT | HYBRID
    ai_reasoning: str
    passed: bool


# =============================================================================
# PROMPT BUILDERS
# =============================================================================

def build_matching_prompt(
    domain: str,
    domain_description: str,
    business_data: Dict[str, Any],
    standards_entity: Dict[str, Any],
    business_source: str,
    standards_source: str
) -> str:
    """Build demanding prompt to match ALL business attrs against ONE standards entity"""
    
    # Collect all business attributes with entity context
    business_attrs = []
    for entity in business_data.get('entities', []):
        entity_name = entity.get('entity_name', '')
        entity_desc = entity.get('description', '')[:100]
        for attr in entity.get('attributes', []):
            business_attrs.append({
                "entity": entity_name,
                "entity_desc": entity_desc,
                "attr": attr.get('attribute_name', ''),
                "type": attr.get('data_type', ''),
                "desc": attr.get('description', '')[:150],
                "required": attr.get('required', False)
            })
    
    # Standards entity attributes
    standards_attrs = []
    for attr in standards_entity.get('attributes', []):
        standards_attrs.append({
            "attr": attr.get('attribute_name', ''),
            "type": attr.get('data_type', ''),
            "desc": attr.get('description', '')[:150],
            "required": attr.get('required', False),
            "cardinality": attr.get('cardinality', {})
        })
    
    prompt = f"""You are an expert data architect performing semantic attribute matching for a CDM foundation assessment.

## CONTEXT

**Domain:** {domain}
**Domain Description:** {domain_description}

**Task:** Determine which business attributes semantically match attributes in the standards entity below.

## CANONICAL FORMAT

IMPORTANT: Both data sets are in the same canonical format with consistent structure:
- attribute_name: standardized name
- data_type: logical data type (VARCHAR, DATE, INTEGER, BOOLEAN, etc.)
- description: business definition
- required: whether mandatory

Use this consistent structure to accelerate your analysis. Compare:
- Semantic meaning (primary - what does it represent?)
- Data type compatibility (secondary - are types compatible?)
- Business context (tertiary - similar usage patterns?)

## STANDARDS ENTITY TO MATCH AGAINST

**Source:** {standards_source}
**Entity:** {standards_entity.get('entity_name', '')}
**Description:** {standards_entity.get('description', '')[:300]}

**Attributes ({len(standards_attrs)}):**
```json
{json.dumps(standards_attrs, indent=2)}
```

## BUSINESS ATTRIBUTES TO EVALUATE

**Source:** {business_source}
**Total Attributes:** {len(business_attrs)}

```json
{json.dumps(business_attrs, indent=2)}
```

## MATCHING REQUIREMENTS

Be THOROUGH and ACCURATE. This determines the CDM foundation approach.

**MATCH if:**
- Attributes represent the SAME business concept (even if named differently)
- Semantic meaning clearly aligns
- Data types are compatible (VARCHAR↔string, DATE↔date, etc.)
- Example: "effective_date" ↔ "period.start" = MATCH (both are start dates)
- Example: "carrier_code" ↔ "identifier" = MATCH (both are business identifiers)

**DO NOT MATCH if:**
- Different business concepts despite similar names
- Superficial similarity only (both are "codes" but represent different things)
- Would lose critical business meaning if mapped

**CONFIDENCE LEVELS:**
- high: Clear semantic equivalence, no ambiguity
- medium: Reasonable match, minor differences in scope
- low: Possible match, needs review
- no_match: No suitable standards attribute

## OUTPUT FORMAT

Return ONLY valid JSON:

```json
{{
  "standards_entity": "{standards_entity.get('entity_name', '')}",
  "standards_source": "{standards_source}",
  "business_source": "{business_source}",
  "matched_pairs": [
    {{
      "business_attr": "attribute_name",
      "business_entity": "entity_name",
      "standards_attr": "standards_attribute_name",
      "confidence": "high|medium|low",
      "reasoning": "Brief explanation of why these match"
    }}
  ],
  "unmatched_business": [
    {{
      "attr": "attribute_name",
      "entity": "entity_name",
      "reason": "Brief explanation why no match exists"
    }}
  ],
  "unmatched_standards": ["attr1", "attr2"],
  "match_summary": "2-3 sentence summary of match quality and coverage"
}}
```

CRITICAL REQUIREMENTS:
- Return ONLY valid JSON (no markdown, no code blocks, no commentary)
- Every business attribute must appear in EITHER matched_pairs OR unmatched_business
- Be thorough - examine EVERY business attribute
- One business attr can match at most ONE standards attr
- Multiple business attrs CAN match the same standards attr
- Provide clear reasoning for matches and non-matches
"""
    
    return prompt


def build_evaluation_prompt(
    domain: str,
    domain_description: str,
    business_source: str,
    standards_source: str,
    entity_results: List[EntityMatchResult],
    total_business_attrs: int,
    total_standards_attrs: int,
    matched_business_attrs: int,
    matched_standards_attrs: int
) -> str:
    """Build prompt for AI evaluation of overall match quality"""
    
    # Summarize matches across all entities
    all_matches = []
    all_unmatched_business = []
    
    for result in entity_results:
        for match in result.matched_pairs:
            all_matches.append({
                "business": f"{match.get('business_entity', '')}.{match.get('business_attr', '')}",
                "standards": f"{result.standards_entity}.{match.get('standards_attr', '')}",
                "confidence": match.get('confidence', ''),
                "reasoning": match.get('reasoning', '')[:100]
            })
        for unmatched in result.unmatched_business:
            all_unmatched_business.append({
                "attr": f"{unmatched.get('entity', '')}.{unmatched.get('attr', '')}",
                "reason": unmatched.get('reason', '')[:100]
            })
    
    business_coverage = matched_business_attrs / total_business_attrs if total_business_attrs > 0 else 0
    standards_coverage = matched_standards_attrs / total_standards_attrs if total_standards_attrs > 0 else 0
    
    prompt = f"""You are an expert data architect evaluating whether a standards model is suitable as a CDM foundation.

## CONTEXT

**Domain:** {domain}
**Domain Description:** {domain_description}

## COMPARISON SUMMARY

**Business Source:** {business_source} ({total_business_attrs} attributes)
**Standards Source:** {standards_source} ({total_standards_attrs} attributes)

**Coverage Metrics:**
- Business attributes matched: {matched_business_attrs}/{total_business_attrs} ({business_coverage:.1%})
- Standards attributes used: {matched_standards_attrs}/{total_standards_attrs} ({standards_coverage:.1%})

## MATCHED ATTRIBUTES ({len(all_matches)} total)

```json
{json.dumps(all_matches[:50], indent=2)}
```
{f"... and {len(all_matches) - 50} more matches" if len(all_matches) > 50 else ""}

## UNMATCHED BUSINESS ATTRIBUTES ({len(all_unmatched_business)} total)

```json
{json.dumps(all_unmatched_business[:30], indent=2)}
```
{f"... and {len(all_unmatched_business) - 30} more unmatched" if len(all_unmatched_business) > 30 else ""}

## EVALUATION CRITERIA

**USE_AS_FOUNDATION** if:
- Most CRITICAL business concepts have standards matches (identifiers, dates, status, core entities)
- Standards coverage is reasonable (>50% of standards attrs map to business)
- Building on this standard makes architectural sense
- Quality of matches matters more than quantity

**HYBRID** if:
- Critical business concepts lack standards representation
- Business model structure differs significantly from standards
- Better to start from business model and incorporate standards where applicable

**TRY_NEXT** if:
- Match quality is borderline
- Worth trying another standard before deciding Hybrid

## OUTPUT FORMAT

Return ONLY valid JSON:

```json
{{
  "recommendation": "USE_AS_FOUNDATION | TRY_NEXT | HYBRID",
  "confidence": "high | medium | low",
  "reasoning": "3-4 sentences explaining your recommendation. Focus on whether CRITICAL business concepts matched, not just percentages.",
  "critical_matches": ["List key business concepts that matched well"],
  "critical_gaps": ["List important business concepts with no match"],
  "assessment": {{
    "core_identifiers_covered": true | false,
    "date_fields_covered": true | false,
    "status_fields_covered": true | false,
    "structural_fit": "good | partial | poor"
  }}
}}
```

CRITICAL: Return ONLY valid JSON. Be decisive.
"""
    
    return prompt


# =============================================================================
# CORE PROCESSING FUNCTIONS
# =============================================================================

def load_rationalized_file(filepath: Path) -> Dict[str, Any]:
    """Load and validate a rationalized file"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if 'entities' not in data:
        raise ValueError(f"Invalid rationalized file (missing 'entities'): {filepath}")
    
    return data


def run_entity_matching(
    config: AppConfig,
    business_data: Dict[str, Any],
    standards_entity: Dict[str, Any],
    business_source: str,
    standards_source: str,
    llm: Optional[LLMClient],
    dry_run: bool,
    prompts_dir: Optional[Path]
) -> Optional[EntityMatchResult]:
    """
    Match ALL business attributes against ONE standards entity
    
    Returns: EntityMatchResult or None (dry run)
    """
    
    entity_name = standards_entity.get('entity_name', 'Unknown')
    standards_attrs = standards_entity.get('attributes', [])
    
    # Count business attrs
    total_business = sum(len(e.get('attributes', [])) for e in business_data.get('entities', []))
    
    print(f"      Matching {total_business} {business_source} attrs → {entity_name} ({len(standards_attrs)} attrs)")
    
    prompt = build_matching_prompt(
        domain=config.cdm.domain,
        domain_description=config.cdm.description,
        business_data=business_data,
        standards_entity=standards_entity,
        business_source=business_source,
        standards_source=standards_source
    )
    
    if dry_run:
        if prompts_dir:
            timestamp = datetime.now().strftime('%H%M%S')
            prompt_file = prompts_dir / f"match_{business_source}_vs_{entity_name}_{timestamp}.txt"
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
            print(f"        Prompt saved: {prompt_file.name}")
        return None
    
    if not llm:
        return None
    
    messages = [
        {"role": "system", "content": "You are a data architect. Return ONLY valid JSON."},
        {"role": "user", "content": prompt}
    ]
    
    try:
        response, _ = llm.chat(messages)
        response_clean = _clean_json_response(response)
        result_data = json.loads(response_clean)
        
        matched_pairs = result_data.get('matched_pairs', [])
        unmatched_business = result_data.get('unmatched_business', [])
        unmatched_standards = result_data.get('unmatched_standards', [])
        match_summary = result_data.get('match_summary', '')
        
        print(f"        ✓ {len(matched_pairs)} matches, {len(unmatched_business)} unmatched")
        
        return EntityMatchResult(
            standards_entity=entity_name,
            standards_source=standards_source,
            business_source=business_source,
            total_business_attrs=total_business,
            total_standards_attrs=len(standards_attrs),
            matched_pairs=matched_pairs,
            unmatched_business=unmatched_business,
            unmatched_standards=unmatched_standards,
            match_summary=match_summary
        )
        
    except Exception as e:
        print(f"        ✗ Error: {e}")
        return None


def run_ai_evaluation(
    config: AppConfig,
    business_source: str,
    standards_source: str,
    entity_results: List[EntityMatchResult],
    total_business_attrs: int,
    total_standards_attrs: int,
    llm: Optional[LLMClient],
    dry_run: bool,
    prompts_dir: Optional[Path]
) -> Tuple[str, str, Dict[str, Any]]:
    """
    Run AI evaluation of match quality
    
    Returns: (recommendation, reasoning, full_evaluation)
    """
    
    # Calculate totals
    matched_business_set = set()
    matched_standards_set = set()
    
    for result in entity_results:
        for match in result.matched_pairs:
            bus_key = f"{match.get('business_entity', '')}.{match.get('business_attr', '')}"
            std_key = f"{result.standards_entity}.{match.get('standards_attr', '')}"
            matched_business_set.add(bus_key)
            matched_standards_set.add(std_key)
    
    matched_business_attrs = len(matched_business_set)
    matched_standards_attrs = len(matched_standards_set)
    
    prompt = build_evaluation_prompt(
        domain=config.cdm.domain,
        domain_description=config.cdm.description,
        business_source=business_source,
        standards_source=standards_source,
        entity_results=entity_results,
        total_business_attrs=total_business_attrs,
        total_standards_attrs=total_standards_attrs,
        matched_business_attrs=matched_business_attrs,
        matched_standards_attrs=matched_standards_attrs
    )
    
    if dry_run:
        if prompts_dir:
            timestamp = datetime.now().strftime('%H%M%S')
            prompt_file = prompts_dir / f"eval_{business_source}_vs_{standards_source}_{timestamp}.txt"
            with open(prompt_file, 'w', encoding='utf-8') as f:
                f.write(prompt)
            print(f"      Eval prompt saved")
        return "DRY_RUN", "Dry run - no evaluation", {}
    
    if not llm:
        return "NO_LLM", "No LLM client", {}
    
    print(f"    Running AI evaluation...")
    
    messages = [
        {"role": "system", "content": "You are a data architect. Return ONLY valid JSON."},
        {"role": "user", "content": prompt}
    ]
    
    try:
        response, _ = llm.chat(messages)
        response_clean = _clean_json_response(response)
        result_data = json.loads(response_clean)
        
        recommendation = result_data.get('recommendation', 'TRY_NEXT')
        reasoning = result_data.get('reasoning', '')
        
        print(f"      Recommendation: {recommendation}")
        print(f"      Reasoning: {reasoning[:100]}...")
        
        return recommendation, reasoning, result_data
        
    except Exception as e:
        print(f"      ✗ Evaluation error: {e}")
        return "ERROR", str(e), {}


def prompt_continue(comparison_name: str, recommendation: str) -> bool:
    """Prompt user whether to continue after a comparison."""
    print(f"\n    ─────────────────────────────────────")
    print(f"    {comparison_name}: {recommendation}")
    response = input(f"    Continue to next comparison? (Y/n): ").strip().lower()
    return response not in ['n', 'no']


def run_comparison(
    config: AppConfig,
    business_data: Dict[str, Any],
    standards_data: Dict[str, Any],
    business_source: str,
    standards_source: str,
    llm: Optional[LLMClient],
    dry_run: bool,
    prompts_dir: Optional[Path]
) -> ComparisonResult:
    """
    Run full comparison: iterate standards entities, then evaluate
    """
    print(f"\n  === Comparing {business_source} → {standards_source} ===")
    
    # Count totals
    total_business = sum(len(e.get('attributes', [])) for e in business_data.get('entities', []))
    total_standards = sum(len(e.get('attributes', [])) for e in standards_data.get('entities', []))
    standards_entities = standards_data.get('entities', [])
    
    print(f"    {business_source}: {total_business} attributes")
    print(f"    {standards_source}: {total_standards} attributes in {len(standards_entities)} entities")
    
    # Match against each standards entity
    print(f"\n    [MATCHING - {len(standards_entities)} calls]")
    entity_results = []
    
    for std_entity in standards_entities:
        result = run_entity_matching(
            config=config,
            business_data=business_data,
            standards_entity=std_entity,
            business_source=business_source,
            standards_source=standards_source,
            llm=llm,
            dry_run=dry_run,
            prompts_dir=prompts_dir
        )
        if result:
            entity_results.append(result)
    
    if dry_run:
        return ComparisonResult(
            business_source=business_source,
            standards_source=standards_source,
            entity_results=[],
            total_business_attrs=total_business,
            total_standards_attrs=total_standards,
            matched_business_attrs=0,
            matched_standards_attrs=0,
            business_coverage_pct=0.0,
            standards_coverage_pct=0.0,
            ai_recommendation="DRY_RUN",
            ai_reasoning="Dry run mode",
            passed=False
        )
    
    # Calculate coverage
    matched_business_set = set()
    matched_standards_set = set()
    
    for result in entity_results:
        for match in result.matched_pairs:
            bus_key = f"{match.get('business_entity', '')}.{match.get('business_attr', '')}"
            std_key = f"{result.standards_entity}.{match.get('standards_attr', '')}"
            matched_business_set.add(bus_key)
            matched_standards_set.add(std_key)
    
    matched_business = len(matched_business_set)
    matched_standards = len(matched_standards_set)
    business_coverage = matched_business / total_business if total_business > 0 else 0
    standards_coverage = matched_standards / total_standards if total_standards > 0 else 0
    
    print(f"\n    Coverage:")
    print(f"      Business: {matched_business}/{total_business} ({business_coverage:.1%})")
    print(f"      Standards: {matched_standards}/{total_standards} ({standards_coverage:.1%})")
    
    # AI Evaluation
    print(f"\n    [EVALUATION]")
    recommendation, reasoning, eval_details = run_ai_evaluation(
        config=config,
        business_source=business_source,
        standards_source=standards_source,
        entity_results=entity_results,
        total_business_attrs=total_business,
        total_standards_attrs=total_standards,
        llm=llm,
        dry_run=dry_run,
        prompts_dir=prompts_dir
    )
    
    passed = recommendation == "USE_AS_FOUNDATION"
    
    return ComparisonResult(
        business_source=business_source,
        standards_source=standards_source,
        entity_results=entity_results,
        total_business_attrs=total_business,
        total_standards_attrs=total_standards,
        matched_business_attrs=matched_business,
        matched_standards_attrs=matched_standards,
        business_coverage_pct=business_coverage,
        standards_coverage_pct=standards_coverage,
        ai_recommendation=recommendation,
        ai_reasoning=reasoning,
        passed=passed
    )


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def _clean_json_response(response: str) -> str:
    """Clean LLM response to extract JSON"""
    response_clean = response.strip()
    if response_clean.startswith("```"):
        lines = response_clean.split("\n")
        if lines[0].strip().lower() in ["```json", "```"]:
            response_clean = "\n".join(lines[1:])
        if response_clean.endswith("```"):
            response_clean = response_clean[:-3]
    return response_clean.strip()


def build_disposition_report(
    domain: str,
    comparisons: List[ComparisonResult],
    foundation_selected: str,
    foundation_source_file: Optional[str],
    user_stopped: bool = False
) -> Dict[str, Any]:
    """Build disposition report from comparison results"""
    
    # Collect all match details
    all_matches = []
    all_unmatched = []
    
    for comp in comparisons:
        for result in comp.entity_results:
            for match in result.matched_pairs:
                all_matches.append({
                    "business_source": comp.business_source,
                    "business_entity": match.get('business_entity', ''),
                    "business_attr": match.get('business_attr', ''),
                    "standards_source": comp.standards_source,
                    "standards_entity": result.standards_entity,
                    "standards_attr": match.get('standards_attr', ''),
                    "confidence": match.get('confidence', ''),
                    "reasoning": match.get('reasoning', '')
                })
            
            for unmatched in result.unmatched_business:
                all_unmatched.append({
                    "business_source": comp.business_source,
                    "business_entity": unmatched.get('entity', ''),
                    "business_attr": unmatched.get('attr', ''),
                    "standards_source": comp.standards_source,
                    "reason": unmatched.get('reason', '')
                })
    
    return {
        "domain": domain,
        "step": "id_foundational_model",
        "timestamp": datetime.now().isoformat(),
        "foundation_selected": foundation_selected,
        "foundation_source_file": foundation_source_file,
        "user_stopped_early": user_stopped,
        "disposition_report": {
            "summary": {
                "comparisons_performed": [
                    {
                        "business_source": c.business_source,
                        "standards_source": c.standards_source,
                        "business_coverage": f"{c.business_coverage_pct:.1%}",
                        "standards_coverage": f"{c.standards_coverage_pct:.1%}",
                        "matched_business_attrs": c.matched_business_attrs,
                        "matched_standards_attrs": c.matched_standards_attrs,
                        "ai_recommendation": c.ai_recommendation,
                        "passed": c.passed
                    }
                    for c in comparisons
                ],
                "foundation_selected": foundation_selected,
                "user_stopped_early": user_stopped
            },
            "matched_attributes": all_matches,
            "unmatched_attributes": all_unmatched
        }
    }


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def run_id_foundational_model(
    config: AppConfig,
    rationalized_files: Dict[str, Path],
    outdir: Path,
    llm: Optional[LLMClient],
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Identify the foundational model for CDM generation.
    
    Args:
        config: Application configuration
        rationalized_files: Dict with keys 'fhir', 'ncpdp', 'guardrails', 'glue' mapping to file paths
        outdir: Output directory for disposition file
        llm: LLM client (None in dry run)
        dry_run: If True, save prompts without calling LLM
    
    Returns:
        Dict with foundation selection and disposition
    """
    
    print(f"\n{'='*60}")
    print(f"IDENTIFY FOUNDATIONAL MODEL")
    print(f"Domain: {config.cdm.domain}")
    print(f"{'='*60}")
    
    # Setup prompts directory for dry run
    prompts_dir = None
    if dry_run:
        prompts_dir = outdir / "prompts"
        prompts_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n🔍 DRY RUN MODE - Prompts will be saved to: {prompts_dir}")
    
    # Load all rationalized files
    print(f"\n📂 Loading rationalized files...")
    
    data = {}
    for source, filepath in rationalized_files.items():
        if filepath and filepath.exists():
            data[source] = load_rationalized_file(filepath)
            entity_count = len(data[source].get('entities', []))
            attr_count = sum(len(e.get('attributes', [])) for e in data[source].get('entities', []))
            print(f"  ✓ {source}: {entity_count} entities, {attr_count} attributes")
        else:
            print(f"  ⚠ {source}: file not found or not provided")
            data[source] = {'entities': []}
    
    # Define comparison sequence
    comparison_sequence = [
        ('guardrails', 'fhir'),
        ('guardrails', 'ncpdp'),
        ('glue', 'fhir'),
        ('glue', 'ncpdp')
    ]
    
    comparisons = []
    foundation_selected = None
    foundation_source_file = None
    user_stopped = False
    
    # Run comparisons until one passes or user stops
    for business_key, standards_key in comparison_sequence:
        business_data = data.get(business_key, {'entities': []})
        standards_data = data.get(standards_key, {'entities': []})
        
        # Skip if either side is empty
        if not business_data.get('entities') or not standards_data.get('entities'):
            print(f"\n  Skipping {business_key} → {standards_key}: missing data")
            continue
        
        result = run_comparison(
            config=config,
            business_data=business_data,
            standards_data=standards_data,
            business_source=business_key.capitalize(),
            standards_source=standards_key.upper(),
            llm=llm,
            dry_run=dry_run,
            prompts_dir=prompts_dir
        )
        
        comparisons.append(result)
        
        if result.passed:
            foundation_selected = standards_key.upper()
            foundation_source_file = str(rationalized_files.get(standards_key, ''))
            print(f"\n  ✓ FOUNDATION SELECTED: {foundation_selected}")
            break
        
        # Prompt user to continue (skip in dry run)
        if not dry_run:
            comparison_name = f"{result.business_source} → {result.standards_source}"
            if not prompt_continue(comparison_name, result.ai_recommendation):
                print(f"\n  User stopped after {comparison_name}")
                user_stopped = True
                break
    
    # If all failed, use Hybrid
    if foundation_selected is None:
        foundation_selected = "Hybrid"
        foundation_source_file = str(rationalized_files.get('guardrails', ''))
        if user_stopped:
            print(f"\n  → User stopped comparisons, using Guardrails as Hybrid foundation")
        else:
            print(f"\n  → All comparisons recommend Hybrid, using Guardrails as foundation")
    
    # Build disposition report
    disposition = build_disposition_report(
        domain=config.cdm.domain,
        comparisons=comparisons,
        foundation_selected=foundation_selected,
        foundation_source_file=foundation_source_file,
        user_stopped=user_stopped
    )
    
    # Save disposition file
    outdir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = config.cdm.domain.replace(' ', '_')
    disposition_file = outdir / f"foundation_disposition_{domain_safe}_{timestamp}.json"
    
    with open(disposition_file, 'w', encoding='utf-8') as f:
        json.dump(disposition, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"RESULT: Foundation = {foundation_selected}")
    print(f"Disposition saved: {disposition_file}")
    print(f"{'='*60}\n")
    
    return {
        "foundation_selected": foundation_selected,
        "foundation_source_file": foundation_source_file,
        "disposition_file": str(disposition_file),
        "comparisons_run": len(comparisons)
    }


# =============================================================================
# STANDALONE EXECUTION (for testing)
# =============================================================================

if __name__ == "__main__":
    import argparse
    from src.config import load_config
    
    parser = argparse.ArgumentParser(description="Identify Foundational Model")
    parser.add_argument("config", help="Config file path")
    parser.add_argument("--dry-run", action="store_true", help="Save prompts only")
    parser.add_argument("--fhir", help="Path to rationalized FHIR file")
    parser.add_argument("--ncpdp", help="Path to rationalized NCPDP file")
    parser.add_argument("--guardrails", help="Path to rationalized Guardrails file")
    parser.add_argument("--glue", help="Path to rationalized Glue file")
    parser.add_argument("--outdir", default="output", help="Output directory")
    
    args = parser.parse_args()
    
    config = load_config(args.config)
    
    rationalized_files = {
        'fhir': Path(args.fhir) if args.fhir else None,
        'ncpdp': Path(args.ncpdp) if args.ncpdp else None,
        'guardrails': Path(args.guardrails) if args.guardrails else None,
        'glue': Path(args.glue) if args.glue else None
    }
    
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    
    llm = None
    if not args.dry_run:
        from src.core.llm_client import LLMClient
        llm = LLMClient()
    
    run_id_foundational_model(
        config=config,
        rationalized_files=rationalized_files,
        outdir=outdir,
        llm=llm,
        dry_run=args.dry_run
    )