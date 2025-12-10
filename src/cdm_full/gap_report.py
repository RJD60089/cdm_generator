# src/cdm_full/gap_report.py
"""
Gap report and summary generation for Full CDM.

Functions:
  - generate_gap_report(): Create gap analysis report for unmapped fields
  - generate_summary(): Create summary statistics for Full CDM
"""
from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def generate_gap_report(
    application_report: Dict, 
    full_cdm_dir: Path, 
    domain: str
) -> Optional[Path]:
    """
    Generate gap report for unmapped fields and low-confidence mappings requiring review.
    
    Args:
        application_report: Report from apply_match_files()
        full_cdm_dir: Output directory for gap report
        domain: CDM domain name
    
    Returns:
        Path to gap report file (None if no gaps)
    """
    
    unmapped = application_report.get("unmapped_fields", [])
    requires_review = application_report.get("requires_review_fields", [])
    errors = application_report.get("application_errors", [])
    
    if not unmapped and not requires_review and not errors:
        print(f"   ✓ No gaps - all source fields mapped successfully with high confidence")
        return None
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_safe = domain.lower().replace(' ', '_')
    
    gap_report = {
        "domain": domain,
        "generated_timestamp": datetime.now().isoformat(),
        "summary": {
            "total_unmapped": len(unmapped),
            "total_requires_review": len(requires_review),
            "total_errors": len(errors),
            "unmapped_by_source": {},
            "requires_review_by_source": {}
        },
        "unmapped_fields": unmapped,
        "requires_review_fields": requires_review,
        "application_errors": errors,
        "suggested_cdm_additions": []
    }
    
    # Group unmapped by source
    for field in unmapped:
        source = field.get("source_type")
        if source not in gap_report["summary"]["unmapped_by_source"]:
            gap_report["summary"]["unmapped_by_source"][source] = 0
        gap_report["summary"]["unmapped_by_source"][source] += 1
    
    # Group requires_review by source
    for field in requires_review:
        source = field.get("source_type")
        if source not in gap_report["summary"]["requires_review_by_source"]:
            gap_report["summary"]["requires_review_by_source"][source] = 0
        gap_report["summary"]["requires_review_by_source"][source] += 1
    
    # Suggest CDM additions (group by suggested entity)
    suggestions = {}
    for field in unmapped:
        suggested_entity = field.get("suggested_cdm_entity")
        suggested_attr = field.get("suggested_attribute_name")
        if suggested_entity and suggested_attr:
            if suggested_entity not in suggestions:
                suggestions[suggested_entity] = []
            suggestions[suggested_entity].append({
                "attribute": suggested_attr,
                "source": f"{field.get('source_type')}.{field.get('source_entity')}.{field.get('source_attribute')}"
            })
    
    gap_report["suggested_cdm_additions"] = [
        {"entity": k, "suggested_attributes": v} 
        for k, v in suggestions.items()
    ]
    
    gap_file = full_cdm_dir / f"gaps_{domain_safe}_{timestamp}.json"
    with open(gap_file, 'w', encoding='utf-8') as f:
        json.dump(gap_report, f, indent=2)
    
    print(f"   ⚠️  Gap report: {gap_file.name}")
    print(f"      Unmapped: {len(unmapped)}, Requires Review: {len(requires_review)}, Errors: {len(errors)}")
    
    return gap_file


def generate_summary(full_cdm: Dict, source_types: List[str]) -> Dict:
    """
    Generate summary statistics for Full CDM.
    
    Args:
        full_cdm: Full CDM dict
        source_types: List of source types processed
    
    Returns:
        Summary statistics dict
    """
    
    total_entities = len(full_cdm.get("entities", []))
    total_attrs = sum(len(e.get("attributes", [])) for e in full_cdm.get("entities", []))
    total_rels = sum(len(e.get("relationships", [])) for e in full_cdm.get("entities", []))
    
    # Count attributes with mappings from each source
    attr_coverage = {st: 0 for st in source_types}
    for entity in full_cdm.get("entities", []):
        for attr in entity.get("attributes", []):
            lineage = attr.get("source_lineage", {})
            for source in source_types:
                if lineage.get(source):
                    attr_coverage[source] += 1
    
    return {
        "total_entities": total_entities,
        "total_attributes": total_attrs,
        "total_relationships": total_rels,
        "attribute_coverage_by_source": attr_coverage
    }