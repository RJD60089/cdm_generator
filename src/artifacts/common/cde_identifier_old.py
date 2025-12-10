# src/artifacts/common/cde_identifier.py
"""Identify Critical Data Elements (CDEs) from CDM attributes."""

from typing import List, Dict, Any
from dataclasses import dataclass
from src.artifacts.common.cdm_extractor import CDMExtractor, AttributeDetail


@dataclass
class CriticalDataElement:
    """A Critical Data Element with justification."""
    entity_name: str
    attribute_name: str
    cde_reasons: List[str]
    business_justification: str
    data_type: str
    is_pii: bool
    is_phi: bool


class CDEIdentifier:
    """Identify Critical Data Elements based on defined criteria."""
    
    # CDE selection criteria (for fallback pattern matching)
    CRITERIA = {
        "pk": "Primary Key - Entity identifier",
        "fk": "Foreign Key - Relationship integrity",
        "business_id": "Business Identifier - External reference",
        "regulatory": "Regulatory/Compliance - Required for reporting",
        "pii": "PII - Requires privacy protection",
        "phi": "PHI - Protected health information",
        "effective_date": "Temporal - Defines validity period",
        "status": "Status - Operational state indicator",
        "code": "Code - Standardized classification"
    }
    
    # Patterns that indicate business identifiers
    BUSINESS_ID_PATTERNS = [
        "_code", "_id", "_number", "_identifier", "_key",
        "bin", "pcn", "ndc", "npi", "tin", "ssn"
    ]
    
    # Patterns that indicate regulatory/date fields
    TEMPORAL_PATTERNS = [
        "effective", "termination", "expiration", "start_date", "end_date",
        "created_at", "updated_at"
    ]
    
    STATUS_PATTERNS = ["status", "state", "active", "flag"]
    
    def __init__(self, extractor: CDMExtractor):
        self.extractor = extractor
        self._ai_cdes = self.extractor.cdm.get("critical_data_elements", [])
    
    def identify_cdes(self) -> List[CriticalDataElement]:
        """
        Get CDEs - uses AI-identified if present, otherwise falls back to pattern matching.
        """
        # Prefer AI-identified CDEs
        if self._ai_cdes:
            return self._get_ai_cdes()
        
        # Fallback to pattern matching
        return self._identify_by_pattern()
    
    def _get_ai_cdes(self) -> List[CriticalDataElement]:
        """Extract CDEs from AI-identified list in CDM."""
        cdes = []
        all_attrs = {(a.entity_name, a.attribute_name): a 
                     for a in self.extractor.get_all_attributes()}
        
        for cde_data in self._ai_cdes:
            entity = cde_data.get("entity", "")
            attribute = cde_data.get("attribute", "")
            justification = cde_data.get("justification", "")
            
            # Find matching attribute for metadata
            attr = all_attrs.get((entity, attribute))
            
            cdes.append(CriticalDataElement(
                entity_name=entity,
                attribute_name=attribute,
                cde_reasons=["candidate"],
                business_justification=justification,
                data_type=attr.data_type if attr else "VARCHAR",
                is_pii=attr.is_pii if attr else False,
                is_phi=attr.is_phi if attr else False
            ))
        
        return cdes
    
    def _identify_by_pattern(self) -> List[CriticalDataElement]:
        """Fallback: identify CDEs using pattern matching."""
        cdes = []
        
        for attr in self.extractor.get_all_attributes():
            reasons = self._evaluate_attribute(attr)
            
            if reasons:
                justification = self._build_justification(attr, reasons)
                cdes.append(CriticalDataElement(
                    entity_name=attr.entity_name,
                    attribute_name=attr.attribute_name,
                    cde_reasons=reasons,
                    business_justification=justification,
                    data_type=attr.data_type,
                    is_pii=attr.is_pii,
                    is_phi=attr.is_phi
                ))
        
        return cdes
    
    def _evaluate_attribute(self, attr: AttributeDetail) -> List[str]:
        """Evaluate attribute against CDE criteria."""
        reasons = []
        name_lower = attr.attribute_name.lower()
        
        # Primary Key
        if attr.pk:
            reasons.append("pk")
        
        # Foreign Key
        if attr.fk_to:
            reasons.append("fk")
        
        # Business Identifier patterns
        if any(p in name_lower for p in self.BUSINESS_ID_PATTERNS):
            if "pk" not in reasons and "fk" not in reasons:
                reasons.append("business_id")
        
        # PII/PHI
        if attr.is_pii:
            reasons.append("pii")
        if attr.is_phi:
            reasons.append("phi")
        
        # Temporal/Effective dates
        if any(p in name_lower for p in self.TEMPORAL_PATTERNS):
            reasons.append("effective_date")
        
        # Status fields
        if any(p in name_lower for p in self.STATUS_PATTERNS):
            reasons.append("status")
        
        # Code fields (external standards)
        if name_lower.endswith("_code") or name_lower.endswith("_type"):
            if "business_id" not in reasons:
                reasons.append("code")
        
        return reasons
    
    def _build_justification(self, attr: AttributeDetail, reasons: List[str]) -> str:
        """Build business justification text."""
        parts = []
        
        for reason in reasons:
            if reason in self.CRITERIA:
                parts.append(self.CRITERIA[reason])
        
        # Add context from description if available
        if attr.description and len(parts) < 3:
            desc_snippet = attr.description[:100]
            if len(attr.description) > 100:
                desc_snippet += "..."
            parts.append(f"Context: {desc_snippet}")
        
        return "; ".join(parts)
    
    def get_cdes_by_entity(self) -> Dict[str, List[CriticalDataElement]]:
        """Group CDEs by entity."""
        cdes = self.identify_cdes()
        by_entity = {}
        
        for cde in cdes:
            if cde.entity_name not in by_entity:
                by_entity[cde.entity_name] = []
            by_entity[cde.entity_name].append(cde)
        
        return by_entity
    
    def get_cde_summary(self) -> Dict[str, int]:
        """Get count of CDEs by reason."""
        cdes = self.identify_cdes()
        summary = {k: 0 for k in self.CRITERIA}
        
        for cde in cdes:
            for reason in cde.cde_reasons:
                if reason in summary:
                    summary[reason] += 1
        
        return summary