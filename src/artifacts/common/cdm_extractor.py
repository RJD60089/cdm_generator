# src/artifacts/common/cdm_extractor.py
"""Extract data from Full CDM JSON for artifact generation."""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


@dataclass
class EntitySummary:
    """Summary of a CDM entity."""
    name: str
    description: str
    classification: str
    attribute_count: int
    primary_keys: List[str]
    foreign_keys: List[Dict[str, str]]
    source_coverage: Dict[str, bool]


@dataclass 
class AttributeDetail:
    """Full attribute details."""
    entity_name: str
    attribute_name: str
    description: str
    data_type: str
    max_length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    required: bool
    nullable: bool
    pk: bool
    fk_to: Optional[str]
    classification: str
    is_pii: bool
    is_phi: bool
    business_rules: List[str]
    validation_rules: List[str]
    source_lineage: Dict[str, List[Dict]]


@dataclass
class RelationshipDetail:
    """FK relationship details."""
    parent_entity: str
    parent_key: str
    child_entity: str
    foreign_key: str
    relationship_type: str
    description: str


class CDMExtractor:
    """Extract structured data from Full CDM JSON."""
    
    def __init__(self, cdm_path: Optional[Path] = None, cdm_dict: Optional[Dict] = None):
        if cdm_dict:
            self.cdm = cdm_dict
        elif cdm_path:
            with open(cdm_path, 'r', encoding='utf-8') as f:
                self.cdm = json.load(f)
        else:
            raise ValueError("Must provide cdm_path or cdm_dict")
        
        self._entities = self.cdm.get("entities", [])
    
    @property
    def domain(self) -> str:
        return self.cdm.get("domain", "Unknown")
    
    @property
    def domain_description(self) -> str:
        return self.cdm.get("domain_description", "")
    
    @property
    def version(self) -> str:
        return self.cdm.get("cdm_version", "1.0")
    
    @property
    def generated_date(self) -> str:
        return self.cdm.get("generated_date", "")
    
    @property
    def source_files(self) -> Dict[str, str]:
        return self.cdm.get("source_files", {})
    
    @property
    def entity_count(self) -> int:
        return len(self._entities)
    
    @property
    def attribute_count(self) -> int:
        return sum(len(e.get("attributes", [])) for e in self._entities)
    
    def get_entities(self) -> List[EntitySummary]:
        """Get summary of all entities."""
        results = []
        for entity in self._entities:
            attrs = entity.get("attributes", [])
            pks = [a.get("attribute_name", "") for a in attrs if a.get("pk")]
            
            # Extract FKs from relationships
            fks = []
            for rel in entity.get("relationships", []):
                fks.append({
                    "fk": rel.get("fk", ""),
                    "to_entity": rel.get("to", ""),
                    "to_column": rel.get("to_column", rel.get("fk", ""))
                })
            
            # Source coverage
            lineage = entity.get("source_lineage", {})
            coverage = {
                "guardrails": bool(lineage.get("guardrails")),
                "glue": bool(lineage.get("glue")),
                "ncpdp": bool(lineage.get("ncpdp")),
                "fhir": bool(lineage.get("fhir"))
            }
            
            results.append(EntitySummary(
                name=entity.get("entity_name", ""),
                description=entity.get("description", ""),
                classification=entity.get("classification", ""),
                attribute_count=len(attrs),
                primary_keys=pks,
                foreign_keys=fks,
                source_coverage=coverage
            ))
        return results
    
    def get_all_attributes(self) -> List[AttributeDetail]:
        """Get all attributes across all entities."""
        results = []
        for entity in self._entities:
            entity_name = entity.get("entity_name", "")
            for attr in entity.get("attributes", []):
                # Determine FK target
                fk_to = None
                for rel in entity.get("relationships", []):
                    if rel.get("fk") == attr.get("attribute_name"):
                        fk_to = f"{rel.get('to')}.{rel.get('to_column', rel.get('fk'))}"
                        break
                
                # Extract business rules
                business_rules = [r.get("rule", "") for r in attr.get("business_rules", [])]
                validation_rules = [r.get("rule", "") for r in attr.get("validation_rules", [])]
                
                results.append(AttributeDetail(
                    entity_name=entity_name,
                    attribute_name=attr.get("attribute_name", ""),
                    description=attr.get("description", ""),
                    data_type=attr.get("data_type", "VARCHAR"),
                    max_length=attr.get("max_length"),
                    precision=attr.get("precision"),
                    scale=attr.get("scale"),
                    required=attr.get("required", False),
                    nullable=attr.get("nullable", True),
                    pk=attr.get("pk", False),
                    fk_to=fk_to,
                    classification=attr.get("classification", ""),
                    is_pii=attr.get("is_pii", False),
                    is_phi=attr.get("is_phi", False),
                    business_rules=business_rules,
                    validation_rules=validation_rules,
                    source_lineage=attr.get("source_lineage", {})
                ))
        return results
    
    def get_relationships(self) -> List[RelationshipDetail]:
        """Get all FK relationships."""
        results = []
        for entity in self._entities:
            child_entity = entity.get("entity_name", "")
            for rel in entity.get("relationships", []):
                results.append(RelationshipDetail(
                    parent_entity=rel.get("to", ""),
                    parent_key=rel.get("to_column", rel.get("fk", "")),
                    child_entity=child_entity,
                    foreign_key=rel.get("fk", ""),
                    relationship_type=rel.get("type", "N:1"),
                    description=rel.get("description", "")
                ))
        return results
    
    def get_entity_by_name(self, name: str) -> Optional[Dict]:
        """Get raw entity dict by name."""
        for entity in self._entities:
            if entity.get("entity_name") == name:
                return entity
        return None
    
    def get_source_coverage_summary(self) -> Dict[str, int]:
        """Count entities per source type."""
        coverage = {"guardrails": 0, "glue": 0, "ncpdp": 0, "fhir": 0}
        for entity in self._entities:
            lineage = entity.get("source_lineage", {})
            for src in coverage:
                if lineage.get(src):
                    coverage[src] += 1
        return coverage
    
    def get_attributes_with_rules(self) -> List[AttributeDetail]:
        """Get attributes that have business or validation rules."""
        return [a for a in self.get_all_attributes() 
                if a.business_rules or a.validation_rules]
