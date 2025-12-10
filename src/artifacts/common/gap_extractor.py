# src/artifacts/common/gap_extractor.py
"""Extract data from gaps and consolidation recommendation files."""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass


@dataclass
class UnmappedField:
    """Unmapped source field."""
    source_type: str
    source_entity: str
    source_attribute: str
    reason: str
    suggested_cdm_entity: str
    suggested_attribute_name: str


@dataclass
class RequiresReviewField:
    """Field requiring SME review."""
    source_type: str
    source_entity: str
    source_attribute: str
    cdm_entity: str
    cdm_attribute: str
    mapping_type: str
    confidence: str
    review_reason: str


@dataclass
class SMEQuestion:
    """Question for SME review."""
    question_id: str
    category: str
    question_text: str
    related_entities: List[str]
    context: str


class GapExtractor:
    """Extract gap analysis data for artifact generation."""
    
    def __init__(
        self, 
        gaps_path: Optional[Path] = None,
        consolidation_path: Optional[Path] = None,
        gaps_dict: Optional[Dict] = None,
        consolidation_dict: Optional[Dict] = None
    ):
        self.gaps = {}
        self.consolidation = {}
        
        if gaps_dict:
            self.gaps = gaps_dict
        elif gaps_path and gaps_path.exists():
            with open(gaps_path, 'r', encoding='utf-8') as f:
                self.gaps = json.load(f)
        
        if consolidation_dict:
            self.consolidation = consolidation_dict
        elif consolidation_path and consolidation_path.exists():
            with open(consolidation_path, 'r', encoding='utf-8') as f:
                self.consolidation = json.load(f)
    
    @property
    def summary(self) -> Dict[str, int]:
        """Get gap summary counts."""
        return self.gaps.get("summary", {
            "total_unmapped": 0,
            "total_requires_review": 0,
            "total_errors": 0
        })
    
    def get_unmapped_fields(self) -> List[UnmappedField]:
        """Get all unmapped source fields."""
        results = []
        for field in self.gaps.get("unmapped_fields", []):
            results.append(UnmappedField(
                source_type=field.get("source_type", ""),
                source_entity=field.get("source_entity", ""),
                source_attribute=field.get("source_attribute", ""),
                reason=field.get("reason", ""),
                suggested_cdm_entity=field.get("suggested_cdm_entity", ""),
                suggested_attribute_name=field.get("suggested_attribute_name", "")
            ))
        return results
    
    def get_requires_review_fields(self) -> List[RequiresReviewField]:
        """Get fields requiring SME review."""
        results = []
        for field in self.gaps.get("requires_review_fields", []):
            results.append(RequiresReviewField(
                source_type=field.get("source_type", ""),
                source_entity=field.get("source_entity", ""),
                source_attribute=field.get("source_attribute", ""),
                cdm_entity=field.get("cdm_entity", ""),
                cdm_attribute=field.get("cdm_attribute", ""),
                mapping_type=field.get("mapping_type", ""),
                confidence=field.get("confidence", ""),
                review_reason=field.get("review_reason", "")
            ))
        return results
    
    def get_sme_questions(self) -> List[SMEQuestion]:
        """Extract SME questions from consolidation recommendations."""
        results = []
        question_num = 1
        
        for rec in self.consolidation.get("consolidation_recommendations", []):
            targets = rec.get("targets", [])
            justification = rec.get("justification", "")
            
            for q in rec.get("questions_for_sme", []):
                results.append(SMEQuestion(
                    question_id=f"Q-{question_num:03d}",
                    category="Consolidation",
                    question_text=q,
                    related_entities=targets,
                    context=justification[:200] + "..." if len(justification) > 200 else justification
                ))
                question_num += 1
        
        # Add questions from analysis notes if present
        analysis_notes = self.consolidation.get("analysis_summary", {}).get("analysis_notes", "")
        if analysis_notes and "?" in analysis_notes:
            # Extract questions from analysis notes
            for sentence in analysis_notes.split("."):
                if "?" in sentence:
                    results.append(SMEQuestion(
                        question_id=f"Q-{question_num:03d}",
                        category="Analysis",
                        question_text=sentence.strip() + ".",
                        related_entities=[],
                        context="From consolidation analysis"
                    ))
                    question_num += 1
        
        return results
    
    def get_unmapped_by_source(self) -> Dict[str, int]:
        """Get unmapped count by source type."""
        return self.gaps.get("summary", {}).get("unmapped_by_source", {})
    
    def get_requires_review_by_source(self) -> Dict[str, int]:
        """Get requires review count by source type."""
        return self.gaps.get("summary", {}).get("requires_review_by_source", {})
    
    def get_consolidation_candidates(self) -> List[Dict]:
        """Get entities flagged for potential consolidation."""
        return self.consolidation.get("consolidation_recommendations", [])
