# src/core/validators.py
"""
Validation schemas and utilities for CDM generation outputs.
Uses Pydantic for type-safe validation.
"""
from __future__ import annotations
from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field, validator, field_validator
import logging


logger = logging.getLogger(__name__)


# ============================================================================
# Step 1: Requirements & Scope
# ============================================================================

class EntitySchema(BaseModel):
    """Schema for an entity definition."""
    name: str = Field(..., min_length=1, description="Entity name in PascalCase")
    definition: str = Field(..., min_length=10, description="Business definition")
    is_core: bool = Field(..., description="Whether this is a core entity")
    notes: Optional[str] = Field(None, description="Additional notes")
    
    @field_validator('name')
    @classmethod
    def name_must_be_pascal_case(cls, v: str) -> str:
        """Validate entity name follows PascalCase convention."""
        if not v:
            raise ValueError("Entity name cannot be empty")
        
        if not v[0].isupper():
            raise ValueError(f"Entity name '{v}' must start with uppercase (PascalCase)")
        
        if '_' in v:
            logger.warning(f"Entity name '{v}' contains underscore, should be PascalCase")
        
        return v


class CoreFunctionalMapSchema(BaseModel):
    """Schema for core functional mapping."""
    component: str = Field(..., min_length=1, description="Component or capability name")
    scope: str = Field(..., min_length=1, description="Scope description")
    rationale: str = Field(..., min_length=1, description="Rationale for inclusion")


class ReferenceSetSchema(BaseModel):
    """Schema for reference data sets."""
    name: str = Field(..., min_length=1, description="Reference set name")
    description: str = Field(..., min_length=1, description="What this reference set contains")
    source_ref: Optional[str] = Field(None, description="Source or standard reference")
    local_stub: Optional[str] = Field(None, description="Local naming stub")


class ConfidenceSchema(BaseModel):
    """Schema for confidence scoring."""
    tab: str = Field(..., description="Tab or section name")
    score: int = Field(..., ge=1, le=10, description="Confidence score 1-10")


class Step1OutputSchema(BaseModel):
    """Validation schema for Step 1 output."""
    assumptions: List[str] = Field(default_factory=list, description="Key assumptions made")
    decisions: List[str] = Field(default_factory=list, description="Architectural decisions")
    open_questions: List[str] = Field(default_factory=list, description="Questions to resolve")
    entities: List[EntitySchema] = Field(..., min_length=1, description="Entity definitions")
    core_functional_map: List[CoreFunctionalMapSchema] = Field(
        default_factory=list,
        description="Core functional mapping"
    )
    reference_sets: List[ReferenceSetSchema] = Field(
        default_factory=list,
        description="Reference data sets"
    )
    confidence: ConfidenceSchema = Field(..., description="Confidence assessment")
    
    @field_validator('entities')
    @classmethod
    def entities_must_have_unique_names(cls, v: List[EntitySchema]) -> List[EntitySchema]:
        """Ensure entity names are unique."""
        names = [e.name for e in v]
        duplicates = [name for name in names if names.count(name) > 1]
        
        if duplicates:
            raise ValueError(f"Duplicate entity names found: {set(duplicates)}")
        
        return v


# ============================================================================
# Step 2: Entity Structure & Relationships
# ============================================================================

class RelationshipSchema(BaseModel):
    """Schema for entity relationship."""
    from_entity: str = Field(..., min_length=1, description="Source entity name")
    to_entity: str = Field(..., min_length=1, description="Target entity name")
    cardinality: str = Field(
        ...,
        description="Relationship cardinality (e.g., '1:M', 'M:M', '1:1')"
    )
    relationship_type: Optional[str] = Field(
        None,
        description="Type of relationship (e.g., 'references', 'contains')"
    )
    notes: Optional[str] = Field(None, description="Additional notes")
    
    @field_validator('cardinality')
    @classmethod
    def validate_cardinality(cls, v: str) -> str:
        """Validate cardinality format."""
        valid_patterns = ['1:1', '1:M', 'M:1', 'M:M', '0:1', '0:M', '1:0', 'M:0']
        
        if v.upper() not in [p.upper() for p in valid_patterns]:
            logger.warning(
                f"Cardinality '{v}' doesn't match standard patterns: {valid_patterns}"
            )
        
        return v


class Step2OutputSchema(BaseModel):
    """Validation schema for Step 2 output."""
    entities: List[EntitySchema] = Field(..., description="Updated entity list")
    relationships: List[RelationshipSchema] = Field(
        ...,
        min_length=1,
        description="Entity relationships"
    )
    assumptions: List[str] = Field(default_factory=list)
    decisions: List[str] = Field(default_factory=list)
    open_questions: List[str] = Field(default_factory=list)


# ============================================================================
# Step 3: Attributes & Fields
# ============================================================================

class AttributeSchema(BaseModel):
    """Schema for an entity attribute/field."""
    entity_name: str = Field(..., min_length=1, description="Entity this attribute belongs to")
    field_name: str = Field(..., min_length=1, description="Field name in snake_case")
    data_type: str = Field(..., min_length=1, description="Logical data type")
    description: str = Field(..., min_length=10, description="Business description")
    is_nullable: bool = Field(..., description="Whether field can be NULL")
    is_pk: bool = Field(default=False, description="Whether this is a primary key")
    is_fk: bool = Field(default=False, description="Whether this is a foreign key")
    references_entity: Optional[str] = Field(
        None,
        description="Entity this FK references (if is_fk=True)"
    )
    classification: Optional[str] = Field(
        None,
        description="Data classification (PII/PHI/Sensitive/Operational/Reference)"
    )
    source_example: Optional[str] = Field(
        None,
        description="Example source system or file"
    )
    business_rules: Optional[str] = Field(
        None,
        description="Business rules or constraints"
    )
    
    @field_validator('field_name')
    @classmethod
    def field_name_must_be_snake_case(cls, v: str) -> str:
        """Validate field name follows snake_case convention."""
        if not v:
            raise ValueError("Field name cannot be empty")
        
        if v[0].isupper():
            logger.warning(f"Field name '{v}' starts with uppercase, should be snake_case")
        
        if '-' in v or ' ' in v:
            raise ValueError(f"Field name '{v}' contains invalid characters")
        
        return v
    
    @field_validator('data_type')
    @classmethod
    def validate_data_type(cls, v: str) -> str:
        """Validate data type is reasonable."""
        common_types = [
            'string', 'varchar', 'text',
            'int', 'integer', 'bigint', 'smallint',
            'decimal', 'numeric', 'float', 'double',
            'date', 'datetime', 'timestamp',
            'boolean', 'bool',
            'uuid', 'guid'
        ]
        
        v_lower = v.lower()
        base_type = v_lower.split('(')[0].strip()  # Handle VARCHAR(200)
        
        if base_type not in [t.lower() for t in common_types]:
            logger.warning(f"Uncommon data type: '{v}'")
        
        return v
    
    def model_post_init(self, __context: Any) -> None:
        """Additional validation after model creation."""
        # If is_fk is True, references_entity must be set
        if self.is_fk and not self.references_entity:
            logger.warning(
                f"Field '{self.field_name}' marked as FK but references_entity not set"
            )


class Step3OutputSchema(BaseModel):
    """Validation schema for Step 3 output."""
    attributes: List[AttributeSchema] = Field(
        ...,
        min_length=1,
        description="Attribute definitions"
    )
    entities: List[EntitySchema] = Field(default_factory=list)
    relationships: List[RelationshipSchema] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    decisions: List[str] = Field(default_factory=list)
    open_questions: List[str] = Field(default_factory=list)
    
    @field_validator('attributes')
    @classmethod
    def validate_pk_fk_consistency(cls, v: List[AttributeSchema]) -> List[AttributeSchema]:
        """Ensure PK/FK relationships are consistent."""
        # Group by entity
        by_entity: Dict[str, List[AttributeSchema]] = {}
        for attr in v:
            if attr.entity_name not in by_entity:
                by_entity[attr.entity_name] = []
            by_entity[attr.entity_name].append(attr)
        
        # Check each entity has a PK
        for entity_name, attrs in by_entity.items():
            pks = [a for a in attrs if a.is_pk]
            if not pks:
                logger.warning(f"Entity '{entity_name}' has no primary key defined")
            elif len(pks) > 1:
                logger.info(
                    f"Entity '{entity_name}' has composite key with {len(pks)} fields"
                )
        
        return v


# ============================================================================
# Step 4: DDL Generation
# ============================================================================

class DDLOutputSchema(BaseModel):
    """Validation schema for Step 4 output (DDL)."""
    ddl_statements: List[str] = Field(
        ...,
        min_length=1,
        description="SQL DDL CREATE TABLE statements"
    )
    tables_created: List[str] = Field(
        ...,
        min_length=1,
        description="List of table names created"
    )
    foreign_keys: List[Dict[str, str]] = Field(
        default_factory=list,
        description="Foreign key relationships"
    )


# ============================================================================
# Step 5: Validation & Alignment
# ============================================================================

class AlignmentIssueSchema(BaseModel):
    """Schema for alignment issue."""
    category: str = Field(..., description="Issue category")
    severity: Literal["high", "medium", "low"] = Field(..., description="Issue severity")
    description: str = Field(..., description="Issue description")
    recommendation: str = Field(..., description="Recommended action")


class Step5OutputSchema(BaseModel):
    """Validation schema for Step 5 output."""
    overlaps_identified: List[str] = Field(default_factory=list)
    missing_lookups: List[str] = Field(default_factory=list)
    passthrough_attributes: List[str] = Field(default_factory=list)
    naming_tweaks: List[str] = Field(default_factory=list)
    alignment_issues: List[AlignmentIssueSchema] = Field(default_factory=list)
    readiness_checklist: List[str] = Field(default_factory=list)
    final_recommendations: List[str] = Field(default_factory=list)


# ============================================================================
# Validation Functions
# ============================================================================

def validate_step_output(
    step_num: int,
    output_data: Dict[str, Any]
) -> tuple[bool, str, BaseModel | None]:
    """
    Validate output data for a specific step.
    
    Args:
        step_num: Step number (1-5)
        output_data: Output data dictionary to validate
    
    Returns:
        Tuple of (is_valid, error_message, validated_model)
        If valid, error_message is empty and validated_model is the Pydantic model
        If invalid, error_message contains details and validated_model is None
    """
    schema_map = {
        1: Step1OutputSchema,
        2: Step2OutputSchema,
        3: Step3OutputSchema,
        4: DDLOutputSchema,
        5: Step5OutputSchema,
    }
    
    schema = schema_map.get(step_num)
    
    if not schema:
        logger.warning(f"No validation schema defined for step {step_num}")
        return True, "", None
    
    try:
        validated = schema(**output_data)
        logger.info(f"Step {step_num} output validation passed")
        return True, "", validated
    
    except Exception as e:
        error_msg = f"Step {step_num} validation failed: {str(e)}"
        logger.error(error_msg)
        return False, error_msg, None


def validate_naming_conventions(
    entity_name: str | None = None,
    field_name: str | None = None,
    naming_rules: Dict[str, Any] | None = None
) -> List[str]:
    """
    Validate names against naming conventions.
    
    Args:
        entity_name: Entity name to validate
        field_name: Field name to validate
        naming_rules: Naming rules dictionary
    
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    if entity_name:
        # Check PascalCase
        if not entity_name[0].isupper():
            errors.append(f"Entity '{entity_name}' should start with uppercase")
        
        if '_' in entity_name:
            errors.append(f"Entity '{entity_name}' should use PascalCase, not snake_case")
    
    if field_name:
        # Check snake_case
        if field_name[0].isupper():
            errors.append(f"Field '{field_name}' should start with lowercase")
        
        if '-' in field_name or ' ' in field_name:
            errors.append(f"Field '{field_name}' should use snake_case with underscores")
    
    return errors
