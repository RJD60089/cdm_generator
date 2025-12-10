"""
FHIR Rationalization Module
Transforms FHIR StructureDefinitions, ValueSets, and CodeSystems into unified rationalized format
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple


class FHIRRationalizer:
    def __init__(self, config_path: str, llm: Optional[Any] = None, dry_run: bool = False):
        self.llm = llm
        self.dry_run = dry_run
        self.prompts_dir: Optional[Path] = None
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.cdm_domain = self.config.get('cdm', {}).get('domain', '')
        self.cdm_classification = self.config.get('cdm', {}).get('type', 'Core')
        self.cdm_description = self.config.get('cdm', {}).get('description', '')
        
        # Parse FHIR IGs from config
        input_files = self.config.get('input_files', {})
        self.fhir_igs = input_files.get('fhir_igs', [])
        
        # Separate by file type
        self.structure_definitions = []
        self.value_sets = []
        self.code_systems = []
        
        for ig in self.fhir_igs:
            file_type = ig.get('file_type', '')
            if file_type == 'StructureDefinition':
                self.structure_definitions.append(ig)
            elif file_type == 'ValueSet':
                self.value_sets.append(ig)
            elif file_type == 'CodeSystem':
                self.code_systems.append(ig)
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  StructureDefinitions: {len(self.structure_definitions)}")
        print(f"  ValueSets: {len(self.value_sets)}")
        print(f"  CodeSystems: {len(self.code_systems)}")
    
    def extract_element_type(self, element: Dict[str, Any]) -> str:
        """Extract type from element, handling complex type structures"""
        type_list = element.get('type', [])
        if not type_list:
            return ''
        
        # Get first type code
        if isinstance(type_list, list) and len(type_list) > 0:
            first_type = type_list[0]
            if isinstance(first_type, dict):
                return first_type.get('code', '')
            return str(first_type)
        return ''
    
    def extract_binding(self, element: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract binding information from element"""
        binding = element.get('binding')
        if not binding:
            return None
        
        return {
            "strength": binding.get('strength', ''),
            "description": binding.get('description', ''),
            "value_set": binding.get('valueSet', '')
        }
    
    def transform_element_to_attribute(self, element: Dict[str, Any], resource_name: str) -> Dict[str, Any]:
        """Transform FHIR element to common rationalized attribute format"""
        
        element_id = element.get('id', '')
        path = element.get('path', '')
        short = element.get('short', '')
        definition = element.get('definition', '')
        min_card = element.get('min', 0)
        max_card = element.get('max', '*')
        elem_type = self.extract_element_type(element)
        
        # Build attribute in common format
        attr = {
            "attribute_name": path,
            "description": definition,
            "data_type": elem_type,
            "source_attribute": path,
            "source_files": [],  # Will be populated at entity level
            "required": min_card >= 1,
            "nullable": min_card == 0,
            "cardinality": {
                "min": min_card,
                "max": max_card
            },
            "length": None,
            "precision": None,
            "scale": None,
            "default_value": None,
            "is_array": max_card != "1" and max_card != 1,
            "is_nested": elem_type in ['BackboneElement', 'Element'],
            "is_pii": False,
            "is_phi": False,
            "data_classification": None,
            "business_context": None,
            "business_rules": None,
            "validation_rules": None,
            "is_calculated": False,
            "calculation_dependency": None,
            "source_metadata": {
                "id": element_id,
                "short": short,
                "is_summary": element.get('isSummary', False)
            }
        }
        
        # Add optional FHIR-specific fields to source_metadata
        if element.get('isModifier'):
            attr["source_metadata"]["is_modifier"] = True
            if element.get('isModifierReason'):
                attr["source_metadata"]["is_modifier_reason"] = element.get('isModifierReason')
        
        if element.get('mustSupport'):
            attr["source_metadata"]["must_support"] = True
        
        if element.get('meaningWhenMissing'):
            attr["source_metadata"]["meaning_when_missing"] = element.get('meaningWhenMissing')
        
        if element.get('requirements'):
            attr["source_metadata"]["requirements"] = element.get('requirements')
        
        # Add binding if present
        binding = self.extract_binding(element)
        if binding:
            attr["source_metadata"]["binding"] = binding
        
        return attr
    
    def build_prune_prompt(self, entity_name: str, ig_source: str, reasoning: str,
                           elements: List[Dict[str, Any]]) -> str:
        """Build prompt for AI to identify elements to keep"""
        
        # Simplify elements for AI - just id, path, short, definition
        simplified = []
        for elem in elements:
            simplified.append({
                "id": elem.get('id', ''),
                "path": elem.get('path', ''),
                "short": elem.get('short', ''),
                "definition": elem.get('definition', ''),
                "type": self.extract_element_type(elem),
                "min": elem.get('min', 0),
                "max": elem.get('max', '*')
            })
        
        prompt = f"""You are a data architect analyzing FHIR StructureDefinition elements for relevance to the CDM domain specified below.

## CDM CONTEXT

**Domain:** {self.cdm_domain}
**Classification:** {self.cdm_classification}
**Description:** {self.cdm_description}

## ENTITY TO ANALYZE

**Entity:** {entity_name}
**Source:** {ig_source}
**Business Purpose:** {reasoning}

## ELEMENT FORMAT KEY

The elements below contain:
- "id": Unique element identifier (use this in your response)
- "path": Element path in the resource
- "short": Short description
- "definition": Full definition
- "type": Data type
- "min"/"max": Cardinality

## ELEMENTS ({len(elements)} total)

```json
{json.dumps(simplified, indent=2)}
```

## YOUR TASK ##
Review each element and determine if it should be retained for downstream processing to create the CDM for the {self.cdm_domain} domain. The purpose of this task is to REDUCE ELEMENTS THAT ARE CLEARLY NOT RELATED TO THIS DOMAIN to avoid unnecessary processing downstream.

## THE APPROACH ##
Evaluate EACH AND EVERY DATA ELEMENT in the FHIR resource and determine if the element provides HIGH VALUE to creating the CDM for the specified domain.

## DIRECTIONS ## 
KEEP an element using the following criteria:
- ALWAYS consider the Definition, in addition to the Path and Type
- ALWAYS consider the domain description when determing relevance 
- The data element provides HIGH VALUE to creating the CDM
- If you are unsure, but have good confidence it belongs

## OUTPUT FORMAT

Return ONLY valid JSON with the list of element IDs to keep:

```json
{{
  "entity_name": "{entity_name}",
  "elements_reviewed": {len(elements)},
  "keep": ["InsurancePlan.id", "InsurancePlan.identifier", ...]
}}
```

CRITICAL: 
- Return ONLY valid JSON (no markdown, no code blocks, no commentary)
- Use exact element IDs from the "id" values provided
- Focus on retaining ONLY data elements that would be utilized by the CDM
"""
        return prompt
    
    def prune_elements_with_ai(self, entity_name: str, ig_source: str, reasoning: str,
                               elements: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, int]:
        """Use AI to prune elements, return filtered elements"""
        
        prompt = self.build_prune_prompt(entity_name, ig_source, reasoning, elements)
        
        # Dry run - save prompt
        if self.dry_run:
            if self.prompts_dir:
                prompt_file = self.prompts_dir / f"prune_{entity_name}_{datetime.now().strftime('%H%M%S')}.txt"
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(prompt)
                print(f"    Prompt saved: {prompt_file.name}")
            return elements, len(elements), 0
        
        # No LLM - return all
        if not self.llm:
            print(f"    Warning: No LLM client, skipping prune for {entity_name}")
            return elements, len(elements), 0
        
        print(f"    Pruning {entity_name} ({len(elements)} elements)...")
        
        messages = [
            {
                "role": "system",
                "content": "You are a data architect. Return ONLY valid JSON with no markdown, no code blocks, no commentary."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        try:
            response, token_usage = self.llm.chat(messages)
            
            # Parse response
            response_clean = response.strip()
            if response_clean.startswith("```"):
                lines = response_clean.split("\n")
                response_clean = "\n".join(lines[1:-1]) if len(lines) > 2 else response_clean
            
            result = json.loads(response_clean)
            
            # Get list of element IDs to keep
            keep_ids = set(result.get('keep', []))
            
            # Debug output
            print(f"    AI returned {len(keep_ids)} IDs to keep")
            
            # Get all element IDs from raw data
            raw_ids = {e.get('id') for e in elements if e.get('id')}
            
            # Check for mismatches
            missing_in_raw = keep_ids - raw_ids
            if missing_in_raw:
                print(f"    WARNING: {len(missing_in_raw)} IDs from AI not found in elements: {list(missing_in_raw)[:5]}...")
            
            # Filter elements by ID
            original_count = len(elements)
            kept_elements = [e for e in elements if e.get('id') in keep_ids]
            kept_count = len(kept_elements)
            removed_count = original_count - kept_count
            
            if kept_count != len(keep_ids) - len(missing_in_raw):
                print(f"    WARNING: AI returned {len(keep_ids)} IDs, but only {kept_count} matched elements")
            
            print(f"    ✓ Kept {kept_count}/{original_count} elements")
            
            return kept_elements, original_count, removed_count
            
        except json.JSONDecodeError as e:
            print(f"    ERROR: Failed to parse AI response: {e}")
            print(f"    Response preview: {response[:200] if response else 'empty'}...")
            return elements, len(elements), 0
        except Exception as e:
            print(f"    ERROR: AI pruning failed: {e}")
            return elements, len(elements), 0
    
    def rationalize_structure_definition(self, sd_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Rationalize a single StructureDefinition: prune elements, then transform"""
        
        file_path = sd_config.get('file', '')
        resource_name = sd_config.get('resource_name', '')
        ig_source = sd_config.get('ig_source', '')
        reasoning = sd_config.get('reasoning', '')
        priority = sd_config.get('priority', 1)
        
        # Use file path directly from config (already includes full relative path)
        full_path = Path(file_path)
        
        # Also try with filename only if full path doesn't exist
        if not full_path.exists():
            filename = sd_config.get('filename', '')
            if filename:
                # Try common locations
                for base in [Path('.'), Path('input/strd_fhir_ig')]:
                    candidate = base / filename
                    if candidate.exists():
                        full_path = candidate
                        break
        
        if not full_path.exists():
            print(f"    Warning: File not found: {file_path}")
            return None
        
        # Load StructureDefinition
        with open(full_path, 'r', encoding='utf-8') as f:
            sd_data = json.load(f)
        
        # Extract snapshot elements
        snapshot = sd_data.get('snapshot', {})
        elements = snapshot.get('element', [])
        
        if not elements:
            print(f"    Warning: No snapshot elements in {resource_name}")
            return None
        
        original_count = len(elements)
        
        # Prune elements with AI
        kept_elements, original_count, removed_count = self.prune_elements_with_ai(
            resource_name, ig_source, reasoning, elements
        )
        
        # Transform kept elements to attributes
        attributes = []
        for elem in kept_elements:
            attr = self.transform_element_to_attribute(elem, resource_name)
            # Populate source_files at attribute level
            attr["source_files"] = [file_path]
            attributes.append(attr)
        
        # Build entity in common format
        entity = {
            "entity_name": resource_name,
            "description": sd_data.get('description', ''),
            "source_type": "FHIR",
            "source_info": {
                "files": [file_path],
                "api": None,
                "schema": None,
                "table": None,
                "url": sd_data.get('url', ''),
                "version": sd_data.get('version', '')
            },
            "business_context": reasoning,
            "technical_context": None,
            "ai_metadata": {
                "selection_reasoning": None,
                "pruning_notes": f"Pruned {removed_count} of {original_count} elements ({len(attributes)} kept)"
            },
            "attributes": attributes,
            # Keep additional FHIR-specific info in source_metadata at entity level
            "source_metadata": {
                "file_type": "StructureDefinition",
                "resource_type": sd_data.get('resourceType', ''),
                "priority": priority,
                "ig_source": ig_source,
                "original_count": original_count,
                "kept_count": len(attributes),
                "removed_count": removed_count
            }
        }
        
        return entity
    
    def rationalize_value_set(self, vs_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Rationalize a ValueSet - no pruning, just transform"""
        
        file_path = vs_config.get('file', '')
        resource_name = vs_config.get('resource_name', '')
        ig_source = vs_config.get('ig_source', '')
        reasoning = vs_config.get('reasoning', '')
        
        # Use file path directly from config
        full_path = Path(file_path)
        
        if not full_path.exists():
            filename = vs_config.get('filename', '')
            if filename:
                for base in [Path('.'), Path('input/strd_fhir_ig')]:
                    candidate = base / filename
                    if candidate.exists():
                        full_path = candidate
                        break
        
        if not full_path.exists():
            print(f"    Warning: File not found: {file_path}")
            return None
        
        # Load ValueSet
        with open(full_path, 'r', encoding='utf-8') as f:
            vs_data = json.load(f)
        
        # Extract compose information
        compose = vs_data.get('compose', {})
        include = compose.get('include', [])
        
        # Build ValueSet in reference_data format (not entity format)
        valueset = {
            "name": resource_name,
            "url": vs_data.get('url', ''),
            "version": vs_data.get('version', ''),
            "title": vs_data.get('title', ''),
            "status": vs_data.get('status', ''),
            "description": vs_data.get('description', ''),
            "source_file": file_path,
            "ig_source": ig_source,
            "business_context": reasoning,
            "compose": {
                "include": include
            }
        }
        
        return valueset
    
    def rationalize_code_system(self, cs_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Rationalize a CodeSystem - no pruning, just transform"""
        
        file_path = cs_config.get('file', '')
        resource_name = cs_config.get('resource_name', '')
        ig_source = cs_config.get('ig_source', '')
        reasoning = cs_config.get('reasoning', '')
        
        # Use file path directly from config
        full_path = Path(file_path)
        
        if not full_path.exists():
            filename = cs_config.get('filename', '')
            if filename:
                for base in [Path('.'), Path('input/strd_fhir_ig')]:
                    candidate = base / filename
                    if candidate.exists():
                        full_path = candidate
                        break
        
        if not full_path.exists():
            print(f"    Warning: File not found: {file_path}")
            return None
        
        # Load CodeSystem
        with open(full_path, 'r', encoding='utf-8') as f:
            cs_data = json.load(f)
        
        # Extract concepts
        concepts = cs_data.get('concept', [])
        
        # Simplify concepts for output
        simple_concepts = []
        for concept in concepts:
            simple_concepts.append({
                "code": concept.get('code', ''),
                "display": concept.get('display', ''),
                "definition": concept.get('definition', '')
            })
        
        # Build CodeSystem in reference_data format (not entity format)
        codesystem = {
            "name": resource_name,
            "url": cs_data.get('url', ''),
            "version": cs_data.get('version', ''),
            "title": cs_data.get('title', ''),
            "status": cs_data.get('status', ''),
            "description": cs_data.get('description', ''),
            "content": cs_data.get('content', ''),
            "source_file": file_path,
            "ig_source": ig_source,
            "business_context": reasoning,
            "concepts": simple_concepts
        }
        
        return codesystem
    
    def run(self, output_dir: str) -> Optional[str]:
        """Run rationalization, return output file path"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Set up prompts directory for dry run
        if self.dry_run:
            self.prompts_dir = output_path / "prompts"
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
            print(f"  Dry run mode - prompts will be saved to: {self.prompts_dir}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = self.cdm_domain.replace(' ', '_')
        
        structure_entities = []
        valueset_entities = []
        codesystem_entities = []
        
        # Process StructureDefinitions
        if self.structure_definitions:
            print(f"  Processing {len(self.structure_definitions)} StructureDefinitions...")
            for sd_config in self.structure_definitions:
                print(f"    Processing: {sd_config.get('resource_name', 'unknown')}")
                entity = self.rationalize_structure_definition(sd_config)
                if entity:
                    structure_entities.append(entity)
        
        # Process ValueSets (no pruning)
        if self.value_sets:
            print(f"  Processing {len(self.value_sets)} ValueSets...")
            for vs_config in self.value_sets:
                print(f"    Processing: {vs_config.get('resource_name', 'unknown')}")
                entity = self.rationalize_value_set(vs_config)
                if entity:
                    valueset_entities.append(entity)
        
        # Process CodeSystems (no pruning)
        if self.code_systems:
            print(f"  Processing {len(self.code_systems)} CodeSystems...")
            for cs_config in self.code_systems:
                print(f"    Processing: {cs_config.get('resource_name', 'unknown')}")
                entity = self.rationalize_code_system(cs_config)
                if entity:
                    codesystem_entities.append(entity)
        
        if not structure_entities and not valueset_entities and not codesystem_entities:
            print("  No FHIR entities generated")
            return None
        
        # Build consolidated output in common format
        consolidated = {
            "rationalization_metadata": {
                "source_type": "FHIR",
                "cdm_domain": self.cdm_domain,
                "cdm_classification": self.cdm_classification,
                "rationalization_timestamp": datetime.now().isoformat(),
                "entities_processed": len(structure_entities),
                "value_sets_processed": len(valueset_entities),
                "code_systems_processed": len(codesystem_entities)
            },
            "entities": structure_entities,
            "reference_data": {
                "value_sets": valueset_entities,
                "code_systems": codesystem_entities
            }
        }
        
        output_file = output_path / f"rationalized_fhir_{domain_safe}_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated, f, indent=2)
        
        # Summary
        sd_attrs = sum(len(e.get('attributes', [])) for e in structure_entities)
        vs_count = sum(len(e.get('compose', {}).get('include', [])) for e in valueset_entities)
        cs_concepts = sum(len(e.get('concepts', [])) for e in codesystem_entities)
        
        print(f"  ✓ Saved: {output_file.name}")
        print(f"    Entities: {len(structure_entities)}, Attributes: {sd_attrs}")
        print(f"    ValueSets: {len(valueset_entities)}")
        print(f"    CodeSystems: {len(codesystem_entities)}, Concepts: {cs_concepts}")
        
        return str(output_file)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("Usage: python rationalize_fhir.py <config_file> <output_dir>")
        sys.exit(1)
    
    rationalizer = FHIRRationalizer(sys.argv[1])
    rationalizer.run(sys.argv[2])


# =============================================================================
# ORCHESTRATOR WRAPPER
# =============================================================================

def run_fhir_rationalization(config, outdir, llm=None, dry_run=False, config_path=None):
    """
    Wrapper function for orchestrator compatibility.
    
    Args:
        config: AppConfig instance (unused, for interface consistency)
        outdir: Output directory path
        llm: LLM client instance
        dry_run: If True, save prompts only
        config_path: Path to config JSON file (required)
    
    Returns:
        Path to output file, or None if dry run
    """
    if not config_path:
        raise ValueError("config_path is required for FHIR rationalization")
    
    rationalizer = FHIRRationalizer(config_path, llm=llm, dry_run=dry_run)
    return rationalizer.run(str(outdir))