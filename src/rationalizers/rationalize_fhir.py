"""
FHIR Rationalization Module
Transforms FHIR StructureDefinitions into unified rationalized format.

Work Item 1: Skip VS/CS processing (handled in post-process via binding URLs)
Work Item 2: Two-pass P1/P2 processing - P1 creates entities+attributes, P2 merges attributes into P1 entities
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
        
        # Work Item 1: Only process StructureDefinitions
        # VS/CS are kept in config for post-process terminology enrichment
        self.structure_definitions = []
        self.value_set_count = 0
        self.code_system_count = 0
        
        for ig in self.fhir_igs:
            file_type = ig.get('file_type', '')
            if file_type == 'StructureDefinition':
                self.structure_definitions.append(ig)
            elif file_type == 'ValueSet':
                self.value_set_count += 1
            elif file_type == 'CodeSystem':
                self.code_system_count += 1
        
        # Work Item 2: Separate by priority (default to 1 for backward compatibility)
        self.p1_structures = [sd for sd in self.structure_definitions if sd.get('priority', 1) == 1]
        self.p2_structures = [sd for sd in self.structure_definitions if sd.get('priority') == 2]
        
        print(f"  Config loaded: {config_path}")
        print(f"  Domain: {self.cdm_domain}")
        print(f"  StructureDefinitions: {len(self.structure_definitions)} (P1: {len(self.p1_structures)}, P2: {len(self.p2_structures)})")
        print(f"  ValueSets: {self.value_set_count} (skipped - used in post-process)")
        print(f"  CodeSystems: {self.code_system_count} (skipped - used in post-process)")
    
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
        
        # Add binding if present (Work Item 3 depends on this)
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
    
    def build_p2_prune_prompt(self, entity_name: str, ig_source: str, reasoning: str,
                              elements: List[Dict[str, Any]], 
                              p1_entities: List[Dict[str, Any]]) -> str:
        """
        Work Item 2: Build P2 prompt with P1 entity context.
        
        P2 files add attributes to EXISTING P1 entities only - no new entities.
        Full P1 entity+attribute metadata provided for context.
        """
        
        # Simplify elements for AI
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
        
        # Build P1 entity context with full attribute metadata
        p1_context = []
        for entity in p1_entities:
            entity_summary = {
                "entity_name": entity.get("entity_name"),
                "description": entity.get("description", "")[:200],  # Truncate long descriptions
                "attributes": []
            }
            for attr in entity.get("attributes", []):
                entity_summary["attributes"].append({
                    "attribute_name": attr.get("attribute_name"),
                    "data_type": attr.get("data_type"),
                    "description": attr.get("description", "")[:150],
                    "required": attr.get("required", False)
                })
            p1_context.append(entity_summary)
        
        p1_entity_names = [e.get("entity_name") for e in p1_entities]
        
        prompt = f"""You are a data architect analyzing FHIR StructureDefinition elements for relevance to the CDM domain specified below.

## CDM CONTEXT

**Domain:** {self.cdm_domain}
**Classification:** {self.cdm_classification}
**Description:** {self.cdm_description}

## PRIORITY 2 PROCESSING - ATTRIBUTE REFINEMENT ONLY

This is a **Priority 2 (P2)** StructureDefinition. P2 files provide REFINEMENT to existing entities.

**CRITICAL CONSTRAINTS:**
1. P2 files can ONLY add attributes to entities already defined by Priority 1 (P1) files
2. Do NOT keep elements that would create NEW entities
3. ONLY keep elements that ADD VALUE to the existing P1 entities listed below
4. If this P2 StructureDefinition does not meaningfully refine any P1 entity, return empty keep list

## EXISTING P1 ENTITIES (with their current attributes)

The following entities were defined by P1 StructureDefinitions. You may ONLY add attributes to these entities:

**P1 Entity Names:** {json.dumps(p1_entity_names)}

**Full P1 Entity Context:**
```json
{json.dumps(p1_context, indent=2)}
```

## P2 STRUCTUREDEFINITION TO ANALYZE

**StructureDefinition:** {entity_name}
**Source:** {ig_source}
**Business Purpose:** {reasoning}

## ELEMENTS ({len(elements)} total)

```json
{json.dumps(simplified, indent=2)}
```

## YOUR TASK

Review each element and determine if it should be retained AS AN ATTRIBUTE ON AN EXISTING P1 ENTITY.

An element should be kept ONLY if:
1. It provides a valuable attribute for one of the P1 entities listed above
2. The attribute is NOT already covered by existing P1 attributes (avoid duplicates)
3. It adds refinement/detail that the P1 entity lacks
4. It semantically belongs on that P1 entity

## OUTPUT FORMAT

Return ONLY valid JSON:

```json
{{
  "p2_structure_name": "{entity_name}",
  "elements_reviewed": {len(elements)},
  "target_p1_entity": "<EXACT name from P1 Entity Names list, or 'NONE' if no match>",
  "mapping_rationale": "<brief explanation of why this P2 maps to the target P1 entity>",
  "keep": ["element.id1", "element.id2", ...]
}}
```

CRITICAL: 
- Return ONLY valid JSON (no markdown, no code blocks, no commentary)
- target_p1_entity MUST be an exact name from the P1 Entity Names list, or "NONE"
- If target_p1_entity is "NONE", keep list MUST be empty
- Use exact element IDs from the "id" values provided
- Only keep elements that add value to the specified target_p1_entity
"""
        return prompt
    
    def prune_elements_with_ai(self, entity_name: str, ig_source: str, reasoning: str,
                               elements: List[Dict[str, Any]], 
                               p1_entities: Optional[List[Dict[str, Any]]] = None) -> Tuple[List[Dict[str, Any]], int, int, Optional[str]]:
        """
        Use AI to prune elements, return filtered elements.
        
        Args:
            entity_name: Name of the entity being processed
            ig_source: Source IG identifier
            reasoning: Business reasoning for inclusion
            elements: List of FHIR elements to evaluate
            p1_entities: For P2 processing, the list of P1 entities for context
            
        Returns:
            Tuple of (kept_elements, original_count, removed_count, target_p1_entity)
            target_p1_entity is None for P1, or the target entity name for P2
        """
        
        # Build appropriate prompt based on whether this is P1 or P2
        if p1_entities is not None:
            prompt = self.build_p2_prune_prompt(entity_name, ig_source, reasoning, elements, p1_entities)
        else:
            prompt = self.build_prune_prompt(entity_name, ig_source, reasoning, elements)
        
        # Dry run - save prompt
        if self.dry_run:
            if self.prompts_dir:
                priority_label = "p2" if p1_entities else "p1"
                prompt_file = self.prompts_dir / f"prune_{priority_label}_{entity_name}_{datetime.now().strftime('%H%M%S')}.txt"
                with open(prompt_file, 'w', encoding='utf-8') as f:
                    f.write(prompt)
                print(f"    Prompt saved: {prompt_file.name}")
            return elements, len(elements), 0, None
        
        # No LLM - return all
        if not self.llm:
            print(f"    Warning: No LLM client, skipping prune for {entity_name}")
            return elements, len(elements), 0, None
        
        priority_label = "P2" if p1_entities else "P1"
        print(f"    Pruning {entity_name} [{priority_label}] ({len(elements)} elements)...")
        
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
            
            # For P2, extract target entity
            target_p1_entity = None
            if p1_entities is not None:
                target_p1_entity = result.get('target_p1_entity')
                if target_p1_entity == "NONE" or not target_p1_entity:
                    print(f"    ✓ P2 has no matching P1 entity - skipping")
                    return [], len(elements), len(elements), None
                print(f"    Target P1 entity: {target_p1_entity}")
            
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
            
            return kept_elements, original_count, removed_count, target_p1_entity
            
        except json.JSONDecodeError as e:
            print(f"    ERROR: Failed to parse AI response: {e}")
            print(f"    Response preview: {response[:200] if response else 'empty'}...")
            return elements, len(elements), 0, None
        except Exception as e:
            print(f"    ERROR: AI pruning failed: {e}")
            return elements, len(elements), 0, None
    
    def rationalize_structure_definition_p1(self, sd_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Rationalize a P1 StructureDefinition: prune elements, then create entity.
        
        Args:
            sd_config: Config dict for this StructureDefinition
            
        Returns:
            Rationalized entity dict, or None if failed
        """
        
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
        
        # Prune elements with AI (P1 - no p1_entities context)
        kept_elements, original_count, removed_count, _ = self.prune_elements_with_ai(
            resource_name, ig_source, reasoning, elements, p1_entities=None
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
    
    def process_p2_structure_definition(self, sd_config: Dict[str, Any], 
                                        p1_entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process a P2 StructureDefinition: prune elements and merge into P1 entity.
        
        Args:
            sd_config: Config dict for this StructureDefinition
            p1_entities: List of P1 entities to potentially merge into
            
        Returns:
            Dict with merge results: {target_entity, attributes_added, skipped_reason}
        """
        
        file_path = sd_config.get('file', '')
        resource_name = sd_config.get('resource_name', '')
        ig_source = sd_config.get('ig_source', '')
        reasoning = sd_config.get('reasoning', '')
        
        result = {
            "p2_structure": resource_name,
            "target_entity": None,
            "attributes_added": 0,
            "skipped_reason": None
        }
        
        # Use file path directly from config
        full_path = Path(file_path)
        
        if not full_path.exists():
            filename = sd_config.get('filename', '')
            if filename:
                for base in [Path('.'), Path('input/strd_fhir_ig')]:
                    candidate = base / filename
                    if candidate.exists():
                        full_path = candidate
                        break
        
        if not full_path.exists():
            print(f"    Warning: File not found: {file_path}")
            result["skipped_reason"] = "file_not_found"
            return result
        
        # Load StructureDefinition
        with open(full_path, 'r', encoding='utf-8') as f:
            sd_data = json.load(f)
        
        # Extract snapshot elements
        snapshot = sd_data.get('snapshot', {})
        elements = snapshot.get('element', [])
        
        if not elements:
            print(f"    Warning: No snapshot elements in {resource_name}")
            result["skipped_reason"] = "no_elements"
            return result
        
        # Prune elements with AI (P2 - with p1_entities context)
        kept_elements, original_count, removed_count, target_p1_entity = self.prune_elements_with_ai(
            resource_name, ig_source, reasoning, elements, p1_entities=p1_entities
        )
        
        # If no target entity or no elements, skip
        if not target_p1_entity or not kept_elements:
            result["skipped_reason"] = "no_matching_p1_entity" if not target_p1_entity else "no_relevant_elements"
            return result
        
        # Find target P1 entity
        target_entity = None
        for entity in p1_entities:
            if entity.get("entity_name") == target_p1_entity:
                target_entity = entity
                break
        
        if not target_entity:
            print(f"    Warning: Target P1 entity '{target_p1_entity}' not found")
            result["skipped_reason"] = f"target_not_found:{target_p1_entity}"
            return result
        
        result["target_entity"] = target_p1_entity
        
        # Get existing attribute names to avoid duplicates
        existing_attr_names = {
            attr.get("attribute_name", "").lower() 
            for attr in target_entity.get("attributes", [])
        }
        
        # Transform and merge attributes
        added_count = 0
        for elem in kept_elements:
            attr = self.transform_element_to_attribute(elem, resource_name)
            attr["source_files"] = [file_path]
            
            # Mark as P2-sourced
            attr["source_metadata"]["p2_source"] = resource_name
            attr["source_metadata"]["p2_ig"] = ig_source
            
            # Check for duplicate
            attr_name_lower = attr.get("attribute_name", "").lower()
            if attr_name_lower not in existing_attr_names:
                target_entity["attributes"].append(attr)
                existing_attr_names.add(attr_name_lower)
                added_count += 1
        
        result["attributes_added"] = added_count
        
        # Update entity metadata
        if added_count > 0:
            p2_notes = target_entity.get("ai_metadata", {}).get("p2_refinements", [])
            p2_notes.append({
                "p2_structure": resource_name,
                "ig_source": ig_source,
                "attributes_added": added_count
            })
            if "ai_metadata" not in target_entity:
                target_entity["ai_metadata"] = {}
            target_entity["ai_metadata"]["p2_refinements"] = p2_notes
            
            # Update source_info files
            if file_path not in target_entity.get("source_info", {}).get("files", []):
                target_entity["source_info"]["files"].append(file_path)
        
        print(f"    ✓ Merged {added_count} attributes into {target_p1_entity}")
        
        return result
    
    def run(self, output_dir: str) -> Optional[str]:
        """
        Run rationalization, return output file path.
        
        Work Item 1: Skip VS/CS processing entirely
        Work Item 2: Two-pass P1/P2 processing with P2 merge
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Set up prompts directory for dry run
        if self.dry_run:
            self.prompts_dir = output_path / "prompts"
            self.prompts_dir.mkdir(parents=True, exist_ok=True)
            print(f"  Dry run mode - prompts will be saved to: {self.prompts_dir}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        domain_safe = self.cdm_domain.replace(' ', '_')
        
        # Work Item 2: Two-pass processing
        p1_entities = []
        p2_merge_results = []
        
        # PASS 1: Process P1 StructureDefinitions (define entities + attributes)
        if self.p1_structures:
            print(f"  === PASS 1: Processing {len(self.p1_structures)} Priority 1 StructureDefinitions ===")
            for sd_config in self.p1_structures:
                print(f"    Processing [P1]: {sd_config.get('resource_name', 'unknown')}")
                entity = self.rationalize_structure_definition_p1(sd_config)
                if entity:
                    p1_entities.append(entity)
            print(f"  ✓ Pass 1 complete: {len(p1_entities)} entities created")
        
        # PASS 2: Process P2 StructureDefinitions (merge attributes into P1 entities)
        if self.p2_structures and p1_entities:
            print(f"\n  === PASS 2: Processing {len(self.p2_structures)} Priority 2 StructureDefinitions ===")
            print(f"  P1 entity targets: {[e.get('entity_name') for e in p1_entities]}")
            
            for sd_config in self.p2_structures:
                print(f"    Processing [P2]: {sd_config.get('resource_name', 'unknown')}")
                merge_result = self.process_p2_structure_definition(sd_config, p1_entities)
                p2_merge_results.append(merge_result)
            
            # Summarize P2 results
            merged_count = sum(1 for r in p2_merge_results if r.get("attributes_added", 0) > 0)
            skipped_count = sum(1 for r in p2_merge_results if r.get("skipped_reason"))
            total_attrs_added = sum(r.get("attributes_added", 0) for r in p2_merge_results)
            
            print(f"  ✓ Pass 2 complete: {merged_count} P2 structures merged, {skipped_count} skipped")
            print(f"    Total P2 attributes added: {total_attrs_added}")
        elif self.p2_structures and not p1_entities:
            print(f"\n  ⚠️ Skipping {len(self.p2_structures)} P2 structures - no P1 entities to refine")
        
        if not p1_entities:
            print("  No FHIR entities generated")
            return None
        
        # Work Item 1: No VS/CS processing - output simplified structure
        consolidated = {
            "rationalization_metadata": {
                "source_type": "FHIR",
                "cdm_domain": self.cdm_domain,
                "cdm_classification": self.cdm_classification,
                "rationalization_timestamp": datetime.now().isoformat(),
                "entities_processed": len(p1_entities),
                "p1_structures_processed": len(self.p1_structures),
                "p2_structures_processed": len(self.p2_structures),
                "p2_merge_results": p2_merge_results,
                "value_sets_skipped": self.value_set_count,
                "code_systems_skipped": self.code_system_count,
                "note": "VS/CS skipped - terminology enrichment handled in post-process via binding URLs"
            },
            "entities": p1_entities  # Only P1 entities (with P2 attributes merged in)
            # No reference_data section - VS/CS loaded on-demand in post-process
        }
        
        output_file = output_path / f"rationalized_fhir_{domain_safe}_{timestamp}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(consolidated, f, indent=2)
        
        # Summary
        total_attrs = sum(len(e.get('attributes', [])) for e in p1_entities)
        
        print(f"  ✓ Saved: {output_file.name}")
        print(f"    Entities: {len(p1_entities)} (P1 only, with P2 attributes merged)")
        print(f"    Attributes: {total_attrs}")
        print(f"    VS/CS: Skipped (post-process enrichment)")
        
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