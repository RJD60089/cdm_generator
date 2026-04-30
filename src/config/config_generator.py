"""
CDM Configuration Generator - Coordinator Module

Orchestrates config generation by coordinating:
- FHIR resource analysis (config_gen_fhir)
- NCPDP standards analysis (config_gen_ncpdp)
- Glue file consolidation (config_gen_glue)
- Auto-discovery of guardrail and DDL files

Flow:
1. Must have base config - stop if none exists
2. Always use latest timestamped config as source
3. Prompt: Run Config gen? (Y/n)
4. If Y: Prompt for each analysis (FHIR, NCPDP, Glue)
5. Auto-discover guardrails and DDL files from directories
6. Only update changed sections, preserve rest from source

Usage:
    python -m src.config.config_generator plan
    python -m src.config.config_generator formulary
"""
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from . import config_utils
from .config_gen_core import ConfigGeneratorBase, prompt_user_choice
from .config_gen_mapping import run_mapping_config
from .config_gen_fhir import FHIRConfigGenerator
from .config_gen_ncpdp import NCPDPConfigGenerator
from .config_gen_glue import GlueConfigGenerator
from .config_gen_edw import EDWConfigGenerator
from .config_gen_ancillary import AncillaryConfigGenerator
from .config_gen_guardrails import GuardrailsConfigGenerator


class ConfigGenerator(ConfigGeneratorBase):
    """Coordinate CDM configuration generation."""
    
    def __init__(self, cdm_name: str, llm_client=None):
        """Initialize config generator.
        
        Args:
            cdm_name: CDM name (e.g., 'plan', 'formulary')
            llm_client: LLM client for AI analysis
        """
        super().__init__(cdm_name, llm_client)
        
        # Sub-generators
        self.fhir_gen = FHIRConfigGenerator(cdm_name, llm_client)
        self.ncpdp_gen = NCPDPConfigGenerator(cdm_name, llm_client)
        self.glue_gen = GlueConfigGenerator(cdm_name, llm_client)
        self.edw_gen  = EDWConfigGenerator(cdm_name, llm_client)
        self.ancillary_gen = AncillaryConfigGenerator(cdm_name, llm_client)
        self.guardrails_gen = GuardrailsConfigGenerator(cdm_name, llm_client)
    
    def run(self, dry_run: bool = False) -> Optional[Path]:
        """Execute config generation workflow.
        
        Args:
            dry_run: If True, save prompts but don't call LLM
            
        Returns:
            Path to saved config, or None if skipped/failed
        """
        print(f"\n{'='*60}")
        print(f"CDM Configuration Generator")
        print(f"{'='*60}")
        print(f"   CDM: {self.cdm_name}")
        print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")
        
        # Step 1: Load source config (REQUIRED)
        source_config = self._load_source_config()
        if source_config is None:
            return None
        
        print(f"   Domain: {source_config['cdm']['domain']}")
        print(f"   Type: {source_config['cdm']['type']}")
        
        # Step 2: Auto-discover guardrails, DDL, and ancillary files.
        # All three are pure filesystem scans — no AI calls — so they
        # always run, even when the user opts into the "skip AI" path.
        guardrails_files = config_utils.list_guardrail_files(self.cdm_name)
        ddl_files = config_utils.list_ddl_files(self.cdm_name)
        new_ancillaries = self._auto_discover_new_ancillaries(source_config)

        print(f"\n   Auto-discovered files:")
        print(f"      Guardrails: {len(guardrails_files)}")
        print(f"      DDL: {len(ddl_files)}")
        existing_anc = (source_config.get("input_files") or {}).get("ancillary") or []
        if new_ancillaries:
            print(f"      Ancillary: {len(existing_anc)} existing + {len(new_ancillaries)} new file(s)")
            for a in new_ancillaries:
                print(f"         + {a['file']}  (source_id={a['source_id']}, type={a['file_type']})")
        else:
            print(f"      Ancillary: {len(existing_anc)} existing, 0 new files")
        
        # Step 3: Run analyses based on user selections
        fhir_result = None
        ncpdp_result = None
        glue_result = None
        edw_result = None
        ancillary_result = None
        guardrails_result = None

        # Shortcut: skip every AI selector and only refresh the mapping block
        skip_ai = prompt_user_choice(
            "\n   Skip AI selectors (FHIR/NCPDP/Guardrails/Glue/EDW/Ancillary) and only refresh mapping block?",
            default="N",
        )

        if not skip_ai:
            # FHIR Analysis
            if prompt_user_choice("\n   Run FHIR analysis?", default="Y"):
                fhir_result = self.fhir_gen.run_analysis(source_config, dry_run)
                if not dry_run and fhir_result:
                    fhir_result, corrections = self.fhir_gen.validate_and_correct_files(fhir_result)

            # NCPDP Analysis
            if prompt_user_choice("\n   Run NCPDP analysis?", default="Y"):
                ncpdp_result = self.ncpdp_gen.run_analysis(source_config, dry_run)
                if not dry_run and ncpdp_result:
                    ncpdp_result = self.ncpdp_gen.validate_codes(ncpdp_result)

            # Guardrails Tab Triage — AI decides which sheets to include per file
            if prompt_user_choice("\n   Run Guardrails tab triage (per-file include/exclude)?", default="Y"):
                # First-pass seeding: when source_config has no guardrails
                # listed yet (e.g., a freshly created config), populate the
                # in-memory config with the auto-discovered file list before
                # triage runs.  Otherwise triage sees an empty list, prints
                # "nothing to triage", and the user must re-run config-gen
                # a second time to actually trigger triage.
                if guardrails_files and not (source_config.get('input_files') or {}).get('guardrails'):
                    source_config.setdefault('input_files', {})['guardrails'] = list(guardrails_files)
                    print(f"   ℹ️  Seeded {len(guardrails_files)} auto-discovered guardrail file(s) into config for triage")
                guardrails_result = self.guardrails_gen.run_analysis(source_config, dry_run)

            # Glue Analysis
            if prompt_user_choice("\n   Run Glue analysis?", default="Y"):
                glue_result = self.glue_gen.run_analysis(source_config, dry_run)

            # EDW Analysis
            if prompt_user_choice("\n   Run EDW entity selection?", default="Y"):
                edw_result = self.edw_gen.run_analysis(source_config, dry_run)

            # Ancillary Analysis
            if prompt_user_choice("\n   Configure ancillary definition sources?", default="Y"):
                ancillary_result = self.ancillary_gen.run_analysis(source_config, dry_run)

        # Mapping block — runs on a snapshot that already reflects any
        # ancillary updates we just made (AI or filesystem-only), so DDL
        # ancillary entries added this run are eligible for the mapping
        # prompt.
        mapping_result = None
        preview_config = self._merge_updates(
            source_config, fhir_result, ncpdp_result, glue_result,
            guardrails_files, ddl_files, edw_result, ancillary_result,
            new_ancillaries=new_ancillaries,
            guardrails_result=guardrails_result,
        )
        if prompt_user_choice("\n   Configure mapping block (Collibra)?", default="Y"):
            mapping_result = run_mapping_config(preview_config)

        # Step 4: Build updated config (merge changes into source)
        updated_config = self._merge_updates(
            source_config, fhir_result, ncpdp_result, glue_result,
            guardrails_files, ddl_files, edw_result, ancillary_result,
            mapping_result=mapping_result,
            new_ancillaries=new_ancillaries,
            guardrails_result=guardrails_result,
        )
        
        # Step 5: Save with new timestamp
        filepath = self.save_config(updated_config)
        
        # Report
        self._print_summary(updated_config, filepath)
        
        return filepath
    
    def _load_source_config(self) -> Optional[Dict]:
        """Load source config file (latest timestamped or base).
        
        MUST have a config to proceed.
        
        Returns:
            Config dict or None (with error message)
        """
        # Try latest timestamped first
        latest_path = config_utils.find_latest_config(self.cdm_name)
        
        if latest_path:
            print(f"\n   Source config: {latest_path.name}")
            return config_utils.load_json_file(latest_path)
        
        # Try base config
        base_path = config_utils.find_base_config(self.cdm_name)
        
        if base_path:
            print(f"\n   Source config: {base_path.name}")
            config = config_utils.load_json_file(base_path)
            
            # Validate base config has required fields
            errors = self.validate_base_config(config)
            if errors:
                print(f"\n   ⚠️  Base config validation errors:")
                for e in errors:
                    print(f"      - {e}")
                return None
            
            return config
        
        # No config found - STOP
        print(f"\n   ❌ ERROR: No config found for CDM: {self.cdm_name}")
        print(f"      Expected location: {self.config_dir}/")
        print(f"      Expected file: config_{self.safe_name}.json")
        print(f"\n      Create a base config file before running config generator.")
        return None
    
    def _auto_discover_new_ancillaries(self, source_config: Dict) -> List[Dict]:
        """Filesystem-only ancillary discovery — no AI calls.

        Returns minimal new-entry dicts for ancillary source files that
        exist on disk but are not yet referenced in
        ``input_files.ancillary``. Each new entry carries:
            file, file_type (from extension), processing_mode (prompted),
            source_id (slugified filename without extension).

        For each new file, the user is prompted to pick one of three
        processing modes:
            driver  — contributes to the Foundational CDM (entity-shaping)
            refiner — refines the CDM during the refinement step
            mapper  — used only for source-to-target mapping in Step 5

        Existing ancillary entries are NEVER modified by this method;
        the caller appends the returned list to whatever's already there
        so AI-enriched fields (description, etc.) survive.
        """
        existing = (source_config.get("input_files") or {}).get("ancillary") or []
        existing_files = {(e.get("file") or "").lower() for e in existing}

        # Discover candidates first so we can summarise before prompting
        candidates: List[str] = []
        for filename in config_utils.list_ancillary_files(self.cdm_name):
            if filename.lower() not in existing_files:
                candidates.append(filename)

        if not candidates:
            return []

        print(f"\n   Found {len(candidates)} new ancillary file(s) — pick a processing mode for each:")
        print(f"     d = driver   (contributes to Foundational CDM)")
        print(f"     r = refiner  (refines the CDM during refinement step) [default]")
        print(f"     m = mapper   (used only for Step 5 source-to-target mapping)")

        new_entries: List[Dict] = []
        for filename in candidates:
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
            file_type = {
                "sql":  "ddl",
                "ddl":  "ddl",
                "json": "json",
                "yaml": "yaml",
                "yml":  "yaml",
                "csv":  "csv",
                "xlsx": "spreadsheet",
                "txt":  "text",
            }.get(ext, ext or "unknown")

            stem = filename.rsplit(".", 1)[0]
            slug = re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-")
            source_id = f"ancillary-{slug}" if slug else f"ancillary-{stem.lower()}"

            # Prompt for processing_mode, default refiner (matches prior hardcoded value)
            mode_default = "r"
            while True:
                raw = input(
                    f"     '{filename}' ({file_type}) [d/r/m, default {mode_default}]: "
                ).strip().lower() or mode_default
                if raw in ("d", "driver"):
                    mode = "driver"
                    break
                if raw in ("r", "refiner"):
                    mode = "refiner"
                    break
                if raw in ("m", "mapper"):
                    mode = "mapper"
                    break
                print(f"        ⚠️  Enter d, r, or m")

            new_entries.append({
                "file":            filename,
                "file_type":       file_type,
                "processing_mode": mode,
                "source_id":       source_id,
            })
        return new_entries

    def _merge_updates(
        self,
        source_config: Dict,
        fhir_result: Optional[Dict],
        ncpdp_result: Optional[Dict],
        glue_result: Optional[Dict],
        guardrails_files: List[str],
        ddl_files: List[str],
        edw_result: Optional[Dict] = None,
        ancillary_result: Optional[Dict] = None,
        mapping_result: Optional[Dict] = None,
        new_ancillaries: Optional[List[Dict]] = None,
        guardrails_result: Optional[Dict] = None,
    ) -> Dict:
        """Merge analysis results into source config.
        
        Only updates sections that were analyzed. Preserves all other fields.
        
        Args:
            source_config: Original config (source of truth for unchanged fields)
            fhir_result: FHIR analysis results (or None to preserve)
            ncpdp_result: NCPDP analysis results (or None to preserve)
            glue_result: Glue analysis results (or None to preserve)
            guardrails_files: Auto-discovered guardrail filenames
            ddl_files: Auto-discovered DDL filenames
            
        Returns:
            Updated config dict
        """
        # Deep copy source config
        import copy
        config = copy.deepcopy(source_config)
        
        # Ensure input_files exists
        if 'input_files' not in config:
            config['input_files'] = {}
        
        # ---- Guardrails — sync filenames + preserve any per-file triage state ----
        # The list is allowed to be either plain strings (filename) or
        # objects ({file, include_sheets, exclude_sheets, triage_reasons}).
        # Auto-discovery refreshes the FILENAME set from the filesystem,
        # but we preserve any prior object-form metadata for files that
        # are still present.
        existing_guardrails = (config.get('input_files') or {}).get('guardrails') or []
        existing_by_filename: Dict[str, Dict] = {}
        for entry in existing_guardrails:
            if isinstance(entry, dict) and entry.get('file'):
                existing_by_filename[entry['file']] = dict(entry)
            elif isinstance(entry, str):
                existing_by_filename[entry] = {"file": entry}

        new_guardrails: List = []
        for fname in guardrails_files:
            if fname in existing_by_filename:
                existing = existing_by_filename[fname]
                # If only "file" key is set, drop back to plain-string form
                # (don't pollute the config with empty objects).
                keys = set(existing.keys()) - {"file"}
                if keys:
                    new_guardrails.append(existing)
                else:
                    new_guardrails.append(fname)
            else:
                new_guardrails.append(fname)

        # If guardrails_result is provided (triage just ran), overlay its
        # decisions onto matching entries.
        if guardrails_result and 'guardrails' in guardrails_result:
            triage_by_filename = {
                e.get('file'): e for e in (guardrails_result['guardrails'] or [])
                if isinstance(e, dict) and e.get('file')
            }
            overlaid: List = []
            for entry in new_guardrails:
                fname = entry if isinstance(entry, str) else entry.get('file')
                if fname in triage_by_filename:
                    overlaid.append(triage_by_filename[fname])
                else:
                    overlaid.append(entry)
            new_guardrails = overlaid

        config['input_files']['guardrails'] = new_guardrails
        config['input_files']['ddl'] = ddl_files
        
        # Update FHIR if analyzed
        if fhir_result:
            config['input_files']['fhir_igs'] = self._build_fhir_file_entries(fhir_result)
            # Update metadata
            if 'metadata' not in config:
                config['metadata'] = {}
            if 'ai_analysis' not in config['metadata']:
                config['metadata']['ai_analysis'] = {}
            config['metadata']['ai_analysis']['fhir_assessment'] = fhir_result.get('domain_assessment', {})
        
        # Update NCPDP if analyzed
        if ncpdp_result:
            config['input_files']['ncpdp_general_standards'] = ncpdp_result.get('ncpdp_general_standards', [])
            config['input_files']['ncpdp_script_standards'] = ncpdp_result.get('ncpdp_script_standards', [])
            # Update metadata
            if 'metadata' not in config:
                config['metadata'] = {}
            if 'ai_analysis' not in config['metadata']:
                config['metadata']['ai_analysis'] = {}
            config['metadata']['ai_analysis']['ncpdp_assessment'] = ncpdp_result.get('domain_assessment', {})
        
        # Update Glue if analyzed
        if glue_result and 'glue' in glue_result:
            config['input_files']['glue'] = glue_result['glue']

        # Update EDW if analyzed
        if edw_result and 'edw' in edw_result:
            config['input_files']['edw'] = edw_result['edw']
            if edw_result.get('domain_assessment'):
                if 'metadata' not in config:
                    config['metadata'] = {}
                if 'ai_analysis' not in config['metadata']:
                    config['metadata']['ai_analysis'] = {}
                config['metadata']['ai_analysis']['edw_assessment'] = edw_result['domain_assessment']
        
        # Update Ancillary if analyzed
        if ancillary_result and 'ancillary' in ancillary_result:
            config['input_files']['ancillary'] = ancillary_result['ancillary']

        # Append filesystem-only newly-discovered ancillaries (no AI).
        # Skip anything already present so we don't clobber AI-enriched
        # entries from a previous run.
        if new_ancillaries:
            current = config['input_files'].get('ancillary') or []
            seen_files = {(e.get('file') or '').lower() for e in current}
            for entry in new_ancillaries:
                if (entry.get('file') or '').lower() not in seen_files:
                    current.append(entry)
                    seen_files.add((entry.get('file') or '').lower())
            config['input_files']['ancillary'] = current

        # Mapping block — replace wholesale when the user reconfigured it
        if mapping_result is not None:
            config['mapping'] = mapping_result

        # Ensure output section is populated
        if 'output' not in config:
            config['output'] = {}
        if not config['output'].get('directory'):
            config['output']['directory'] = f"output/{self.safe_name}"
            print(f"   ⚠️  output.directory was missing — setting to: {config['output']['directory']} (will be saved to config)")
        if not config['output'].get('filename'):
            domain = config.get('cdm', {}).get('domain', self.safe_name)
            safe_domain = domain.replace(' ', '_').replace('/', '_')
            config['output']['filename'] = f"{safe_domain}_CDM.xlsx"
            print(f"   ⚠️  output.filename was missing — setting to: {config['output']['filename']} (will be saved to config)")
        
        # Update metadata timestamp
        if 'metadata' not in config:
            config['metadata'] = {}
        config['metadata']['generated_at'] = datetime.now().isoformat()
        config['metadata']['generator_version'] = "3.0"
        
        return config
    
    

    def _build_fhir_file_entries(self, fhir_result: Dict) -> List[Dict]:
        """Build FHIR file entries with full paths."""
        entries = []
        fhir_dir = config_utils.get_standards_fhir_dir()
        
        for resource in fhir_result.get('fhir_igs', []):
            filename = resource['filename']
            filepath = config_utils.find_file_recursive(fhir_dir, filename)
            
            if filepath:
                rel_path = config_utils.normalize_path(filepath, self.project_root)
                entries.append({
                    "file": rel_path,
                    "filename": filename,
                    "resource_name": resource['resource_name'],
                    "file_type": resource['file_type'],
                    "ig_source": resource['ig_source'],
                    "priority": resource.get('priority', 1),
                    "reasoning": resource['reasoning']
                })
            else:
                entries.append({
                    "file": f"NOT_FOUND/{filename}",
                    "filename": filename,
                    "resource_name": resource['resource_name'],
                    "file_type": resource['file_type'],
                    "ig_source": resource['ig_source'],
                    "priority": resource.get('priority', 1),
                    "reasoning": resource['reasoning']
                })
        
        return entries

    def _print_summary(self, config: Dict, filepath: Path):
        """Print generation summary.
        
        Args:
            config: Final config
            filepath: Path where config was saved
        """
        input_files = config.get('input_files', {})
        
        print(f"\n{'='*60}")
        print(f"✅ Configuration Saved: {filepath.name}")
        print(f"{'='*60}")
        
        print(f"\n📊 Summary:")
        print(f"   FHIR/IG files: {len(input_files.get('fhir_igs', []))}")
        print(f"   Guardrails files: {len(input_files.get('guardrails', []))}")
        print(f"   Glue files: {len(input_files.get('glue', []))}")
        print(f"   DDL files: {len(input_files.get('ddl', []))}")
        print(f"   NCPDP General: {len(input_files.get('ncpdp_general_standards', []))}")
        print(f"   NCPDP SCRIPT: {len(input_files.get('ncpdp_script_standards', []))}")
        print(f"   EDW entities: {len(input_files.get('edw', []))}")
        ancillary = input_files.get('ancillary', [])
        if ancillary:
            print(f"   Ancillary sources: {len(ancillary)}")
            for a in ancillary:
                print(f"      - {a['file']} (type={a['file_type']}, mode={a['processing_mode']})")
        
        print(f"\n🚀 Next Steps:")
        print(f"   1. Review config: {filepath}")
        print(f"   2. Run: python cdm_orchestrator.py {self.cdm_name}")


def main():
    """Entry point for config generator."""
    if len(sys.argv) < 2:
        print("Usage: python -m src.config.config_generator <cdm_name>")
        print("\nExamples:")
        print("  python -m src.config.config_generator plan")
        print("  python -m src.config.config_generator formulary")
        print("  python -m src.config.config_generator 'plan and benefit'")
        sys.exit(1)
    
    cdm_name = sys.argv[1]
    
    # Check for dry-run flag
    dry_run = '--dry-run' in sys.argv or '-d' in sys.argv
    
    try:
        # Import LLM client
        from src.core.llm_client import LLMClient
        
        llm = None
        if not dry_run:
            llm = LLMClient(timeout=1800)
        
        generator = ConfigGenerator(cdm_name, llm_client=llm)
        generator.run(dry_run=dry_run)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()