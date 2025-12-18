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
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from . import config_utils
from .config_gen_core import ConfigGeneratorBase, prompt_user_choice
from .config_gen_fhir import FHIRConfigGenerator
from .config_gen_ncpdp import NCPDPConfigGenerator
from .config_gen_glue import GlueConfigGenerator


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
        
        # Step 2: Auto-discover guardrails and DDL files
        guardrails_files = config_utils.list_guardrail_files(self.cdm_name)
        ddl_files = config_utils.list_ddl_files(self.cdm_name)
        
        print(f"\n   Auto-discovered files:")
        print(f"      Guardrails: {len(guardrails_files)}")
        print(f"      DDL: {len(ddl_files)}")
        
        # Step 3: Run analyses based on user selections
        fhir_result = None
        ncpdp_result = None
        glue_result = None
        
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
        
        # Glue Analysis
        if prompt_user_choice("\n   Run Glue analysis?", default="Y"):
            glue_result = self.glue_gen.run_analysis(source_config, dry_run)
            # Note: Glue generator prints its own skip messages
        
        # Step 4: Build updated config (merge changes into source)
        updated_config = self._merge_updates(
            source_config, fhir_result, ncpdp_result, glue_result,
            guardrails_files, ddl_files
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
    
    def _merge_updates(
        self,
        source_config: Dict,
        fhir_result: Optional[Dict],
        ncpdp_result: Optional[Dict],
        glue_result: Optional[Dict],
        guardrails_files: List[str],
        ddl_files: List[str]
    ) -> Dict:
        """Merge analysis results into source config.
        
        Only updates sections that were analyzed. Preserves all other fields.
        Auto-discovered guardrails and DDL files always override config values.
        
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
        
        # Always update guardrails and DDL from auto-discovery (filename only)
        config['input_files']['guardrails'] = guardrails_files
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
        
        # Update metadata timestamp
        if 'metadata' not in config:
            config['metadata'] = {}
        config['metadata']['generated_at'] = datetime.now().isoformat()
        config['metadata']['generator_version'] = "3.0"
        
        return config
    
    def _build_fhir_file_entries(self, fhir_result: Dict) -> List[Dict]:
        """Build FHIR file entries with full paths.
        
        Args:
            fhir_result: FHIR analysis results
            
        Returns:
            List of file entry dicts
        """
        entries = []
        fhir_dir = config_utils.get_standards_fhir_dir()
        
        for resource in fhir_result.get('fhir_igs', []):
            filename = resource['filename']
            
            # Find actual file
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
                # File not found - include anyway for manual fix
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