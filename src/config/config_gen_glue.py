"""
Glue configuration generator for CDM.

Handles:
- Glue file discovery from CDM source directory
- Glue file consolidation (multiple source files -> single consolidated file)
- Glue schema validation
- File path management

Directory structure:
- Source files: input/business/cdm_{name}/glue/source/*.json
- Consolidated: input/business/cdm_{name}/glue/GLUE_{name}_cdm.json
"""
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import config_utils
from .config_gen_core import ConfigGeneratorBase, prompt_user_choice


class GlueConfigGenerator(ConfigGeneratorBase):
    """Glue table schema management for CDM configuration."""
    
    def __init__(self, cdm_name: str, llm_client=None):
        """Initialize Glue config generator.
        
        Args:
            cdm_name: CDM name (e.g., 'plan', 'formulary')
            llm_client: LLM client (not typically needed for Glue)
        """
        super().__init__(cdm_name, llm_client)
        self.glue_dir = self.cdm_dir / "glue"
        self.source_dir = self.glue_dir / "source"
        self.consolidated_filename = f"GLUE_{self.safe_name}_cdm.json"
    
    def get_source_files(self) -> List[Path]:
        """Get Glue source files from source directory.
        
        Looks in: input/business/cdm_{name}/glue/source/
        
        Returns:
            List of source file paths
        """
        if not self.source_dir.exists():
            return []
        
        return sorted(self.source_dir.glob("*.json"))
    
    def get_consolidated_file(self) -> Optional[Path]:
        """Get consolidated Glue file if it exists.
        
        Looks for: input/business/cdm_{name}/glue/GLUE_{name}_cdm.json
        
        Returns:
            Path to consolidated file, or None
        """
        consolidated_path = self.glue_dir / self.consolidated_filename
        return consolidated_path if consolidated_path.exists() else None
    
    def run_analysis(self, config: Dict, dry_run: bool = False) -> Optional[Dict]:
        """Run Glue file discovery and consolidation.
        
        Logic:
        1. If nothing in source/ → skip
        2. If no consolidated file AND files in source/ → run consolidation
        3. If consolidated exists AND files in source/ → prompt to rebuild
        
        Args:
            config: Current config dict
            dry_run: If True, report but don't modify
            
        Returns:
            Dict with updated glue config, or None if no changes
        """
        print("\n📦 Glue Analysis")
        
        # Check source files
        source_files = self.get_source_files()
        consolidated = self.get_consolidated_file()
        
        # Case 1: No source files - skip
        if not source_files:
            print(f"   ℹ️  No source files in {self.source_dir}")
            # If consolidated exists, keep it in config
            if consolidated:
                rel_path = config_utils.normalize_path(consolidated, self.project_root)
                print(f"   ✓ Using existing: {consolidated.name}")
                return {'glue': [rel_path]}
            return None
        
        # Report source files found
        print(f"\n   Source files ({len(source_files)}):")
        for f in source_files:
            print(f"      • {f.name}")
        
        # Case 2: Source files exist, no consolidated file - run automatically
        if not consolidated:
            print(f"\n   No consolidated file found - creating...")
            
            if dry_run:
                print(f"   [DRY RUN] Would consolidate {len(source_files)} file(s)")
                return {'glue': [], '_dry_run': True}
            
            return self._run_consolidation(source_files)
        
        # Case 3: Both exist - prompt to rebuild
        print(f"\n   Existing consolidated: {consolidated.name}")
        
        if dry_run:
            print(f"   [DRY RUN] Would prompt to rebuild")
            rel_path = config_utils.normalize_path(consolidated, self.project_root)
            return {'glue': [rel_path], '_dry_run': True}
        
        if prompt_user_choice("   Rebuild consolidated file?", default="N"):
            return self._run_consolidation(source_files)
        
        # Keep existing
        rel_path = config_utils.normalize_path(consolidated, self.project_root)
        print(f"   ✓ Keeping existing: {consolidated.name}")
        return {'glue': [rel_path]}
    
    def _run_consolidation(self, source_files: List[Path]) -> Optional[Dict]:
        """Execute consolidation of source files.
        
        Args:
            source_files: List of source file paths
            
        Returns:
            Dict with glue file list, or None on failure
        """
        # Convert to string paths
        source_paths = [str(f) for f in source_files]
        
        consolidated_path, summary = self.consolidate_files(source_paths)
        
        if consolidated_path:
            rel_path = config_utils.normalize_path(consolidated_path, self.project_root)
            return {
                'glue': [rel_path],
                '_consolidation_summary': summary
            }
        
        return None
    
    def consolidate_files(self, source_files: List[str], output_filename: Optional[str] = None) -> Tuple[Optional[Path], Dict]:
        """Consolidate multiple Glue table files into single array format.
        
        Takes individual Glue table JSON files (each containing a single table definition)
        and merges them into a single JSON array file.
        
        Args:
            source_files: List of paths to individual Glue JSON files
            output_filename: Output filename (default: GLUE_{cdm}_cdm.json)
            
        Returns:
            Tuple of (output path, consolidation summary)
        """
        print(f"\n   Consolidating Glue files...")
        
        if not source_files:
            print("   ⚠️  No source files provided")
            return None, {'error': 'No source files'}
        
        # Default output filename
        if output_filename is None:
            output_filename = self.consolidated_filename
        
        tables = []
        errors = []
        
        for filepath_str in source_files:
            filepath = Path(filepath_str)
            
            # Resolve relative paths from project root
            if not filepath.is_absolute():
                filepath = self.project_root / filepath
            
            if not filepath.exists():
                errors.append(f"File not found: {filepath_str}")
                print(f"      ⚠️  Not found: {filepath.name}")
                continue
            
            try:
                data = config_utils.load_json_file(filepath)
                
                # Handle both formats:
                # 1. Single table object: {"Name": "...", "DatabaseName": "...", ...}
                # 2. Already an array: [{"Name": "...", ...}, ...]
                if isinstance(data, list):
                    tables.extend(data)
                    print(f"      ✓ {filepath.name}: {len(data)} table(s)")
                elif isinstance(data, dict) and 'Name' in data:
                    tables.append(data)
                    print(f"      ✓ {filepath.name}: {data.get('Name', 'unknown')}")
                else:
                    errors.append(f"Unknown format: {filepath_str}")
                    print(f"      ⚠️  Unknown format: {filepath.name}")
                    
            except json.JSONDecodeError as e:
                errors.append(f"JSON error in {filepath_str}: {e}")
                print(f"      ⚠️  JSON error: {filepath.name}")
            except Exception as e:
                errors.append(f"Error reading {filepath_str}: {e}")
                print(f"      ⚠️  Error: {filepath.name}")
        
        if not tables:
            print("   ❌ No valid tables found")
            return None, {'errors': errors}
        
        # Ensure glue directory exists
        self.glue_dir.mkdir(parents=True, exist_ok=True)
        
        # Save consolidated file
        output_path = self.glue_dir / output_filename
        config_utils.save_json_file(output_path, tables)
        
        # Generate relative path for config
        rel_path = config_utils.normalize_path(output_path, self.project_root)
        
        summary = {
            'output_file': str(output_path),
            'output_relative': rel_path,
            'tables_count': len(tables),
            'source_files_count': len(source_files),
            'tables': [t.get('Name', 'unknown') for t in tables],
            'errors': errors
        }
        
        print(f"\n   ✓ Consolidated {len(tables)} tables → {output_filename}")
        
        return output_path, summary
    
    def validate_files(self, glue_files: List[str]) -> Tuple[List[str], List[str]]:
        """Validate Glue files exist and have valid structure.
        
        Args:
            glue_files: List of Glue file paths
            
        Returns:
            Tuple of (valid files, warnings)
        """
        valid = []
        warnings = []
        
        for filepath_str in glue_files:
            filepath = Path(filepath_str)
            
            # Resolve relative paths
            if not filepath.is_absolute():
                filepath = self.project_root / filepath
            
            if not filepath.exists():
                warnings.append(f"Glue file not found: {filepath_str}")
                continue
            
            try:
                data = config_utils.load_json_file(filepath)
                
                # Validate structure
                if isinstance(data, list):
                    # Array format - check each table has Name
                    if all(isinstance(t, dict) and 'Name' in t for t in data):
                        valid.append(filepath_str)
                    else:
                        warnings.append(f"Invalid table structure in: {filepath_str}")
                elif isinstance(data, dict) and 'Name' in data:
                    # Single table format
                    valid.append(filepath_str)
                else:
                    warnings.append(f"Unknown Glue format: {filepath_str}")
                    
            except Exception as e:
                warnings.append(f"Error reading {filepath_str}: {e}")
        
        return valid, warnings
    
    def get_table_summary(self, glue_files: List[str]) -> Dict:
        """Get summary of tables in Glue files.
        
        Args:
            glue_files: List of Glue file paths
            
        Returns:
            Summary dict with table counts and names
        """
        summary = {
            'total_tables': 0,
            'files': {}
        }
        
        for filepath_str in glue_files:
            filepath = Path(filepath_str)
            
            if not filepath.is_absolute():
                filepath = self.project_root / filepath
            
            if not filepath.exists():
                summary['files'][filepath_str] = {'error': 'not found'}
                continue
            
            try:
                data = config_utils.load_json_file(filepath)
                
                if isinstance(data, list):
                    tables = [t.get('Name', 'unknown') for t in data if isinstance(t, dict)]
                    summary['files'][filepath_str] = {
                        'count': len(tables),
                        'tables': tables
                    }
                    summary['total_tables'] += len(tables)
                elif isinstance(data, dict) and 'Name' in data:
                    summary['files'][filepath_str] = {
                        'count': 1,
                        'tables': [data.get('Name', 'unknown')]
                    }
                    summary['total_tables'] += 1
                else:
                    summary['files'][filepath_str] = {'error': 'unknown format'}
                    
            except Exception as e:
                summary['files'][filepath_str] = {'error': str(e)}
        
        return summary
    
    def update_config_with_consolidated(self, config: Dict, consolidated_path: Path) -> Dict:
        """Update config to use consolidated Glue file.
        
        Args:
            config: Original config dict
            consolidated_path: Path to consolidated file
            
        Returns:
            Updated config dict
        """
        rel_path = config_utils.normalize_path(consolidated_path, self.project_root)
        
        # Replace multiple files with single consolidated file
        if 'input_files' not in config:
            config['input_files'] = {}
        
        config['input_files']['glue'] = [rel_path]
        
        return config