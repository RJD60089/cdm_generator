# src/core/prompt_builder.py
"""
Prompt template management using Jinja2.
Builds prompts from templates with context substitution.
"""
from __future__ import annotations
from typing import Dict, Any
from pathlib import Path
import logging

from jinja2 import Environment, FileSystemLoader, Template, TemplateNotFound

from .run_state import RunState
from .context_header import build_context_header
from .standard_loader import naming_rules_snippet


logger = logging.getLogger(__name__)


class PromptBuilder:
    """
    Manages prompt template loading and rendering.
    
    Uses Jinja2 for powerful templating with:
    - Variable substitution
    - Conditional sections
    - Loops and filters
    - Template inheritance
    """
    
    def __init__(self, prompts_dir: Path):
        """
        Initialize the prompt builder.
        
        Args:
            prompts_dir: Directory containing prompt template files
        """
        self.prompts_dir = prompts_dir
        
        if not prompts_dir.exists():
            logger.warning(f"Prompts directory does not exist: {prompts_dir}")
            prompts_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(str(prompts_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True
        )
        
        # Add custom filters
        self.env.filters['json_snippet'] = self._json_snippet_filter
        
        logger.info(f"PromptBuilder initialized with templates from {prompts_dir}")
    
    def build(self, template_name: str, **context) -> str:
        """
        Build a prompt from a template with the given context.
        
        Args:
            template_name: Name of template file (e.g., "step_1.md.j2")
            **context: Template context variables
        
        Returns:
            Rendered prompt string
        
        Raises:
            TemplateNotFound: If template file doesn't exist
        """
        try:
            template = self.env.get_template(template_name)
            rendered = template.render(**context)
            
            logger.debug(f"Rendered template '{template_name}' ({len(rendered)} chars)")
            return rendered
        
        except TemplateNotFound:
            logger.error(f"Template not found: {template_name}")
            raise
    
    def build_step_prompt(
        self,
        step_num: int,
        state: RunState,
        naming_rules: Dict[str, Any],
        business_model: str = "passthrough",
        additional_context: Dict[str, Any] | None = None
    ) -> str:
        """
        Build a prompt for a specific CDM generation step.
        
        Automatically includes standard context like domain, naming rules,
        previous work, etc.
        
        Args:
            step_num: Step number (1-5)
            state: Current run state
            naming_rules: Naming rules dictionary
            business_model: PBM business model ("passthrough" or "spread")
            additional_context: Extra context variables to include
        
        Returns:
            Rendered prompt string
        """
        # Build standard context
        context = {
            "step_num": step_num,
            "domain": state.meta.domain,
            "business_model": business_model,
            "naming_rules_snippet": naming_rules_snippet(naming_rules),
            "naming_rules": naming_rules,
            "context_header": build_context_header(state.to_dict()),
            "run_id": state.meta.run_id,
            "model": state.meta.model,
            "temperature": state.meta.temperature,
        }
        
        # Add work products from previous steps
        context.update({
            "assumptions": state.work.get("assumptions", []),
            "decisions": state.work.get("decisions", []),
            "open_questions": state.work.get("open_questions", []),
            "entities": state.work.get("entities", []),
            "relationships": state.work.get("relationships", []),
            "attributes": state.work.get("attributes", []),
            "keys": state.work.get("keys", []),
            "reference_sets": state.work.get("reference_sets", []),
            "core_functional_map": state.work.get("core_functional_map", []),
        })
        
        # Add any additional context
        if additional_context:
            context.update(additional_context)
        
        # Try Jinja2 template first, fall back to markdown
        template_name = f"step_{step_num}.md.j2"
        fallback_name = f"prompt_{step_num}_*.md"
        
        try:
            return self.build(template_name, **context)
        except TemplateNotFound:
            logger.warning(
                f"Jinja2 template '{template_name}' not found, "
                f"trying fallback '{fallback_name}'"
            )
            return self._build_from_markdown_fallback(step_num, context)
    
    def _build_from_markdown_fallback(
        self,
        step_num: int,
        context: Dict[str, Any]
    ) -> str:
        """
        Fallback to simple markdown template with {{PLACEHOLDER}} substitution.
        
        For backward compatibility with existing markdown templates.
        
        Args:
            step_num: Step number
            context: Context dictionary
        
        Returns:
            Rendered prompt string
        """
        # Look for markdown file matching the pattern
        pattern = f"prompt_{step_num}_*.md"
        matches = list(self.prompts_dir.glob(pattern))
        
        if not matches:
            logger.error(f"No template found for step {step_num}")
            raise TemplateNotFound(f"No template for step {step_num}")
        
        template_path = matches[0]
        logger.info(f"Using fallback markdown template: {template_path.name}")
        
        with open(template_path, "r", encoding="utf-8") as f:
            template_text = f.read()
        
        # Simple placeholder replacement
        replacements = {
            "{{NAMING_RULES_SNIPPET}}": context.get("naming_rules_snippet", ""),
            "{{CONTEXT_HEADER}}": context.get("context_header", ""),
            "{{DOMAIN}}": context.get("domain", ""),
            "{{BUSINESS_MODEL}}": context.get("business_model", ""),
        }
        
        for placeholder, value in replacements.items():
            template_text = template_text.replace(placeholder, str(value))
        
        return template_text
    
    def build_from_string(self, template_string: str, **context) -> str:
        """
        Build a prompt from a template string (not a file).
        
        Args:
            template_string: Template content as string
            **context: Template context variables
        
        Returns:
            Rendered prompt string
        """
        template = self.env.from_string(template_string)
        return template.render(**context)
    
    @staticmethod
    def _json_snippet_filter(obj: Any, max_items: int = 3) -> str:
        """
        Jinja2 filter to create a JSON snippet from an object.
        
        Useful for showing a preview of large lists.
        
        Args:
            obj: Object to convert to snippet
            max_items: Maximum items to show in lists
        
        Returns:
            JSON snippet string
        """
        import json
        
        if isinstance(obj, list) and len(obj) > max_items:
            snippet = obj[:max_items]
            return json.dumps(snippet, indent=2) + f"\n... ({len(obj) - max_items} more)"
        
        return json.dumps(obj, indent=2)
    
    def list_templates(self) -> list[str]:
        """
        List all available template files.
        
        Returns:
            List of template file names
        """
        templates = []
        
        for ext in [".j2", ".md"]:
            templates.extend([
                p.name for p in self.prompts_dir.glob(f"*{ext}")
            ])
        
        return sorted(templates)


def create_default_templates(prompts_dir: Path):
    """
    Create default Jinja2 template files if they don't exist.
    
    This creates starter templates for all 5 steps that can be customized.
    
    Args:
        prompts_dir: Directory to create templates in
    """
    prompts_dir.mkdir(parents=True, exist_ok=True)
    
    # Template for Step 1
    step1_template = """# CDM Prompt 1 — Requirements & Scope Synthesis

You are a senior data architect generating a **canonical data model (CDM)** workbook outline for a PBM context.
You must follow the **Enterprise Data Field Naming Standard** and the **Naming Rules** strictly.

Domain: {{ domain }}
Business Model: {{ business_model }}

## What to produce (strict JSON):
Return a JSON object with keys:
- assumptions: string[]
- decisions: string[]
- open_questions: string[]
- entities: [ { name, definition, is_core, notes } ]
- core_functional_map: [ { component, scope, rationale } ]
- reference_sets: [ { name, description, source_ref, local_stub } ]
- confidence: { tab: "Entities", score: number }  # 1–10

Only return JSON. No prose.

## Naming Rules (authoritative)
{{ naming_rules_snippet }}

{% if context_header %}
## Context Header
{{ context_header }}
{% endif %}
"""
    
    template_path = prompts_dir / "step_1.md.j2"
    if not template_path.exists():
        with open(template_path, "w", encoding="utf-8") as f:
            f.write(step1_template)
        logger.info(f"Created default template: {template_path}")
