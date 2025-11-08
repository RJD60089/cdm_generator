from __future__ import annotations
import os, argparse, json
from src.core.llm_client import LLMClient
from src.core.run_state import RunState
from src.core.standard_loader import load_naming_rules
from src.steps.step1_requirements import run_step
from dotenv import load_dotenv
load_dotenv()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", required=True, help="Label for this CDM run (e.g., PlanBenefit)")
    ap.add_argument("--standard", help="Path to Enterprise Data Field Naming Standard (docx/txt)")
    ap.add_argument("--outdir", default="output", help="Output folder")
    args = ap.parse_args()

    llm = LLMClient()
    state_obj = RunState.new(domain=args.domain, model=llm.model, temperature=llm.temperature)
    state = state_obj.to_dict()
    state["naming_rules"] = load_naming_rules(args.standard)

    os.makedirs(args.outdir, exist_ok=True)
    run_base = os.path.join(args.outdir, f"{args.domain}_{state['meta']['timestamp']}_{state['meta']['run_id']}")
    work_json = f"{run_base}.work.json"
    runlog = f"{run_base}.run.jsonl"

    # STEP 1
    state = run_step(state, "prompts/prompt_1_requirements.md", llm, runlog)

    with open(work_json, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    print(f"Step 1 complete.\n- Work JSON: {work_json}\n- Run log:   {runlog}")

if __name__ == "__main__":
    main()
