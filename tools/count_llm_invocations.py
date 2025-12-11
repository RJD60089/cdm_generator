import pathlib, re
root = pathlib.Path('.').resolve()
patterns = [re.compile(p, re.I) for p in [r'llm_client\.chat\(', r'\bllm\.chat\(', r'self\.llm_client\.chat\(', r'llm_client\.call\(', r'\bllm\.call\(', r'self\.llm_client\.call\(']]
results = {}
total = 0
for p in root.rglob('*.py'):
    try:
        text = p.read_text(encoding='utf-8')
    except Exception:
        continue
    cnt = 0
    for pat in patterns:
        c = len(pat.findall(text))
        cnt += c
    if cnt>0:
        results[str(p)] = cnt
        total += cnt
for k in sorted(results):
    print(f"{k}:{results[k]}")
print(f"TOTAL_LLM_INVOCATIONS:{total}")
