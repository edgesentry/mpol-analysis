# LLM Setup

arktrace supports three LLM providers configured via `.env`. Set `LLM_PROVIDER` to one of:

| `LLM_PROVIDER` | Where it runs | Requires |
|---|---|---|
| `llamacpp` *(default)* | Local — no server, no internet | GGUF model file |
| `anthropic` | Remote — Anthropic API | `ANTHROPIC_API_KEY` |
| `openai` | Remote — any OpenAI-compatible API | `LLM_BASE_URL` + `LLM_API_KEY` |

## What the LLM does in arktrace

| Feature | Prompt shape | Typical output |
|---|---|---|
| **Analyst brief** | Vessel profile + top SHAP signals + 3 GDELT events | One paragraph citing a specific event and how it connects to the vessel's risk score |
| **Analyst chat** | Fleet overview + optional vessel detail + analyst question | Direct factual answer grounded in the provided data |

Prompts are short (500–1,200 tokens in, 150–300 out). Instruction following matters more than reasoning ability — a 4B model is sufficient.

---

## Provider: llamacpp (local, no server)

The simplest setup — no separate server, no internet, runs on any laptop with 8 GB RAM.

**1. Install:**
```bash
uv pip install llama-cpp-python
# Apple Silicon — Metal acceleration:
CMAKE_ARGS="-DGGML_METAL=on" uv pip install llama-cpp-python --force-reinstall
```

**2. Download a GGUF model** (~2.5 GB, Gemma 4B recommended):
```bash
pip install huggingface-hub
huggingface-cli download bartowski/gemma-3-4b-it-GGUF \
    gemma-3-4b-it-Q4_K_M.gguf --local-dir ~/models/
```

**3. Configure `.env`:**
```bash
LLM_PROVIDER=llamacpp
LLAMACPP_MODEL_PATH=/Users/yourname/models/gemma-3-4b-it-Q4_K_M.gguf
```

**4. Start the dashboard** — no other process needed:
```bash
uv run uvicorn src.api.main:app --reload
```

The model loads once on first request. If `LLAMACPP_MODEL_PATH` is unset or the file is missing, the dashboard loads normally and brief generation returns a "LLM not configured" placeholder.

**Memory guide:**

| Model | Quantisation | File size | RAM needed |
|---|---|---|---|
| Gemma 4B Instruct | Q4_K_M | ~2.5 GB | 8 GB |
| Gemma 4B Instruct | Q8_0 | ~4.3 GB | 8 GB |
| Gemma 12B Instruct | Q4_K_M | ~7.5 GB | 16 GB |

---

## Provider: anthropic (remote)

```bash
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
LLM_MODEL=claude-haiku-4-5-20251001   # default — fast and cheap for briefs
```

---

## Provider: openai (remote, any OpenAI-compatible API)

Works with OpenAI, Ollama, MLX LM, LM Studio, or any other OpenAI-compatible endpoint.

**OpenAI:**
```bash
LLM_PROVIDER=openai
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
# LLM_BASE_URL defaults to https://api.openai.com/v1 if not set
```

**Self-hosted (Ollama, MLX LM, LM Studio, etc.):**
```bash
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:11434/v1   # Ollama
LLM_API_KEY=local
LLM_MODEL=qwen2.5:7b
```
