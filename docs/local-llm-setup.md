# LLM Setup

arktrace supports two LLM providers configured via `.env`. Set `LLM_PROVIDER` to one of:

| `LLM_PROVIDER` | Where it runs | Requires |
|---|---|---|
| `openai` *(default)* | Local (mlx-lm) or any OpenAI-compatible API | mlx-lm server or remote endpoint |
| `anthropic` | Remote — Anthropic API | `ANTHROPIC_API_KEY` |

## What the LLM does in arktrace

| Feature | Prompt shape | Typical output |
|---|---|---|
| **Analyst brief** | Vessel profile + top SHAP signals + 3 GDELT events | One paragraph citing a specific event and how it connects to the vessel's risk score |
| **Analyst chat** | Fleet overview + optional vessel detail + analyst question | Direct factual answer grounded in the provided data |

Prompts are short (500–1,200 tokens in, 150–300 out). Instruction following matters more than reasoning ability — a 4B model is sufficient.

---

## Recommended: mlx-lm local server (Apple Silicon)

mlx-lm runs natively on Apple Silicon via the MLX framework and exposes an OpenAI-compatible REST API. No Docker, no compilation, no GPU driver setup required.

**1. Install:**
```bash
pip install mlx-lm
```

**2. Start the server:**
```bash
mlx_lm.server --model mlx-community/Mistral-7B-Instruct-v0.3-4bit
# or a smaller model:
mlx_lm.server --model mlx-community/Qwen2.5-3B-Instruct-4bit
```

The server listens on `http://localhost:8080/v1` by default.

**3. Configure `.env`** (defaults — no changes needed):
```bash
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:8080/v1
LLM_API_KEY=local
LLM_MODEL=gpt-4o-mini   # model name is ignored by mlx-lm; any string works
```

**4. Start the dashboard:**
```bash
docker compose -f docker-compose.infra.yml up -d   # MinIO only
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

**Model guide:**

| Model | Size | Min RAM | Licence |
|---|---|---|---|
| `mlx-community/Qwen2.5-3B-Instruct-4bit` | ~2 GB | 8 GB | Apache 2.0 |
| `mlx-community/Mistral-7B-Instruct-v0.3-4bit` | ~4 GB | 10 GB | Apache 2.0 |
| `mlx-community/phi-4-mini-instruct-4bit` | ~2.4 GB | 8 GB | MIT |

---

## Provider: anthropic (remote)

```bash
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
LLM_MODEL=claude-haiku-4-5-20251001   # default — fast and cheap for briefs
```

---

## Provider: openai (remote or other local servers)

Works with OpenAI, Ollama, LM Studio, or any other OpenAI-compatible endpoint.

**OpenAI:**
```bash
LLM_PROVIDER=openai
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini
LLM_BASE_URL=https://api.openai.com/v1
```

**Ollama:**
```bash
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:11434/v1
LLM_API_KEY=local
LLM_MODEL=qwen2.5:7b
```
