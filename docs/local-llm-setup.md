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
| **Dispatch brief** | Vessel data (dark count, ownership hop, flag changes, ATT, p-value, confidence) | Officer-to-commander verbal brief in a fixed format |
| **Analyst chat** | Fleet overview + optional vessel detail + analyst question | Direct factual answer grounded in the provided data |

Prompts are short (500–1,200 tokens in, 150–300 out). A 4–7B model is sufficient.

---

## 🍎 Recommended: Native macOS dev mode (mlx-lm)

`mlx-lm` runs natively on Apple Silicon via the MLX framework and exposes an OpenAI-compatible REST endpoint. The dashboard connects to it as a standard `openai` provider.

> **Infra (MinIO) still runs in Docker.** Only the FastAPI process and mlx-lm run on the host.

### One-time setup

```bash
uv pip install mlx-lm
```

### Start everything

```bash
# Recommended — one command starts MinIO, mlx-lm, and the dashboard:
bash scripts/run_app.sh

# Override the default model:
bash scripts/run_app.sh --model mlx-community/Qwen2.5-7B-Instruct-4bit

# Skip Docker infra (MinIO already running):
bash scripts/run_app.sh --no-infra

# Skip mlx-lm (server already running on port 8080):
bash scripts/run_app.sh --no-llm

# Use Anthropic instead of local LLM:
bash scripts/run_app.sh --provider anthropic
```

`scripts/run_app.sh` flag reference:

| Flag | Default | Description |
|---|---|---|
| `--model MODEL` | `mlx-community/Qwen2.5-7B-Instruct-4bit` | mlx-community model ID or local path |
| `--provider NAME` | `openai` | `openai` (mlx-lm or remote) or `anthropic` |
| `--port PORT` | `8000` | uvicorn port |
| `--llm-port PORT` | `8080` | mlx-lm server port |
| `--no-infra` | — | Skip starting Docker infra |
| `--no-llm` | — | Skip starting mlx-lm server |

### Manual startup (step-by-step)

```bash
# 1. Start MinIO
docker compose -f docker-compose.infra.yml up -d

# 2. Start mlx-lm server (downloads model on first run — ~4 GB)
uv run mlx_lm.server --model mlx-community/Qwen2.5-7B-Instruct-4bit --port 8080 &

# 3. Start the dashboard
S3_ENDPOINT=http://localhost:9000 \
LLM_PROVIDER=openai \
LLM_BASE_URL=http://localhost:8080/v1 \
LLM_API_KEY=local \
LLM_MODEL=mlx-community/Qwen2.5-7B-Instruct-4bit \
  uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Model guide

| Model | Size (4-bit) | Min RAM | Licence | Notes |
|---|---|---|---|---|
| `mlx-community/Qwen2.5-7B-Instruct-4bit` | ~4.3 GB | 8 GB | Apache 2.0 | Default — good balance of quality and speed |
| `mlx-community/Qwen2.5-3B-Instruct-4bit` | ~1.9 GB | 6 GB | Apache 2.0 | Faster, lower RAM |
| `mlx-community/Phi-4-mini-instruct-4bit` | ~2.4 GB | 8 GB | MIT | No restrictions on government or defence use |
| `mlx-community/Mistral-7B-Instruct-v0.3-4bit` | ~4.1 GB | 10 GB | Apache 2.0 | Highest quality local option |

All models above are permissively licensed — no restrictions on government or defence use.

---

## Provider: anthropic (remote)

```bash
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
LLM_MODEL=claude-haiku-4-5-20251001   # default — fast and cheap for briefs
```

---

## Provider: openai — remote or any OpenAI-compatible API

Works with OpenAI, Ollama, LM Studio, or any other OpenAI-compatible endpoint.

```bash
# OpenAI
LLM_PROVIDER=openai
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini

# Ollama
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:11434/v1
LLM_API_KEY=local
LLM_MODEL=qwen2.5:7b

# mlx-lm (already running)
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:8080/v1
LLM_API_KEY=local
LLM_MODEL=mlx-community/Qwen2.5-7B-Instruct-4bit
```
