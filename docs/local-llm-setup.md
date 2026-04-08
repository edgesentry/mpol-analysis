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

## 🍎 Native macOS dev mode (Apple Metal — recommended for local dev)

Docker on macOS runs through a Colima Linux VM which has no access to Apple Metal (GPU/ANE).
Running the dashboard natively on the host bypasses the VM and enables Metal-accelerated inference — typically **5–10× faster** on Apple Silicon.

> **Infra (MinIO) still runs in Docker.** Only the FastAPI process runs on the host.

### One-time setup

```bash
# 1. Install llama-cpp-python with Metal support
CMAKE_ARGS="-DGGML_METAL=on" uv pip install llama-cpp-python --force-reinstall

# 2. Download the model (saves to ~/models/ by default)
uv run python scripts/download_model.py gemma-4-e4b-it
```

### Start infra + dashboard

```bash
# Recommended — one command starts everything:
bash scripts/run_dev.sh

# Or step-by-step:
docker compose -f docker-compose.infra.yml up -d   # MinIO only, no dashboard container

S3_ENDPOINT=http://localhost:9000 \
LLAMACPP_MODEL_PATH=~/models/gemma-4-E4B-it-Q4_K_M.gguf \
  uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

`scripts/run_dev.sh` accepts a few flags:

| Flag | Default | Description |
|---|---|---|
| `--model PATH` | auto-detect | Path to GGUF model file |
| `--provider NAME` | `llamacpp` | Override `LLM_PROVIDER` |
| `--port PORT` | `8000` | uvicorn port |
| `--no-infra` | — | Skip Docker infra (MinIO already running) |

---

## Provider: llamacpp (local, no server)

The simplest setup — no separate server, no internet, runs on any laptop with 8 GB RAM.

**1. Install:**
```bash
uv pip install llama-cpp-python
# Apple Silicon — Metal acceleration:
CMAKE_ARGS="-DGGML_METAL=on" uv pip install llama-cpp-python --force-reinstall
```

**2. Download a GGUF model:**
```bash
# Gemma 4 4B Instruct (~2.5 GB) — recommended for 8 GB+ RAM:
uv run python scripts/download_model.py gemma-4-e4b-it

# Gemma 4 2B Instruct (~1.4 GB) — for 8 GB RAM with other apps running:
uv run python scripts/download_model.py gemma-4-e2b-it
```

Models are saved to `~/models/` by default. Override with `--dir /path/to/dir`.

**3. Configure `.env`:**
```bash
LLM_PROVIDER=llamacpp
LLAMACPP_MODEL_PATH=/Users/yourname/models/gemma-4-E4B-it-Q4_K_M.gguf
```

Alternatively, skip the download step and let the dashboard pull the model from HuggingFace on first request:
```bash
LLM_PROVIDER=llamacpp
LLAMACPP_MODEL_REPO=unsloth/gemma-4-E4B-it-GGUF
LLAMACPP_MODEL_FILE=*Q4_K_M*
```

**4. Start the dashboard** — no other process needed:
```bash
uv run uvicorn src.api.main:app --reload
```

**Docker (full stack, no Metal):** `docker compose up` handles everything — `model_init` downloads the model into a named volume on first run, then the dashboard starts automatically:
```bash
# Default: gemma-4-e4b-it
docker compose up

# Use the 2B model instead:
MODEL_NAME=gemma-4-e2b-it docker compose up
```

**Docker infra only (for native macOS dev):** Start only MinIO without the dashboard container:
```bash
docker compose -f docker-compose.infra.yml up -d
```

See the [native macOS dev mode](#-native-macos-dev-mode-apple-metal--recommended-for-local-dev) section above for the complete workflow.

The model loads once on first request. If `LLAMACPP_MODEL_PATH` is unset or the file is missing, the dashboard loads normally and brief generation returns a "LLM not configured" placeholder.

**Model guide:**

| Short name | HuggingFace repo | Q4_K_M size | Min RAM |
|---|---|---|---|
| `gemma-4-e4b-it` | `unsloth/gemma-4-E4B-it-GGUF` | ~2.5 GB | 8 GB |
| `gemma-4-e2b-it` | `unsloth/gemma-4-E2B-it-GGUF` | ~1.4 GB | 8 GB |

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

**Self-hosted (Ollama, LM Studio, etc.):**
```bash
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:11434/v1   # Ollama
LLM_API_KEY=local
LLM_MODEL=qwen2.5:7b
```
