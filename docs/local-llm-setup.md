# LLM Setup

arktrace supports two LLM providers configured via `.env`. Set `LLM_PROVIDER` to one of:

| `LLM_PROVIDER` | Where it runs | Requires |
|---|---|---|
| `openai` *(default)* | Local (llama-server) or any OpenAI-compatible API | llama.cpp installed or remote endpoint |
| `anthropic` | Remote — Anthropic API | `ANTHROPIC_API_KEY` |

## What the LLM does in arktrace

| Feature | Prompt shape | Typical output |
|---|---|---|
| **Analyst brief** | Vessel profile + top SHAP signals + 3 GDELT events | One paragraph citing a specific event and how it connects to the vessel's risk score |
| **Dispatch brief** | Vessel data (dark count, ownership hop, flag changes, ATT, p-value, confidence) | Officer-to-commander verbal brief in a fixed format |
| **Analyst chat** | Fleet overview + optional vessel detail + analyst question | Direct factual answer grounded in the provided data |

Prompts are short (500–1,200 tokens in, 150–300 out). A 4–7B model is sufficient.

---

## Recommended: llama.cpp (cross-platform)

arktrace uses **llama.cpp** (`llama-server`) as its local inference backend. It runs on macOS (Metal), Linux (CPU/CUDA), and Windows — the same stack everywhere.

The default model is `bartowski/Qwen2.5-7B-Instruct-GGUF` (Q4_K_M quantisation), commercially licensed under Apache 2.0.

### One-time installation

**macOS**

```bash
brew install llama.cpp caddy
```

**Linux (Debian / Ubuntu)**

```bash
# llama.cpp — download the pre-built binary for your arch
# https://github.com/ggml-org/llama.cpp/releases/latest
# e.g. llama-<tag>-bin-ubuntu-x64.zip → unzip, add to PATH

# Caddy
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https curl
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
  | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
  | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update && sudo apt install caddy
```

**Linux (Fedora / RHEL)**

```bash
sudo dnf install caddy
```

**Windows**

```powershell
# llama.cpp — download the pre-built binary:
# https://github.com/ggml-org/llama.cpp/releases/latest
# e.g. llama-<tag>-bin-win-avx2-x64.zip → unzip, add folder to PATH

# Caddy (winget)
winget install Caddy.Caddy

# or Chocolatey
choco install caddy
```

Caddy is required for Safari support — `run_llama.sh` starts it automatically as an HTTPS proxy on `:8443`. On first run it adds its local CA to the system trust store (macOS Keychain / Windows Certificate Store); Linux users may need to accept the cert manually in the browser on first visit.

### Start everything

```bash
# One command — starts llama-server and the dashboard:
bash scripts/run_app.sh

# Different region:
bash scripts/run_app.sh --region japan

# Override the default model:
bash scripts/run_app.sh --model bartowski/Qwen2.5-14B-Instruct-GGUF --gguf-file Qwen2.5-14B-Instruct-Q4_K_M.gguf

# Skip llama-server (already running on port 8080):
bash scripts/run_app.sh --no-llm

# Use Anthropic instead of local LLM:
bash scripts/run_app.sh --provider anthropic
```

`scripts/run_app.sh` flag reference:

| Flag | Default | Description |
|---|---|---|
| `--model MODEL` | `bartowski/Qwen2.5-7B-Instruct-GGUF` | HuggingFace repo or local `.gguf` path |
| `--gguf-file FILE` | `Qwen2.5-7B-Instruct-Q4_K_M.gguf` | GGUF filename within the HF repo |
| `--provider NAME` | `openai` | `openai` (llama-server) or `anthropic` |
| `--port PORT` | `8000` | uvicorn port |
| `--llm-port PORT` | `8080` | llama-server port |
| `--no-llm` | — | Skip starting llama-server |

### Manual startup (step-by-step)

```bash
# 1. Start llama-server (downloads model from HuggingFace on first run — ~4 GB)
llama-server \
  --hf-repo bartowski/Qwen2.5-7B-Instruct-GGUF \
  --hf-file Qwen2.5-7B-Instruct-Q4_K_M.gguf \
  --port 8080 \
  --ctx-size 4096 \
  --n-gpu-layers 99 &

# 2. Start the dashboard
LLM_PROVIDER=openai \
LLM_BASE_URL=http://localhost:8080/v1 \
LLM_API_KEY=local \
LLM_MODEL=Qwen2.5-7B-Instruct-Q4_K_M.gguf \
  uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Model guide

All models below are permissively licensed (Apache 2.0 or MIT) — compatible with arktrace's Apache 2.0 / MIT dual licence and safe for government and defence use.

| HF repo | GGUF file (Q4_K_M) | Size | Min RAM | Licence | Notes |
|---|---|---|---|---|---|
| `bartowski/Qwen2.5-7B-Instruct-GGUF` | `Qwen2.5-7B-Instruct-Q4_K_M.gguf` | ~4.4 GB | 8 GB | **Apache 2.0** | **Default** — best balance of quality and speed |
| `bartowski/Qwen2.5-3B-Instruct-GGUF` | `Qwen2.5-3B-Instruct-Q4_K_M.gguf` | ~2.0 GB | 6 GB | **Apache 2.0** | Faster, lower RAM; good for briefs |
| `bartowski/Qwen2.5-14B-Instruct-GGUF` | `Qwen2.5-14B-Instruct-Q4_K_M.gguf` | ~8.5 GB | 16 GB | **Apache 2.0** | Higher quality; recommended if RAM allows |
| `bartowski/Qwen2.5-Coder-7B-Instruct-GGUF` | `Qwen2.5-Coder-7B-Instruct-Q4_K_M.gguf` | ~4.4 GB | 8 GB | **Apache 2.0** | Strong structured/JSON output |
| `Qwen/Qwen3-8B-GGUF` | `Qwen3-8B-Q4_K_M.gguf` | ~4.9 GB | 8 GB | **Apache 2.0** | Newer generation; strong reasoning |
| `bartowski/Phi-3.5-mini-instruct-GGUF` | `Phi-3.5-mini-instruct-Q4_K_M.gguf` | ~2.2 GB | 6 GB | **MIT** | Very low RAM; suitable for constrained environments |

**Models to avoid** (licence restrictions):

| Model family | Licence issue |
|---|---|
| LLaMA 3.x (Meta) | Meta Community Licence — not OSI-approved; usage restrictions above 700M MAU |
| Gemma (Google) | Google Gemma ToS — restricts certain government/defence applications |
| Mistral via `mistral.ai` API | Commercial API ToS applies |

---

## Docker — analyst briefs in containers

The Docker image does not include `llama-server`. Two options for briefs in Docker:

**Option A — Anthropic API (recommended):** Pass `LLM_PROVIDER=anthropic` and `LLM_API_KEY` to `docker run`. No model download required.

**Option B — Native run_app.sh:** For local model inference with GPU acceleration (Metal on macOS, CUDA on Linux), use `bash scripts/run_app.sh` instead of Docker. llama-server runs on the host with full GPU access.

See [docs/deployment.md](deployment.md) → "Enable analyst briefs" for the exact compose snippets.

---

## Provider: anthropic (remote)

```bash
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
LLM_MODEL=claude-haiku-4-5-20251001   # default — fast and cheap for briefs
```

---

## Provider: openai — remote or any OpenAI-compatible API

Works with OpenAI, LM Studio, or any other OpenAI-compatible endpoint.

```bash
# OpenAI
LLM_PROVIDER=openai
LLM_BASE_URL=https://api.openai.com/v1
LLM_API_KEY=sk-...
LLM_MODEL=gpt-4o-mini

# llama-server (already running)
LLM_PROVIDER=openai
LLM_BASE_URL=http://localhost:8080/v1
LLM_API_KEY=local
LLM_MODEL=Qwen2.5-7B-Instruct-Q4_K_M.gguf
```
