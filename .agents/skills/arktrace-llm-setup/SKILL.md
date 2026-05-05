---
name: arktrace-llm-setup
description: Configure the LLM provider for arktrace (OpenAI, local Ollama, or llama.cpp). Use when setting up a new environment or switching LLM providers.
license: Apache-2.0
compatibility: Requires .env file; optional local GPU for self-hosted models
metadata:
  repo: arktrace
---

Set `LLM_PROVIDER` in `.env` to one of:

| Value | Where it runs | Requires |
|---|---|---|
| `openai` | OpenAI API | `OPENAI_API_KEY` in `.env` |
| `ollama` | Local (Ollama) | `ollama serve` running on `:11434` |
| `llama_cpp` | Local (llama.cpp) | server running on `LLM_BASE_URL` |

## Ollama quick start

```bash
ollama serve                  # separate terminal
ollama pull llama3.2
echo 'LLM_PROVIDER=ollama' >> .env
```

See [references/local-llm-setup.md](references/local-llm-setup.md) for model selection, GPU config, and performance tuning.
