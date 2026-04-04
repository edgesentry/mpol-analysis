# Local LLM Setup (macOS)

For macOS users (Intel or Apple Silicon), you can run inference locally without API keys or costs. This is particularly useful for generating analyst briefs and answering analyst chat questions in the dashboard.

## What the LLM does in arktrace

The LLM is called in two places:

| Feature | Prompt shape | Typical output |
| :--- | :--- | :--- |
| **Analyst brief (C2)** | Vessel profile (MMSI, flag, confidence score, top SHAP signals) + 3 recent GDELT geopolitical events | One paragraph citing a specific event and explaining how it connects to the vessel's risk score |
| **Analyst chat (C6)** | Fleet overview (top 10 watchlist candidates) + optional vessel detail + analyst question | Direct factual answer grounded in the provided data |

### Task requirements

The prompts are structured and data-dense but short (typically 500–1 200 tokens in, 150–300 tokens out). The LLM does not need to reason from general knowledge — all facts are supplied in the context. What matters is:

- **Instruction following** — stay within the one-paragraph brief format; cite the event given, do not hallucinate new ones
- **Structured output fidelity** — refer to specific field values (MMSI, flag state, confidence score) by name
- **Low latency** — briefs are streamed live to the analyst; a 3B model at 50–80 tok/s on Apple Silicon feels instant; a 7B model at 20–40 tok/s is still acceptable

A frontier-class model is not needed. The task is closer to *templated summarisation* than *open-ended reasoning*.

---

## Recommended Models

Model IDs and full config blocks live in **`.env.example`** — that is the single source of truth.

The recommended model for shadow fleet analysis is **Qwen 2.5 Coder 7B (Instruct 4-bit)** as it provides the best balance of speed and instruction-following for maritime data.

---

## Setup

We use the **`mlx-lm-coding-agent-proxy`** to run a local LLM that is compatible with both OpenAI and Anthropic API standards. This allows `arktrace` and `Claude Code` to share the same model instance in memory.

1. **Install and Start the Proxy**:
   Follow the instructions in the [mlx-lm-coding-agent-proxy](https://github.com/yohei1126/mlx-lm-coding-agent-proxy) repository to install and start the proxy server.

2. **Configure `.env`**:
   Uncomment the "Unified Local Proxy" block in your `.env` file:
   ```bash
   LLM_PROVIDER=mlx
   LLM_BASE_URL=http://localhost:8888/v1
   LLM_API_KEY=local
   LLM_MODEL=mlx-community/Qwen2.5-Coder-7B-Instruct-4bit
   ```

3. **Verify Connection**:
   Once the proxy is running on port 8888, the `arktrace` dashboard will automatically use it for generating briefs and chat responses.

---

## Hardware & Performance Notes

### Memory Requirements
- **7B models (Qwen 2.5 7B):** ~8 GB RAM. Recommended for 16 GB+ machines.

### Processor Support
- **Apple Silicon (M1/M2/M3/M4):** Native support via MLX for maximum performance.
- **Intel:** Not supported by this specific MLX proxy (use Ollama directly if on Intel).
