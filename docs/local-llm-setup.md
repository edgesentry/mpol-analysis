# Local LLM Setup (macOS)

For macOS users (Intel or Apple Silicon), you can run inference locally without API keys or costs. This is particularly useful for generating analyst briefs in the dashboard.

## Recommended Models

| Category | Model | MLX ID (Hugging Face) | Ollama ID |
| :--- | :--- | :--- | :--- |
| **OpenAI-alternative** | Llama 3.2 3B | `mlx-community/Llama-3.2-3B-Instruct-4bit` | `llama3.2:3b` |
| **Chinese Model** | Qwen 2.5 7B | `mlx-community/Qwen2.5-7B-Instruct-4bit` | `qwen2.5:7b` |
| **Compact Model** | Qwen 2.5 3B | `mlx-community/Qwen2.5-3B-Instruct-4bit` | `qwen2.5:3b` |

---

## ⚠️ Docker Access Note
If you are running the **MPOL Dashboard or Pipeline inside Docker**, you cannot use `localhost` in your `.env` file to reach the LLM server. You must use **`host.docker.internal`** to allow the container to communicate with your Mac host:

*   **MLX:** `LLM_BASE_URL=http://host.docker.internal:8080/v1`
*   **Ollama:** `LLM_BASE_URL=http://host.docker.internal:11434/v1`

---

## Option A: MLX LM (Recommended for Apple Silicon)

Optimized for Apple Silicon. MLX LM runs quantized models natively on the Apple Neural Engine. It is the fastest local option on M-series Macs.

1. **Install the dependencies** (requires Python 3.10+):
   ```bash
   # Sync the environment and include the mlx extra
   uv sync --extra mlx
   ```

2. **Start the OpenAI-compatible server**:
   Replace `--model` with your chosen model ID from the table above.
   ```bash
   # Example: Running Llama 3.2 3B
   uv run mlx_lm.server \
     --model mlx-community/Llama-3.2-3B-Instruct-4bit \
     --port 8080
   ```

3. **Update `.env`**:
   ```env
   LLM_PROVIDER=mlx
   LLM_BASE_URL=http://localhost:8080/v1  # Use host.docker.internal if running in Docker
   LLM_API_KEY=local
   LLM_MODEL=mlx-community/Llama-3.2-3B-Instruct-4bit
   ```

---

## Option B: Ollama (Intel & Apple Silicon)

Supports Metal acceleration on Apple Silicon and CPU on Intel.

1. **Install via Homebrew**:
   ```bash
   brew install ollama
   ```

2. **Pull a model and start the server**:
   ```bash
   # Example: Pulling the Chinese Qwen 2.5 model
   ollama pull qwen2.5:7b
   ollama serve
   ```

3. **Update `.env`**:
   ```env
   LLM_PROVIDER=ollama
   LLM_BASE_URL=http://localhost:11434/v1 # Use host.docker.internal if running in Docker
   LLM_API_KEY=local
   LLM_MODEL=qwen2.5:7b
   ```

---

## Hardware & Performance Notes

### Memory Requirements
- **2B - 4B Models (Llama 3.2 3B, Qwen 2.5 3B):** Requires ~4GB RAM. Runs comfortably on 8GB MacBook Air.
- **7B - 9B Models (Qwen 2.5 7B):** Requires ~8GB RAM. Recommended for 16GB+ machines.

### Processor Support
- **MLX LM:** Native support for **Apple Silicon (M1/M2/M3/M4)** only.
- **Ollama:** Supports both **Apple Silicon** (Metal) and **Intel** (CPU inference).
