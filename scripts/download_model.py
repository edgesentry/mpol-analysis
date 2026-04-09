"""Download a GGUF model from HuggingFace Hub.

Supported model names:

    phi-4-mini-it    bartowski/microsoft_Phi-4-mini-instruct-GGUF  (3.8B instruct, ~2.4 GB Q4_K_M)
                     MIT licence — no restrictions on government or defence use

    qwen2.5-3b-it    bartowski/Qwen2.5-3B-Instruct-GGUF  (3B instruct, ~2.0 GB Q4_K_M)
                     Apache 2.0 — no restrictions on government or defence use

    smollm2-1.7b-it  bartowski/SmolLM2-1.7B-Instruct-GGUF  (1.7B instruct, ~1.1 GB Q4_K_M)
                     Apache 2.0 — smallest supported model; runs on 6 GB RAM

    mistral-7b-it    bartowski/Mistral-7B-Instruct-v0.3-GGUF  (7B instruct, ~4.4 GB Q4_K_M) [DEFAULT]
                     Apache 2.0 — highest quality local option; requires 10 GB RAM

Usage:
    # By short name (recommended):
    uv run python scripts/download_model.py phi-4-mini-it

    # Override output directory:
    uv run python scripts/download_model.py phi-4-mini-it --dir ~/models

    # Via environment variable (used by docker-compose model_init):
    MODEL_NAME=phi-4-mini-it uv run python scripts/download_model.py

Gated models require a HuggingFace token:
    HF_TOKEN=hf_... uv run python scripts/download_model.py phi-4-mini-it
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


class _LogTqdm:
    """tqdm-compatible shim that writes download progress to stdout (no TTY needed)."""

    def __init__(self, *, total: int | None = None, **_kwargs):
        self.total = total
        self.n = 0
        self._last_report = 0.0

    def update(self, n: int = 1) -> None:
        self.n += n
        now = time.monotonic()
        if now - self._last_report >= 5.0:  # print every 5 seconds
            if self.total:
                pct = self.n / self.total * 100
                mb_done = self.n / 1_048_576
                mb_total = self.total / 1_048_576
                print(f"  {pct:.1f}%  {mb_done:.0f} / {mb_total:.0f} MB", flush=True)
            else:
                print(f"  {self.n / 1_048_576:.0f} MB downloaded …", flush=True)
            self._last_report = now

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.total:
            print(f"  100%  {self.total / 1_048_576:.0f} MB — done", flush=True)
        return False


# repo_id, filename (Q4_K_M quantisation — good quality/size balance)
MODEL_CATALOG: dict[str, tuple[str, str]] = {
    "phi-4-mini-it": (
        "bartowski/microsoft_Phi-4-mini-instruct-GGUF",
        "microsoft_Phi-4-mini-instruct-Q4_K_M.gguf",
    ),
    "qwen2.5-3b-it": (
        "bartowski/Qwen2.5-3B-Instruct-GGUF",
        "Qwen2.5-3B-Instruct-Q4_K_M.gguf",
    ),
    "smollm2-1.7b-it": (
        "bartowski/SmolLM2-1.7B-Instruct-GGUF",
        "SmolLM2-1.7B-Instruct-Q4_K_M.gguf",
    ),
    "mistral-7b-it": (
        "bartowski/Mistral-7B-Instruct-v0.3-GGUF",
        "Mistral-7B-Instruct-v0.3-Q4_K_M.gguf",
    ),
}

DEFAULT_MODEL = "mistral-7b-it"
DEFAULT_DIR = Path.home() / "models"


def download(model_name: str, output_dir: Path, token: str | None) -> Path:
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        print("ERROR: huggingface-hub not installed. Run: uv pip install huggingface-hub")
        sys.exit(1)

    if model_name not in MODEL_CATALOG:
        known = ", ".join(MODEL_CATALOG)
        print(f"ERROR: unknown model '{model_name}'. Known models: {known}")
        sys.exit(1)

    repo_id, filename = MODEL_CATALOG[model_name]
    output_dir.mkdir(parents=True, exist_ok=True)
    dest = output_dir / filename

    if dest.exists():
        print(f"Already present: {dest}")
        return dest

    print(f"Downloading {filename} from {repo_id} …", flush=True)
    path = hf_hub_download(
        repo_id=repo_id,
        filename=filename,
        local_dir=str(output_dir),
        token=token or None,
        tqdm_class=_LogTqdm,
    )
    print(f"Saved to {path}", flush=True)
    return Path(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download a GGUF model from HuggingFace")
    parser.add_argument(
        "model",
        nargs="?",
        default=os.getenv("MODEL_NAME", DEFAULT_MODEL),
        help=f"Model name (default: {DEFAULT_MODEL}). Known: {', '.join(MODEL_CATALOG)}",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=Path(os.getenv("MODEL_DIR", str(DEFAULT_DIR))),
        help="Output directory (default: ~/models or $MODEL_DIR)",
    )
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN") or os.getenv("HUGGING_FACE_HUB_TOKEN")
    download(args.model, args.dir, token)


if __name__ == "__main__":
    main()
