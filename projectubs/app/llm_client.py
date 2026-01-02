# -*- coding: utf-8 -*-
"""
Ollama LLM Client (urllib-based)
- No extra dependencies
- Safe fallback if Ollama is not running
"""

import json
import os
import urllib.request
import urllib.error
from typing import Optional, Dict, Any


class LLMUnavailable(Exception):
    pass


class OllamaClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = 25,
    ):
        self.base_url = (base_url or os.getenv("OLLAMA_URL", "http://localhost:11434")).rstrip("/")
        self.model = model or os.getenv("OLLAMA_MODEL", "llama3.1:latest")
        self.timeout = timeout

    def generate(
        self,
        prompt: str,
        system: str,
        temperature: float = 0.2,
        top_p: float = 0.9,
        max_tokens: int = 350,
    ) -> str:
        url = f"{self.base_url}/api/generate"
        payload: Dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "system": system,
            "stream": False,
            "options": {
                "temperature": temperature,
                "top_p": top_p,
                "num_predict": max_tokens,  # ollama output tokens
            },
        }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
                obj = json.loads(raw)
                return (obj.get("response") or "").strip()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            raise LLMUnavailable(str(e)) from e