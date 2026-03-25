"""
Historial de cambios (auditoría).
Registra quién hizo qué y cuándo. Se persiste en _historial.json en GitHub.
"""
import json
import base64
import os
import requests
import streamlit as st
from datetime import datetime, timezone

HISTORIAL_FILE = "_historial.json"
MAX_ENTRADAS   = 300


def _headers():
    token = st.secrets.get("GITHUB_TOKEN", os.environ.get("GITHUB_TOKEN", ""))
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}


def _repo():
    return st.secrets.get("GITHUB_REPO", os.environ.get("GITHUB_REPO", ""))


def _branch():
    return st.secrets.get("GITHUB_BRANCH", os.environ.get("GITHUB_BRANCH", "main"))


def _read():
    url = f"https://api.github.com/repos/{_repo()}/contents/{HISTORIAL_FILE}"
    try:
        r = requests.get(url, headers=_headers(), params={"ref": _branch()}, timeout=10)
        if r.status_code == 200:
            j = r.json()
            data = json.loads(base64.b64decode(j["content"]).decode())
            return data.get("entradas", []), j["sha"]
    except Exception:
        pass
    return [], None


def _write(entradas, sha):
    url = f"https://api.github.com/repos/{_repo()}/contents/{HISTORIAL_FILE}"
    payload = {
        "message": "historial update",
        "content": base64.b64encode(
            json.dumps({"entradas": entradas}, indent=2, ensure_ascii=False).encode()
        ).decode(),
        "branch": _branch(),
    }
    if sha:
        payload["sha"] = sha
    try:
        requests.put(url, headers=_headers(), json=payload, timeout=10)
    except Exception:
        pass


def registrar(usuario: str, accion: str, detalle: str):
    """Agrega una entrada al historial. Falla silenciosamente para no interrumpir operaciones."""
    try:
        entradas, sha = _read()
        entradas.append({
            "ts":      datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "usuario": usuario,
            "accion":  accion,
            "detalle": detalle,
        })
        entradas = entradas[-MAX_ENTRADAS:]
        _write(entradas, sha)
    except Exception:
        pass


def leer_historial() -> list:
    """Retorna las entradas del historial, más reciente primero."""
    try:
        entradas, _ = _read()
        return list(reversed(entradas))
    except Exception:
        return []
